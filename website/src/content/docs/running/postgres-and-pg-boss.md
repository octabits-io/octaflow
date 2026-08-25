---
title: Postgres & pg-boss
description: "Production wiring: the Postgres store, the pg-boss dispatcher, workers, DLQ and cron."
---

The durable setup swaps the in-memory store for Postgres and the in-process queue for pg-boss.
**One-time:** apply the DDL. **Per process:** build the engine, start a step worker (drives
`handleStepJob`), a DLQ worker (handles exhausted jobs), and optionally a cron scheduler.

```ts
import { Pool } from 'pg';
import { PgBoss } from 'pg-boss';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
} from 'octaflow';
import {
  createPgWorkflowStore,
  createPgStepGate,
  createPgEventSink,
  applySchema,
  FLOW_STORE_DDL,
  FLOW_GATE_DDL,
  FLOW_EVENT_DDL,
} from 'octaflow/store-pg';
import {
  createPgBossDispatcher,
  createPgBossStepWorker,
  createPgBossDlqWorker,
} from 'octaflow/dispatcher-pgboss';

const partitionKey = 'tenant-42';
const queueName = 'flow-steps';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const boss = new PgBoss({ connectionString: process.env.DATABASE_URL });
await boss.start();

// One-time schema setup (idempotent CREATE TABLE IF NOT EXISTS …).
// `applySchema` is a dev/test convenience — in production, paste the DDL into your
// own migration system instead so schema changes stay reviewed and versioned.
await applySchema(pool, FLOW_STORE_DDL);
await applySchema(pool, FLOW_GATE_DDL);
await applySchema(pool, FLOW_EVENT_DDL);

// Recommended for shared databases: keep the flow tables in their own Postgres
// schema, and grant access on it only to the worker's role. That isolates the
// engine's state from app tables without any row-level-security choreography.
//   await applySchema(pool, flowStoreDdl('flow'));           // emits CREATE SCHEMA IF NOT EXISTS
//   await applySchema(pool, flowGateDdl({ schema: 'flow' }));
//   await applySchema(pool, flowEventDdl('flow'));
//   … then pass `schema: 'flow'` to createPgWorkflowStore / createPgStepGate / createPgEventSink.

// Per-partition engine.
const store = createPgWorkflowStore({ pool, partitionKey });
const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey });
const gate = createPgStepGate({ pool, partitionKey, concurrency: { 'ai:generate': { maxConcurrent: 3 } } });
const observer = createPgEventSink({ pool, partitionKey }); // run history → flow_step_event
const registry = createStepHandlerRegistry();
const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey, gate, observer });

myWorkflow.register(registry);

// Step worker: pull a job, run it. Throwing triggers a pg-boss retry; exhaustion → DLQ.
// Each job settles on its own outcome, so one failing step never drags its batch along.
const worker = createPgBossStepWorker({
  boss,
  queueName,
  workerOptions: {
    batchSize: 25,
    burstWhenBatchFull: true, // keep fetching while batches come back full — see Performance
    concurrency: 8,           // steps run at once from one batch; each holds a store connection
  },
});
await worker.start(async (payload) => {
  // `handleStepJob`, not `executeStep`: the queue also carries the wait deadlines of
  // suspended steps, and only the payload's `kind` tells them apart.
  await engine.handleStepJob(payload);
});

// DLQ worker: a job that exhausted retries — mark the step terminally failed.
const dlq = createPgBossDlqWorker({ boss, queueName });
await dlq.start(async (payload) => {
  await engine.handleStepExhausted(payload.workflowId, payload.stepId, 'retries exhausted');
});

// Start work — the dispatcher enqueues, the worker drives it. No manual drain.
await myWorkflow.start(engine, { /* input */ });
```

Multi-tenant: build **one engine + store + dispatcher per partition**, all sharing the same
pool/boss. The step worker reads `payload.partitionKey` and routes to that partition's engine.

See [`examples/12-postgres-pgboss-production.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/12-postgres-pgboss-production.ts).

## Queue configuration

`createPgBossDispatcher` and `ensureStepQueue` take a `StepQueueConfig` that sets up the queue
and its dead-letter queue:

| Field | Default | |
|---|---|---|
| `retryLimit` | `2` | Job-level retries before dead-lettering — **separate** from a step's own [`retry` policy](/core/retry-and-timeout/#the-other-retry-layer) |
| `retryDelay` | `30` | Seconds between those retries |
| `expireInSeconds` | `600` | How long an in-flight job may occupy a worker |

:::danger[`expireInSeconds` and `stepExpirySeconds` must agree]
The engine keeps its own copy of the job expiry to compute the
[stuck-step threshold](/running/cancellation-and-recovery/#the-stuck-threshold). It is
not read from pg-boss — change one and you must change the other:

```ts
const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey,
  config: { expireInSeconds: 900 } });
const engine = createWorkflowEngine({ …, config: { stepExpirySeconds: 900 } });
```
:::

## Don't forget the sweeper

Nothing in this wiring recovers a step whose worker died mid-run. Run
[`engine.recoverStuckWorkflows()`](/running/cancellation-and-recovery/) on a timer in
one process — without it, a crashed step sits in `running` forever and its workflow never
finishes.

## Transactional dispatch, for free

With `store-pg` and `dispatcher-pgboss` on the **same** Postgres, the engine detects both
optional capabilities and commits each state change together with the jobs it unlocks —
closing the crash window that would otherwise strand steps `pending` with no job behind them.
No configuration; it is on whenever the pair is used together. See
[transactional dispatch](/extending/interfaces/#transactional-dispatch).

This is one reason to point `PgBoss` and `Pool` at the same database rather than separate ones.

## Cron / scheduled starts

```ts
import { createPgBossScheduler, createPgBossStartWorker } from 'octaflow/dispatcher-pgboss';

const scheduler = createPgBossScheduler({ boss, queueName: 'flow-starts', partitionKey });
await scheduler.schedule({
  key: 'nightly',              // unique per queue; re-scheduling the same key replaces it
  cron: '0 3 * * *',
  workflowType: 'enrichment',
  input: { full: true },
  idempotencyKeyPrefix: 'nightly',  // per-tick dedup — see below
  tz: 'Europe/Berlin',         // optional IANA zone, default UTC
});
// await scheduler.unschedule('nightly');

// A start worker turns each cron tick into a workflow start (host maps type → definition).
const starter = createPgBossStartWorker({ boss, queueName: 'flow-starts' });
await starter.start(async (job) => {
  const wf = workflowsByType[job.workflowType];
  await engine.startWorkflow(wf.definition, job.input, { idempotencyKey: job.idempotencyKey });
});
```

### Cron idempotency

A schedule stores its payload **once**, and pg-boss redelivers that same payload on every tick.
So a key fixed at schedule time can't distinguish "this tick, redelivered" from "the next tick"
— and since [start keys never expire](/core/idempotency/#scope-and-lifetime), a fixed
key would collapse every future tick into the first workflow.

The start worker therefore resolves the key **per delivery**:

| On the schedule | The worker hands the processor | Effect |
|---|---|---|
| `idempotencyKeyPrefix: 'nightly'` | `nightly:<jobId>` | one workflow **per tick**; a redelivered tick is deduped |
| `idempotencyKey: 'backfill-2026'` | `backfill-2026` | one workflow **ever** — every later tick returns the first |
| neither | `undefined` | no dedup; every delivery starts a workflow |

`idempotencyKeyPrefix` is what you want on a recurring schedule. The job id is unique per cron
tick and stable across that job's retries, which is exactly the identity dedup needs. Reach for
the verbatim `idempotencyKey` only when "run this once and never again" is genuinely the intent
(an explicit key wins if you somehow set both).

The processor also receives `job.jobId` if you'd rather build the key yourself.
