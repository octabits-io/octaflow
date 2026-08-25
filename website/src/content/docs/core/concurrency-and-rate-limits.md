---
title: Concurrency & rate limits
description: Per-step-type caps and token buckets through a pluggable admission gate.
---

A `StepGate` is consulted **before** each step runs. It either admits the step (handing back a
`release` the engine calls when the step settles) or defers it.

```ts
const gate = createInMemoryStepGate({
  concurrency: { 'ai:generate': { maxConcurrent: 2 } },
  rateLimit: { 'ai:generate': { perSecond: 5, burst: 10 } },
});
const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey, gate });
```
Rules are keyed by **step type**, and a type with no rule is admitted unconditionally.
→ [`examples/05-concurrency-rate-limit.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/05-concurrency-rate-limit.ts)

## Deferral costs nothing

A gated step is **re-enqueued** with the gate's `retryAfterSeconds` and stays `pending`. It
consumes **no retry attempt** — being throttled is not a failure, so a step held back fifty
times still has its full `maxAttempts` budget when it finally runs.

The flip side is that deferral is *polling*: a deferred step comes back around, asks again, and
is deferred again. A tight cap on a hot step type means a lot of re-enqueue traffic. Tune it
with `concurrencyRetrySeconds` (default `1`):

```ts
createInMemoryStepGate({
  concurrency: { 'ai:generate': { maxConcurrent: 2 } },
  concurrencyRetrySeconds: 5,   // check back every 5s instead of every 1s
});
```

The gate slot is released when the step finishes — success, terminal failure, or a
retry-reschedule — and also when a step loses the atomic claim race to another worker.

## In-memory vs Postgres

`createInMemoryStepGate` is **per-process**. Correct for a single worker and for tests; with
four workers a `maxConcurrent: 2` cap becomes eight. For a cap that actually holds across
processes, use the Postgres gate:

```ts
import { createPgStepGate, flowGateDdl, applySchema, FLOW_GATE_DDL } from 'octaflow/store-pg';

await applySchema(pool, FLOW_GATE_DDL);        // ← required: the gate has its own tables

const gate = createPgStepGate({
  pool, partitionKey,
  concurrency: { 'ai:generate': { maxConcurrent: 3 } },
  rateLimit:   { 'ai:generate': { perSecond: 5, burst: 10 } },
  leaseTtlSeconds: 600,       // crash-safety for held slots — default 600
  concurrencyRetrySeconds: 1, // default 1
  schema: 'flow',             // optional; pair with flowGateDdl({ schema: 'flow' })
});
```

:::caution[The Postgres gate needs its own DDL]
`FLOW_GATE_DDL` / `flowGateDdl({ schema })` creates the lease and token-bucket tables. They are
**separate** from the store's tables — applying `FLOW_STORE_DDL` alone is not enough, and the
gate will fail at runtime without them.
:::

Concurrency slots are held as **leases** with a TTL, so a worker that crashes holding a slot
doesn't leak it forever — the lease expires after `leaseTtlSeconds` and the slot returns to the
pool. Set it comfortably above your longest step; too low and a slow step's slot is handed out
while it is still running.

Rate limiting is a token bucket: `perSecond` is the refill rate and `burst` the bucket capacity
(default `max(1, ceil(perSecond))`), so `{ perSecond: 5, burst: 10 }` sustains 5/s while
tolerating a spike of 10.

## Scope

Both rule kinds are scoped to the gate's **`partitionKey`**. A per-partition engine gets a
per-partition cap; there is no built-in global-across-partitions limit. If you need one, that's
a custom `StepGate` — the interface is a single method:

```ts
interface StepGate {
  acquire(req: StepGateRequest): Promise<StepGateDecision>;
}
// req:      { partitionKey, workflowId, stepId, stepKey, stepType }
// decision: { admitted: true, release } | { admitted: false, retryAfterSeconds }
```

For [map steps](/core/fan-out-and-map/), cap the **child** type (`myMap__item`), not
the parent — the parent runs once to produce the list, the children are the fan-out.
