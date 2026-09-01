# octaflow

## 0.17.1

### Patch Changes

- [`1aaa315`](https://github.com/octabits-io/octaflow/commit/1aaa315cad4008eee94c641fd07ac0dd0e78c565) - Housekeeping release — no engine changes.
  
  - `homepage` now points at the docs' own domain, https://octaflow.octabits.io/, instead of the
    GitHub Pages URL it used to redirect from.
  - Dev/peer tooling refreshed: `ai` 7.0.87, `pg-boss` 12.29.0, `zod` 4.5.4, `@ai-sdk/provider`
    4.0.9, `@types/node` 26.4.0, `@types/pg` 8.23.1, `@changesets/cli` 3.0.1,
    `simple-git-hooks` 2.14.0, `vitest` 4.1.11. Published peer ranges are unchanged, so nothing
    is required of consumers.
  - Docs and examples: the diamond DAG's first step reads `fetchRecord`, and the site links have
    been rewritten for the new domain.

## 0.17.0

### Minor Changes

- [`6f34d38`](https://github.com/octabits-io/octaflow/commit/6f34d383fcab4eb058c697e7e33ee387fa3dc199) - Step heartbeats — liveness for long steps, and a way to interrupt a cancelled one.
  
  `startedAt` used to be the engine's only liveness signal, so the stuck-step sweeper had to read
  "started a while ago" as "the worker is dead". One number could not serve both: a short
  threshold condemns a legitimately long step, a long one leaves a dead step squatting for the
  full 15 minutes.
  
  A step type that declares `heartbeatTimeoutMs` is now judged on **silence** instead:
  
  ```ts
  const transcode = defineStep({
    type: 'transcode',
    heartbeatTimeoutMs: 2 * 60 * 1000,   // silence for 2 minutes ⇒ presumed dead
    handler: async (ctx) => { /* … */ },
  });
  ```
  
  That is the whole opt-in — the engine beats automatically while the handler runs, so an evicted
  pod is noticed in seconds without the handler being touched. `heartbeat: 'manual'` suppresses
  the timer for handlers that want silence to mean *hung* as well as *dead*, and
  `defineMapStep` takes `itemHeartbeatTimeoutMs` for per-item work.
  
  **The beat doubles as a cancellation channel.** `ctx.heartbeat()` resolves `false` when the step
  is no longer this invocation's to run — the workflow was cancelled, it blew its deadline, or the
  sweeper re-queued the step. The engine then fires `ctx.signal` and **discards the handler's
  outcome**. Two consequences:
  
  - Cancelling a run can now interrupt a step that was already executing, for any step that beats
    and respects its abort signal. Previously that was impossible by construction.
  - The concurrent-double-execution hazard introduced when crashed steps became re-queueable is
    removed rather than merely made unlikely: a live step keeps proving it, and a superseded one
    finds out on its next beat instead of stamping its result over the new owner's.
  
  Opt-in throughout: a step type that declares no `heartbeatTimeoutMs` behaves exactly as before,
  judged by `stepExpirySeconds + stuckStepBufferSeconds` from when it started.
  
  ### Breaking changes
  
  Both land in the same **unreleased** minor as the store changes from the branching/deadlines
  work, so consumers absorb one store migration rather than two.
  
  **`WorkflowStore` gains `heartbeatStep(stepId, at)`.** It must write conditionally in one
  statement and return whether the step is still `running` under a live workflow; `markStepRunning`
  and `resetStep` must clear `heartbeat_at`, or a fresh attempt inherits a stale stamp and looks
  dead on arrival. The bundled stores are updated — see CONTRIBUTING for the contract.
  
  **`StepRecord` gains `heartbeatAt`** and `StepExecutionContext` / `TypedStepContext` gain
  `heartbeat`, backed by a new `heartbeat_at` column. `flowStoreDdl()` emits an
  `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, so re-applying the DDL migrates an existing database.
  If you host the tables in your own migrations:
  
  ```sql
  ALTER TABLE flow_workflow_step ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz;
  ```
  
  `findStuckSteps` now selects on `COALESCE(heartbeat_at, started_at)`, and the engine holds each
  candidate to its own step type's window afterwards.

- [`81c3d85`](https://github.com/octabits-io/octaflow/commit/81c3d85ae158efaa08f3533b78078c42f4e036f1) - Conditional branching, deadlines, and recovery for failed runs.
  
  Three gaps where the engine handled the happy path and the first branch of the unhappy
  path, then stopped.
  
  **Conditional branching.** A `when` guard on a step decides whether it runs at all; a
  step whose guard says no is skipped, and so is everything reachable only through it.
  `join: 'any'` lets the branches converge again — it runs once every dependency has
  settled and at least one completed, so a skipped arm no longer skips the join with it.
  Under `'any'` the `deps` type becomes possibly-absent per branch. A guard that throws is
  classified like a failing handler, so a transient error is retried rather than quietly
  pruning the branch. Available on `defineStep`, `defineWaitStep`, `defineMapStep` and
  `defineSubWorkflowStep`.
  
  **Deadlines.** `defineWaitStep` now takes `timeoutMs` plus `onTimeout`: `'fail'` (the
  default) ends the run, `{ output }` completes the step with a stand-in answer and lets
  the DAG carry on — which, paired with a `when` guard, is "approve within 48 hours,
  otherwise escalate". `StartOptions.timeoutMs` puts a wall-clock budget on a whole run,
  enforced when a step is picked up and by `recoverStuckWorkflows`, so it also catches a
  run suspended on an event that never came.
  
  **Recovering a failed run.** `engine.retryWorkflow(id)` resumes a `failed` workflow from
  where it stopped: steps that failed, were skipped in the fallout, or had their work
  compensated away go back to `pending` with a fresh attempt budget, while completed steps
  keep their output and do not run again. Emits `workflow.retried`.
  
  ### Breaking changes
  
  **Workers must call `engine.handleStepJob(payload)` instead of `executeStep`.** The queue
  now carries wait-deadline jobs alongside step runs, and only the payload's `kind` tells
  them apart. A worker still calling `executeStep` keeps working but will never time out a
  suspended step.
  
  ```diff
    await worker.start(async (payload) => {
  -   await engine.executeStep(payload.workflowId, payload.stepId);
  +   await engine.handleStepJob(payload);
    });
  ```
  
  **A crashed step is now re-queued rather than failed.** `recoverStuckWorkflows` puts a
  step whose worker died back on the queue while its attempt budget has room, and only
  fails it once that budget is spent — a dead pod costs an attempt, not the whole run.
  Handlers must therefore tolerate re-execution after a partial run, which is the contract
  retries already imposed. Set `config.onStuckStep: 'fail'` for the previous behaviour. Its
  return type gained `retriedSteps` and `expiredWorkflows`.
  
  **`WorkflowStore` gained three methods and changed one.** Custom stores must implement
  `resetStep`, `reopenWorkflow` and `deleteChildSteps`, and `markStepWaiting` now takes a
  `waitingAt` timestamp to stamp on `started_at` (a wait deadline is measured from it). The
  bundled `store-pg` and in-memory stores are updated; see CONTRIBUTING for the contract.
  
  **`WorkflowRecord` gained `deadlineAt`**, backed by a new `deadline_at` column.
  `flowStoreDdl()` emits an `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, so re-applying the
  DDL migrates an existing database. If you host the tables in your own migrations, add:
  
  ```sql
  ALTER TABLE flow_workflow ADD COLUMN IF NOT EXISTS deadline_at timestamptz;
  CREATE INDEX IF NOT EXISTS flow_workflow_deadline_idx
    ON flow_workflow (deadline_at) WHERE deadline_at IS NOT NULL;
  ```
  
  **The pg-boss wire payload gained `kind`**, defaulted to `'execute'` so jobs enqueued by
  an older version still parse and still mean "run the step".
  
  ### Internals
  
  Completion, failure, conditional skip and operator retry now funnel through one `settle()`
  pass — prune what can no longer run, dispatch what became runnable, finish the workflow
  once nothing is left moving. The readiness rules it uses are a pure, exported function
  (`computeReadiness`), so join rules and skip cascades are decided in one place instead of
  two that could disagree.

## 0.16.0

### Minor Changes

- [`9aba202`](https://github.com/octabits-io/octaflow/commit/9aba20245adad7f6d10e52011b04b7d6bb0d257a) - Resolve a scheduled start's idempotency key **per delivery**, so a cron schedule no longer
  collapses every tick into one workflow.
  
  A pg-boss schedule stores its payload once and redelivers that same payload on every tick. The
  scheduler baked `idempotencyKey` into that payload at schedule time, so every tick carried an
  identical key — and because start idempotency keys never expire, a nightly cron created with a
  key started **exactly one workflow, ever**. Every subsequent tick found the existing workflow
  and returned it. The failure is silent: the schedule looks healthy and the job succeeds.
  
  `ScheduleStartInput` and `WireStartPayload` now take `idempotencyKeyPrefix`, and the start
  worker resolves it against the pg-boss job id — unique per cron tick, stable across that job's
  retries:
  
  ```ts
  await scheduler.schedule({
    key: 'nightly', cron: '0 3 * * *', workflowType: 'enrichment',
    idempotencyKeyPrefix: 'nightly',        // → 'nightly:<jobId>' per tick
  });
  
  await starter.start(async (job) => {
    await engine.startWorkflow(wf.definition, job.input, { idempotencyKey: job.idempotencyKey });
  });
  ```
  
  A redelivered tick reuses its key (deduped); the next tick gets a new one (starts fresh).
  
  `idempotencyKey` is still accepted and still used verbatim, which is the right behaviour for an
  ad-hoc start and for a deliberate "run this once and never again" schedule. An explicit key wins
  over a prefix.
  
  **Breaking for `StartJobProcessor` implementations only in shape, not usage**: the processor now
  receives a `StartJobContext` — the wire payload plus `jobId` and the resolved `idempotencyKey` —
  instead of a bare `WireStartPayload`. Code that already forwarded `payload.idempotencyKey` keeps
  compiling and starts behaving correctly. Also exports `resolveStartIdempotencyKey` so a host can
  reproduce the derivation.
  
  Unrelated fix in the same area: `examples/12` and the Postgres docs imported pg-boss as
  `import PgBoss from 'pg-boss'`, but pg-boss has no default export — `new PgBoss(...)` threw at
  runtime. Both now use the named import.

## 0.15.0

### Minor Changes

- [`134a4ba`](https://github.com/octabits-io/octaflow/commit/134a4ba37756f85a879b57edfc4e0f20fc7b605f) - Commit a state change and the dispatches it unlocks in one transaction, when the
  adapters allow it.
  
  The engine wrote state and then enqueued, as two operations. A crash in that
  window left steps `pending` with no job behind them — a workflow stalled forever,
  invisible to `recoverStuckWorkflows`, which only looks at steps stuck in
  `running`. An ordinary deploy was enough. `startWorkflow` had the same shape: a
  queue failure returned `ok` with a workflow nobody would ever run.
  
  Two optional capabilities close it:
  
  - `WorkflowStore.runInTransaction(fn)` — runs `fn` in one transaction, handing it
    a transaction-bound store and an opaque handle. Implemented by `store-pg`.
  - `Dispatcher.enqueueStepIn(handle, payload, options)` — enqueues on that handle.
    Implemented by `dispatcher-pgboss` via pg-boss's `SendOptions.db`.
  
  The engine negotiates at construction. **Both present → the write and its
  dispatches commit atomically; either missing → the previous behaviour, unchanged.**
  So `store-pg` + `dispatcher-pgboss` on one Postgres now gets an exactly-once
  handoff, while a queue in a different system (SQS, Redis) keeps working as before.
  
  Both additions are optional, so existing custom stores and dispatchers continue
  to compile and run.
  
  Two deliberate boundaries:
  
  - Only writes and enqueues go inside the transaction. The failure path runs saga
    compensation — user handlers that may do network I/O — so it stays outside; a
    rollback handler must never hold a database transaction open.
  - `workflow.started` is now emitted after the transaction commits, so an observer
    never records a workflow that rolled back.
  
  Currently covers `startWorkflow` and step completion, the two paths that run on
  every workflow. Map fan-out and sub-workflow starts still write-then-enqueue and
  are covered by the redelivery repair added alongside this.

### Patch Changes

- [`55d3c69`](https://github.com/octabits-io/octaflow/commit/55d3c691761385895dad8be4ee489692851175aa) - Fix a permanent stall when the dispatch following a step completion is lost.
  
  The engine commits `completeStep` and then enqueues the newly-ready steps as
  separate operations. A crash in that window — an ordinary deploy is enough —
  left the dependents `pending` with no job behind them. Nothing recovered it:
  `recoverStuckWorkflows` only looks at steps stuck in `running`, so a step that
  was never picked up is invisible to it, and the workflow sat in `running`
  forever with no error.
  
  The queue redelivering the completed step's job was the one remaining signal,
  and the engine discarded it — a redelivered job for a non-`pending` step
  returned early as "already processed".
  
  A redelivered job for a step that already **completed** now re-drives readiness
  instead of no-opping, for both keyed steps and map children. It re-runs only the
  advance, not the completion write, so counters are not inflated; and repeating a
  dispatch is safe because claiming a step is atomic, so the duplicate delivery
  loses and does nothing.
  
  This is a repair path, not a guarantee: it depends on the dispatcher redelivering
  the completed step's job. Removing the underlying dual write — enqueueing inside
  the same transaction as the state change, which pg-boss supports via
  `SendOptions.db` — is the durable fix and is tracked separately.

## 0.14.0

### Minor Changes

- [`fc8b1f5`](https://github.com/octabits-io/octaflow/commit/fc8b1f54f0f1f6f21a710cf2b2b5bdbabe1c78a0) - **Renamed: `@octabits-io/flow` is now `octaflow`.**
  
  "Flow" already means Facebook's type checker to most JavaScript developers, and
  the scope added friction without adding meaning — nobody searching for a workflow
  engine types `@octabits-io/`. `octaflow` is unscoped, unambiguous in search, and
  still on-brand.
  
  To migrate, change the dependency and every import:
  
  ```diff
  -import { buildWorkflow } from '@octabits-io/flow';
  -import { createPgWorkflowStore } from '@octabits-io/flow/store-pg';
  +import { buildWorkflow } from 'octaflow';
  +import { createPgWorkflowStore } from 'octaflow/store-pg';
  ```
  
  ```bash
  npm remove @octabits-io/flow && npm install octaflow
  ```
  
  Nothing else changed: the subpath exports (`.`, `./ai`, `./store-pg`,
  `./dispatcher-pgboss`), every export name, and all behaviour are identical. A
  find-and-replace of the package string is the whole migration.
  
  The repository moved to `octabits-io/octaflow` (GitHub redirects the old URLs)
  and the docs are now at https://octabits-io.github.io/octaflow/.
  `@octabits-io/flow` is deprecated on npm at its last version, 0.13.0.

## 0.13.0

### Minor Changes

- [`d342f6a`](https://github.com/octabits-io/flow/commit/d342f6a85f65384fde29295c1a71fe602c397d32) - Fix a double-execution window in the step claim.
  
  `executeStep` read a step, checked it was `pending`, and then wrote `running` as a
  separate statement. Two workers handed the same job by an at-least-once dispatcher
  could both pass the read and both run the handler — with the step's `attempts`
  double-incremented.
  
  The claim is now atomic: `markStepRunning` flips `pending` → `running` only if the
  step is still `pending`, and reports whether the caller won. The Postgres store does
  this with `UPDATE … WHERE id = $1 AND status = 'pending'` and checks `rowCount`; the
  engine bails out (releasing its gate slot) when it loses the race.
  
  **Breaking for custom `WorkflowStore` implementations**: `markStepRunning` now returns
  `Promise<boolean>` instead of `Promise<void>`, and MUST perform the status check and
  the write as one atomic operation. Implementations that unconditionally write the row
  will reintroduce the double-execution window. The bundled Postgres and in-memory
  stores are already updated; consumers using them need no changes.

- [`0a3aff0`](https://github.com/octabits-io/flow/commit/0a3aff0f47a81405b3259afcacf39ffd986ebbb9) - Make the pg-boss step worker settle jobs individually, and expose the throughput
  knobs that were previously unreachable.
  
  **Per-job settlement.** pg-boss fails an entire batch when the handler throws, so
  one bad step dragged its batch neighbours into a retry — wasteful (the engine's
  atomic claim made re-execution a no-op) and it obscured which job actually
  dead-lettered. The worker now reports each job's own outcome: a step that throws
  fails alone under the queue's retry policy, and a payload that fails schema
  validation is dead-lettered directly rather than burning attempts it can never pass.
  
  **New `workerOptions`** on `createPgBossStepWorker`, all optional and defaulting to
  today's behaviour:
  
  - `burstWhenBatchFull` — keep fetching with no delay while batches come back full.
  - `burstWhenReadyExceeds` — burst while the queue's ready count exceeds a threshold.
  - `notifyPollingIntervalSeconds` — poll interval used while LISTEN/NOTIFY is active.
  - `concurrency` — steps run at once from one fetched batch (default 1, i.e. serial).
  
  Measured on the repo's benchmark (200 workflows × 6 steps, 1 worker, batch 25):
  50 → 274 steps/sec with `burstWhenBatchFull`, and 646 with `concurrency: 8` on top.
  `concurrency` alone, without burst, changes nothing — a poll-bound worker drains its
  batch in milliseconds and then waits, so the wait is the bottleneck, not the work.
  Budget connections before raising it: each in-flight step holds one, so
  `workers × concurrency` must fit the pool and Postgres `max_connections`.
  
  **Peer range**: the optional `pg-boss` peer moves from `^12.0.0` to `^12.21.0`, the
  release that introduced `perJobResults` and the burst options.

- [`f032407`](https://github.com/octabits-io/flow/commit/f032407ef3581aabbe9d88a0e7e3b4cca787f656) - Add an explicit escape hatch for retryability.
  
  Whether a failed step was retried was decided solely by `isRetryableError`, which
  matches the error *message* against a small vocabulary (`rate limit`, `429`,
  `timeout`, `ECONNRESET`, `503`, …). That silently misjudges both directions:
  `'connection refused'` is transient but failed terminally, while a permanent bug
  whose message happened to contain `'timeout'` was retried until the budget ran out.
  
  Retryability is now decided in this order:
  
  1. **An explicit marker on the error** — `retryableError(msg)`, `nonRetryableError(msg)`,
     or `markRetryable(err, bool)` to tag an error you didn't construct. Also
     `explicitRetryability(err)` to read the decision back. Markers are found through
     the `cause` chain, so wrapping an error doesn't lose its decision.
  2. **The step's own predicate** — `defineStep({ isRetryable: (e) => … })`, and
     `defineMapStep({ itemIsRetryable })` for per-item children.
  3. **`isRetryableError`** — now reads structured fields before the message: `code`
     (`ECONNRESET`, `ECONNREFUSED`, `ETIMEDOUT`, `EAI_AGAIN`, undici timeouts, …) and
     HTTP status from `status` / `statusCode` / `httpStatusCode` / `response.status`
     (408, 425, 429, 5xx except 501/505). The message vocabulary is unchanged and is
     now the last resort.
  
  Also adds `defaultRetryable` to the engine config, for hosts that would rather not
  guess at all:
  
  ```ts
  createWorkflowEngine({ …, config: { defaultRetryable: false } });
  ```
  
  It applies **only where the classifier guessed** — explicit markers, per-step
  predicates and engine-generated failures (a step timeout) are unaffected. `StepError`
  gains `retryableFrom: 'explicit' | 'predicate' | 'heuristic'` to make that distinction
  available to custom dispatchers.
  
  The marker is a non-enumerable `Symbol.for` property, so it does not leak into
  `JSON.stringify` or spread, and survives a duplicated copy of the module. Marking
  never throws — errors that are frozen, sealed or non-extensible fall back to a
  `WeakMap`, since marking is usually evaluated inside a `throw` and a `TypeError`
  there would replace the failure being reported.
  
  **Behaviour change**: errors carrying a transient `code` or a 5xx/429 status now
  retry where previously they failed terminally (their message was never consulted).
  Steps that mark nothing, define no predicate, and throw plain message-only errors
  behave exactly as before.

## 0.12.0

### Minor Changes

- [`914b82f`](https://github.com/octabits-io/flow/commit/914b82f58ff66a19514dea95a2687cc13350ee77) - Add the public wire view to core: `toPublicWorkflow`/`toPublicStep` project engine records for HTTP consumers (dropping `partitionKey`, `idempotencyKey`, sub-workflow linkage, `metadata`, and `attempts`), `STEP_DISPLAY_STATUS`/`toDisplayStepStatus` fold engine step statuses to the five display states (suspensions → `running`, `compensated` → `skipped`), and `PUBLIC_WORKFLOW_SCHEMA`/`PUBLIC_WORKFLOW_STEP_SCHEMA`/`WORKFLOW_STATUS_SCHEMA`/`STEP_DISPLAY_STATUS_SCHEMA` ship the same shapes as Zod schemas for route `response` declarations. Extend with consumer fields via `PUBLIC_WORKFLOW_SCHEMA.extend({...})` + spread.

## 0.11.2

### Patch Changes

- [`c4de746`](https://github.com/octabits-io/platform/commit/c4de746634cb164c51580ae16fd2cee2100941b2) - Fix workflows stranded in `running` forever when a parallel branch fails while another branch is still in flight.

  `checkWorkflowFailure` correctly waited for in-flight steps to settle, but `onStepCompleted`'s terminal check only counted `completed`/`skipped` — a `failed` sibling made it wait too, so when the LAST in-flight step completed after an earlier parallel failure, neither path finalized the workflow. The completion path now routes through the failure check when any keyed sibling has failed, finalizing the workflow as `failed` (with dependent-skip cascade and compensation) once every remaining step settles. The map-child path already re-checked; this brings keyed DAG steps in line.

## 0.11.1

### Patch Changes

- [`eae8882`](https://github.com/octabits-io/platform/commit/eae888215cf06b50c1da2a71f424966f7f8ec3f9) - Widen the `typescript` peer range to `^5 || ^6 || ^7` — the packages build and typecheck cleanly under TypeScript 7 (native compiler), and the emitted declarations are semantically identical to the TS 5/6 output.

## 0.11.0

### Minor Changes

- [`4643be5`](https://github.com/octabits-io/platform/commit/4643be58b3eea62325e4e85268963adbb872f77f) - store-pg: **remove the `@octabits-io/flow/store-pg/schema` Drizzle column-set subpath** (`flowWorkflowColumns`, `flowWorkflowStepColumns`, `flowStepEventColumns`, `flowRateBucketColumns`, `flowStepLeaseColumns`).

  BREAKING for anyone importing that subpath — but it had no known consumers: it was added speculatively for a host that ended up defining the flow tables from the DDL blob (`flowStoreDdl()`/`flowGateDdl()`/`flowEventDdl()`) instead. It also shipped columns only, leaving the load-bearing partial-unique idempotency index (`createWorkflow`'s `ON CONFLICT` target) as a copy-paste snippet, and had no test tying the column-sets to the DDL — so the two representations could silently drift.

  Removing it drops `drizzle-orm` as an (optional) peer dependency entirely — the raw-`pg` store bundle never imported it. Hosts that want the flow tables in their own Drizzle migrations should model them on the DDL emitted by `flowStoreDdl()` / `flowGateDdl()` / `flowEventDdl()`, which remain the single source of truth. If a real Drizzle-native consumer appears, a column-set subpath can be reintroduced with the constraints exported (not copy-pasted) and a DDL-parity test.

- [`fe07889`](https://github.com/octabits-io/platform/commit/fe078899d1613ded7a63e20ae5559b0ee7d1ec27) - store-pg: thread the injectable `SqlExecutor` seam through the **step gate** and **event sink**, so a host can run _all_ flow SQL (store + gate + sink) through one executor — e.g. one that sets a transaction-local tenant GUC, bringing the flow tables under Row Level Security. Previously only `createWorkflowStore` took an executor while the gate and sink hardwired a `pg.Pool`, so a host could not adopt RLS on `flow.*` consistently (the 0.10.0 follow-up called out in that changelog).

  - **`createStepGate({ exec, … })`** — executor-backed gate; `createPgStepGate({ pool, … })` is unchanged and now delegates over `poolExecutor(pool)`. The concurrency-lease acquire runs inside `exec.transaction`, preserving the exact prior rollback-on-cap-hit behavior (advisory lock + expired-lease cleanup roll back together when the cap is hit).
  - **`createEventSink({ exec, … })`** — executor-backed observer; `createPgEventSink({ pool, … })` unchanged and delegates. `readFlowEvents` now accepts a `Pool | SqlExecutor`, so run-history reads can also run scoped.
  - The `SqlExecutor` / `SqlResult` / `poolExecutor` seam moved to a shared `./executor` module (re-exported from `./store` for compatibility) and gained `toExecutor(pool | exec)`.

  No behavior change for existing `createPg*` callers (all delegate through `poolExecutor`, verified against the full integration suite). The pg-boss dispatcher still takes a `Pool` directly — it owns its own connections and writes no `flow.*` tables, so it is out of scope for the executor seam.

## 0.10.0

### Minor Changes

- [`0c26dbd`](https://github.com/octabits-io/platform/commit/0c26dbdffe7ca94439b31b65f21abfe63969be95) - Add an injectable `SqlExecutor` seam to the Postgres `WorkflowStore` plus a `./store-pg/schema` Drizzle column-set subpath, so a consumer can host the flow tables in its own schema, migrations, and Row Level Security instead of applying a copied DDL blob.

  - **`SqlExecutor` + `createWorkflowStore({ exec, partitionKey, schema })`** — the store now addresses all SQL through an injected executor instead of opening its own pool connections. Because the executor owns the transactions, a host can inject one that sets a transaction-local tenant GUC, so the engine's own `createWorkflow`/`completeStep`/… transactions run under RLS. `poolExecutor(pool)` is the batteries-included executor (top-level queries autocommit; `transaction` wraps `BEGIN`/`COMMIT`/`ROLLBACK`).
  - **`createPgWorkflowStore(deps)` is unchanged** — it now delegates to `createWorkflowStore` over a `poolExecutor(deps.pool)`. Same signature, same behavior (verified against the full integration suite); existing callers need no change.
  - **`@octabits-io/flow/store-pg/schema`** — spreadable Drizzle column-sets (`flowWorkflowColumns`, `flowWorkflowStepColumns`, `flowStepEventColumns`, `flowRateBucketColumns`, `flowStepLeaseColumns`) mirroring `flowStoreDdl()`/`flowEventDdl()`/`flowGateDdl()`. Following the `drizzle-toolkit/scope` precedent they ship columns only; the required indexes/uniques/PKs/FKs — notably the partial-unique `flow_workflow_idempotency_idx` that `createWorkflow`'s `ON CONFLICT` targets — are documented as a copy-paste snippet for the consumer to own.
  - `drizzle-orm` is added as an **optional** peer dependency, needed only by the new `./store-pg/schema` subpath; the raw-`pg` store bundle does not import it.

  The pg-boss dispatcher, `createPgEventSink`, and `createPgStepGate` still take a `Pool` directly — threading the executor seam through them is a follow-up.

## 0.8.0

### Minor Changes

- [`ed7813e`](https://github.com/octabits-io/platform/commit/ed7813e8274c1246ab694703d59ced0839b2e5d3) - `./ai` gains store-agnostic quota enforcement and usage aggregation.

  - `createAiQuotaService({ store, getQuota })` — concurrency / per-day / per-month workflow quota checks per `partitionKey`; quota config comes from an injected `getQuota` callback (`null` = exempt), errors surface as `ai_quota_exceeded` Result values.
  - `createAiUsageAggregationService({ store })` — token/cost rollups (daily upsert deltas, date and workflow-type aggregation, current-quota-usage windows) reusing the existing `TokenUsage` shape.

  Both engines talk to narrow structural stores (`AiQuotaStore`, `AiUsageStore`) so consumers keep raw SQL on their side; the ai layer stays free of pg/drizzle per the boundary lint.

## 0.7.0

### Minor Changes

- [`1cc1230`](https://github.com/octabits-io/platform/commit/1cc12302fb98e38267d3d15a785050f0711a4e69) - store-pg: consistent schema qualification across all DDL and runtime SQL, making a dedicated Postgres schema a first-class deployment option. `flowGateDdl` and `createPgStepGate` now accept `schema` (default `'public'`), matching the store and event sink — previously the gate's two tables resolved via `search_path` while the rest were pinned to `public`, so a non-default `search_path` could split the tables across schemas. DDL for a non-default schema now emits `CREATE SCHEMA IF NOT EXISTS` (new `createSchemaDdl` export).

## 0.5.0

### Minor Changes

- `keySource` in the AI hooks (`AiModelResolver.resolveKeySource`, `AiUsageRecorder.recordWorkflowDaily`) is now `string` instead of the hardcoded `'platform' | 'tenant'` union — that pair stays the documented convention and `'platform'` remains the default, but consumers can stamp any attribution value (e.g. `'byok'`). Non-breaking for existing implementers.

## 0.3.0

### Minor Changes

- [`2446776`](https://github.com/octabits-io/platform/commit/2446776b6007b2be8eaa9890d84b9b0df4af1cf0) - **flow/ai:** add embedding-model usage instrumentation. New exports `createInstrumentedEmbeddingModel` and `createEmbeddingUsageAccumulator` (with `EmbeddingUsageAccumulator` / `EmbeddingAccumulatedUsage` types) mirror the existing language-model instrumentation for `EmbeddingModelV4`: they transparently capture input-token usage from every `embed`/`embedMany` call via the AI SDK's `wrapEmbeddingModel` middleware, additively across a batch, with a `reset()` for long-lived accumulators. The recorded `inputTokens` feed straight into the existing `estimateCostMicros` pricing table (output/cache fields = 0). Provider-agnostic. Unblocks consumers that track embedding costs (e.g. listing-vector / semantic-search pipelines).

## 0.2.0

### Minor Changes

- [`ef2238e`](https://github.com/octabits-io/platform/commit/ef2238e3549096c88b3c48e539f5faef4d9d5e30) - Add `@octabits-io/flow` — durable DAG workflow engine (Zod-typed steps, Postgres store, pg-boss dispatcher, optional AI add-on with token/cost/quota instrumentation).

  BREAKING (`@octabits-io/drizzle-toolkit`): the `./workflow` export has been removed; it is superseded by `@octabits-io/flow`. The unused `drizzle-orm` and `zod` peer dependencies were dropped along with it — the remaining `./db` module (error handling, pagination) is unchanged. `@octabits-io/foundation` moved from peerDependencies to dependencies (it is a plain utility library — consumers no longer need to install it themselves).

- Widened `typescript` peer range to `^5 || ^6`.
