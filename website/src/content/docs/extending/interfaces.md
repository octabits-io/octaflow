---
title: Stores, dispatchers, gates
description: Every backend is an interface — and the correctness requirements a custom one must meet.
---

Everything is an interface — implement your own backend without touching the engine:

- **`WorkflowStore`** — persistence (`createWorkflow`, `listSteps`, `markStep*`, `addChildSteps`, …).
  Ship one for any database. See [the claim requirement](#the-one-hard-requirement) below.
- **`Dispatcher`** — `enqueueStep(payload, { startAfterSeconds })`. Back it with any queue (SQS,
  BullMQ, …); honor `startAfterSeconds` for retry/sleep to work durably.
- **`StepGate`** — `acquire(req)` → admit (with a `release`) or defer. Build org-wide caps however
  you like.
- **`FlowObserver` / `FlowTracer`** — run history + spans for your telemetry stack.
- **`WorkflowHooks`** — `onBeforeStart` (guard/quota), `buildStepContext` (inject `ctx.context`),
  `onAfterStep`, `onWorkflowCompleted`.

The in-memory store and the Postgres/pg-boss adapters are reference implementations.

## The one hard requirement

`markStepRunning` is the step **claim**. It must flip `pending` → `running` *atomically* and
return whether the caller won:

```ts
async markStepRunning(stepId, startedAt): Promise<boolean> {
  const res = await sql`UPDATE flow_step
                           SET status = 'running', started_at = ${startedAt},
                               attempts = attempts + 1
                         WHERE id = ${stepId} AND status = 'pending'`;
  return res.rowCount === 1;   // false → another worker got there first
}
```

A read-then-write lets two workers handed the same job by an at-least-once dispatcher both run
the handler. The engine treats `false` as "I lost the race" and bails out, releasing its gate
slot — so the boolean is not advisory.

Two softer contracts: `completeStep` must atomically increment the workflow's `completedSteps`
and `failStep` its `failedSteps`. Those counters are used for progress reporting only —
readiness and termination are always recomputed from `listSteps` — so getting them slightly
wrong degrades a number rather than the engine.

## Transactional dispatch

Persisting a transition and enqueueing the jobs it unlocks are two writes. A crash between
them — an ordinary deploy is enough — leaves steps `pending` with no job behind them: a
workflow stalled forever, and invisible to
[`recoverStuckWorkflows`](/running/cancellation-and-recovery/), which only looks at
steps stuck in `running`.

Two **optional** capabilities close that window. Implement both and the engine commits the
write and its dispatches together:

```ts
// On the store: run fn in one transaction, hand it a transaction-bound store
// plus an opaque handle the dispatcher knows how to join.
runInTransaction?<T>(fn: (scope: TransactionalScope) => Promise<T>): Promise<T>;

// On the dispatcher: enqueue onto that handle.
enqueueStepIn?(handle: unknown, payload: DispatchStepPayload, options?: EnqueueOptions):
  Promise<Result<void, FlowErrorShape>>;
```

`TransactionalScope` is `{ store, handle }`. The engine never inspects `handle` — it belongs
to the adapter pair. `store-pg` hands back its transaction-bound `SqlExecutor`, and
`createPgBossDispatcher` feeds that to pg-boss's `SendOptions.db`.

The engine **negotiates at construction**:

| Store has `runInTransaction` | Dispatcher has `enqueueStepIn` | Behaviour |
|---|---|---|
| yes | yes | write + dispatches commit atomically |
| either missing | | write, then enqueue — the previous behaviour, unchanged |

So implement this only when the queue lives in the same database as the store. pg-boss on the
same Postgres does; SQS or a separate Redis cannot, and omitting both fields is a perfectly
valid backend.

Implementations of `runInTransaction` MUST run every operation on `scope.store` inside the same
transaction, and MUST roll back if `fn` throws.

Two deliberate boundaries in the engine's use of it:

- **Only writes and enqueues go inside the transaction.** The failure path runs saga
  compensation — user handlers that may do network I/O — so it stays outside. A rollback
  handler must never hold a database transaction open.
- **`workflow.started` is emitted after commit**, so an observer never records a workflow that
  rolled back.

Coverage today is `startWorkflow` and step completion — the two paths that run on every
workflow. Map fan-out and sub-workflow starts still write-then-enqueue; those are covered by
the redelivery repair, where a redelivered job for an already-`completed` step re-drives
readiness instead of no-opping.

## Validating a definition

`buildWorkflow` checks only that every dependency key names a real step, and **throws** if not.
The full check runs on start, and is available directly:

```ts
const check = engine.validateDefinition(wf.definition); // Result<void, FlowError>
if (!check.ok) console.error(check.error.message);
```

It rejects an empty step list, duplicate keys, unknown or self dependencies, cycles (Kahn's
algorithm), and any step whose `type` has no handler in the registry. `startWorkflow` runs it
first and returns the same error, so calling it yourself is for fail-fast at boot rather than
per-start.
