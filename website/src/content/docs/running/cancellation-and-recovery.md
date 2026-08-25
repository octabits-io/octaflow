---
title: Cancellation & recovery
description: Stop a workflow on purpose, clear the ones a crashed worker left behind, and put a failed one back in flight.
---

Three engine methods handle the endings that aren't "every step completed": one you
call deliberately, one you run on a timer, and one you reach for when a run failed for
a reason you have since fixed.

## Cancelling a workflow

```ts
const cancelled = await engine.cancelWorkflow(workflowId);
if (!cancelled.ok) throw new Error(cancelled.error.message); // workflow_not_found
```

Every `pending` **and `waiting`** step is marked `skipped`, the workflow finishes
`cancelled`, and a `workflow.cancelled` event is emitted. Three things worth knowing:

- **It interrupts a running step only if that step beats.** There is no signal into
  an in-flight handler on its own, so by default the step runs to completion and its
  result lands on a workflow that has already finished. A step with a
  [heartbeat](/core/heartbeats/) learns it was cancelled on its next beat:
  the engine fires `ctx.signal` and discards whatever the handler returns. Without
  one, cancellation remains a "stop scheduling more work" operation, not a kill.
- **Compensation does not run.** Saga rollback is wired to the *failure* path only
  (see [Saga compensation](/core/saga-compensation/)). If you need completed
  steps undone on a cancel, do it yourself after the call returns.
- **Cancelling a sub-workflow child fails the parent step**, which cascades into the
  parent workflow exactly as a child failure would.

Calling it on a workflow that already reached a terminal state is a no-op that still
returns `ok` — so a double-click on a cancel button is safe.

## Recovering after a crash

A worker that dies mid-step leaves its step in `running` forever. Nothing detects that
on its own: the queue's job expiry releases the *job*, but the row stays claimed, and
an atomic claim (`markStepRunning`) means a redelivered job can't take it over.

`recoverStuckWorkflows` is the sweeper that clears them:

```ts
const { retriedSteps, recoveredSteps, recoveredWorkflows, expiredWorkflows } =
  await engine.recoverStuckWorkflows();
```

It scans every `running` workflow in the partition and does three things:

- **Re-queues a crashed step** that has been silent longer than its liveness window,
  provided its attempt budget has room. A dead pod costs an attempt, not the run.
- **Fails a crashed step** whose budget is spent, cascading the workflow to `failed` in
  the usual way (dependents skipped, compensation run).
- **Fails a run past its [deadline](/core/deadlines/)**, which nothing else
  would notice while the run is suspended or idle.

:::caution[Your handlers must tolerate re-execution]
A re-queued step runs again from the top, and the first attempt may have got partway
through its side effects before the worker died. This is the same contract retries
already impose — but it now applies to crashes too, which it did not before.

Worse, on a *false* positive the re-run is **concurrent** with an original invocation
that never actually died. A [heartbeat](/core/heartbeats/) is what removes
that case rather than making it unlikely: a live step keeps proving it, and one that
was superseded anyway finds out on its next beat and discards its outcome.

If re-entering a half-finished step is worse than losing the run, set
`config.onStuckStep: 'fail'` and a stuck step is failed outright whatever its budget
says.
:::

Nothing calls this for you. Run it on a schedule in one process — a cron job, a
`setInterval`, or a pg-boss schedule:

```ts
setInterval(() => {
  void engine.recoverStuckWorkflows().catch((e) => logger.error('sweep failed', e));
}, 60_000);
```

Once per partition is enough. Two sweepers racing on the same partition will not corrupt
state — the step transition is a plain `UPDATE` and the cascade is recomputed from
`listSteps` — but each one increments the workflow's `failedSteps` counter, so a
double sweep can inflate that progress number. Run it from one process, or accept the
skew.

### The stuck threshold

```
stuckThreshold = stepExpirySeconds + stuckStepBufferSeconds
```

This is the **default** window, measured from when a step started. A step type that
declares [`heartbeatTimeoutMs`](/core/heartbeats/) overrides it with its own,
measured from when the step last reported in — which is what lets the window be short
without condemning work that is merely slow.

Both come from `WorkflowEngineConfig`, and both have defaults:

| Option | Default | Meaning |
|---|---|---|
| `stepExpirySeconds` | `600` | What you told the *dispatcher* a step may occupy a worker for |
| `stuckStepBufferSeconds` | `300` | Grace on top, so a step that is merely slow isn't swept |
| `onStuckStep` | `'retry'` | `'retry'` re-queues within the attempt budget; `'fail'` always fails |

Per step type, `heartbeatTimeoutMs` replaces the first two entirely for that type.

```ts
const engine = createWorkflowEngine({
  store, dispatcher, registry, partitionKey,
  config: { stepExpirySeconds: 900, stuckStepBufferSeconds: 300 }, // sweep at 20 min
});
```

:::danger[Keep this in sync with the queue]
`stepExpirySeconds` is **not** enforced by the engine — it is the engine's *copy* of
the dispatcher's expiry, used only to compute the threshold. With pg-boss the real
value is `StepQueueConfig.expireInSeconds` (also 600 by default). Change one and you
must change the other:

```ts
const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey,
  config: { expireInSeconds: 900 } });
const engine = createWorkflowEngine({ …, config: { stepExpirySeconds: 900 } });
```

Set the engine's value **too low** and the sweeper fails steps that are still legitimately
running. Set it **too high** and genuinely dead work sits in `running` for longer than
it needs to.
:::

## What recovery does not cover

The sweeper only looks at steps stuck in `running`. A step stuck in `pending` with no
job behind it is invisible to it — that is the dual-write window between persisting a
transition and enqueueing the jobs it unlocks.

Closing that window is the job of
[transactional dispatch](/extending/interfaces/#transactional-dispatch): with
`store-pg` + `dispatcher-pgboss` on one Postgres, the write and its enqueues commit
together and the window doesn't exist. On a queue that lives elsewhere (SQS, Redis) the
engine falls back to write-then-enqueue, and the repair path is the dispatcher
redelivering the *completed* step's job, which re-drives readiness.

## Retrying a failed run

A workflow that used up its retries is `failed`, and `failed` is terminal. Starting a
fresh run repeats every side effect the first one already committed — the charge, the
email, the file that was written. `retryWorkflow` is the alternative: resume the
*existing* run from where it stopped.

```ts
const retried = await engine.retryWorkflow(workflowId);
if (!retried.ok) throw new Error(retried.error.message);
retried.value.resetSteps;   // ['fulfil', 'notify'] — what will run again
```

Every step that **failed**, was **skipped** in the fallout, or had its work
**compensated** away goes back to `pending` with a fresh attempt budget, its output,
error and timestamps cleared. Steps that **completed** are left exactly as they are —
they keep their output, and they do not run again. The workflow returns to `running`
with recomputed counters, and the frontier is dispatched.

→ [`examples/16-deadlines-and-retry.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/16-deadlines-and-retry.ts)

Details worth knowing:

- **A compensated step *does* re-run.** Saga rollback undid its effects, so its work has
  to happen again — that is why `compensated` is reset alongside `failed` and `skipped`.
- **A map parent's children are dropped** before it re-runs, so it fans out afresh rather
  than aggregating two generations of items.
- **A guard is re-evaluated.** A step skipped by a [`when`](/core/branching/)
  guard is reset too, so the branch decision is made again against the current data.
- **It refuses a run that is not `failed`.** A `completed`, `running` or `cancelled`
  workflow returns a `workflow_not_retryable` error rather than being restarted.
- **It refuses a sub-workflow child**, pointing you at the parent. Retrying a child
  cannot un-fail the parent step that was waiting on it; retry the parent, and it starts
  a fresh child.
- **A `workflow.retried` event** is emitted for your run history.

:::note[This is an operator action, not an automatic one]
Nothing calls `retryWorkflow` for you, and it deliberately has no built-in loop — a run
that fails for a permanent reason would retry forever. Wire it to an admin endpoint, a
button in your dashboard, or a script; decide the policy yourself.
:::
