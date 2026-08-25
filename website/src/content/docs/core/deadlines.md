---
title: Deadlines
description: A budget for a suspended step, and a budget for a whole run.
---

[Retry & timeout](/core/retry-and-timeout/) bounds how long a handler may *run*.
This page is about the two waits it does not cover: a step suspended on an event that
never arrives, and a run that drifts past the point where finishing it still matters.

## A budget for a suspended step

Give a [wait step](/core/signals/) a `timeoutMs` and it cannot wait forever:

```ts
const approval = defineWaitStep({
  type: 'await-approval',
  outputSchema: z.object({ approved: z.boolean() }),
  timeoutMs: 48 * 60 * 60 * 1000,   // 48 hours
  onTimeout: { output: { approved: false } },
});
```

`onTimeout` decides what the expiry means:

| | Effect |
|---|---|
| `'fail'` *(default)* | The step fails; the workflow fails with it, dependents are skipped and compensation runs. |
| `{ output }` | The step **completes** with that output and the DAG carries on. |

`{ output }` is the interesting one. Paired with a
[`when` guard](/core/branching/) it is how "approve within 48 hours, otherwise
escalate" is expressed — the wait ends in a defined answer, and the branch does the rest:

```ts
const publish  = defineStep({ dependencies: { approval }, when: (c) =>  c.deps.approval.approved, /* … */ });
const escalate = defineStep({ dependencies: { approval }, when: (c) => !c.deps.approval.approved, /* … */ });
```

→ [`examples/16-deadlines-and-retry.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/16-deadlines-and-retry.ts)

`timeoutMs` is the same field a normal step uses for its handler budget — one name for
"how long this step may take", whether it spends the time working or waiting. A step
cannot do both, so there is no ambiguity.

### How it survives a restart

Suspending a step enqueues a **deadline job** on the same durable queue, with the same
delay mechanism a [sleep step](/core/durable-sleep/) uses. Nothing is held in
memory, so a redeploy doesn't lose it.

Two properties fall out of that, both of which the engine re-checks when the job fires:

- **It may arrive late, or twice.** If the step already resumed, failed, or was cancelled,
  the deadline job is a logged no-op.
- **It may arrive early**, or for a suspension younger than the job (a step that was reset
  by [`retryWorkflow`](/running/cancellation-and-recovery/#retrying-a-failed-run)
  and suspended again). The engine measures the elapsed wait from when the step actually
  suspended and re-arms for whatever is left, rather than firing early.

:::caution[Your worker must route by job kind]
The queue now carries two kinds of job for a step: run it, and settle its deadline. Call
**`engine.handleStepJob(payload)`** from your worker, not `executeStep` — the payload's
`kind` field is what tells them apart.

```ts
await worker.start(async (payload) => {
  await engine.handleStepJob(payload);
});
```

A worker still calling `executeStep` will silently never time anything out.
:::

A wait step **without** `timeoutMs` still waits indefinitely. That remains a valid choice
— just an explicit one now.

## A budget for the whole run

`StartOptions.timeoutMs` puts a wall-clock deadline on a workflow, stored as an absolute
`deadlineAt` on the row:

```ts
await wf.start(engine, input, { timeoutMs: 30 * 60 * 1000 });   // 30 minutes
```

Once it passes, the workflow fails — with pending and waiting steps skipped, compensation
run, and a `workflow.failed` event whose error names the deadline. It applies wherever the
run happened to be: executing, sitting in the queue, or suspended on an event.

It is enforced in two places:

- **When a step is picked up.** The check runs before the step is claimed, so a queued
  step never starts work for a run that is already over.
- **By [`recoverStuckWorkflows`](/running/cancellation-and-recovery/).** Which
  matters, because a run that is *suspended* has nothing trying to start — without the
  sweeper its deadline would never be noticed. The sweeper's cadence is therefore the
  resolution of a run deadline; if you need it enforced promptly, sweep more often.

:::note[A running step is not interrupted]
Same as [cancellation](/running/cancellation-and-recovery/): there is no signal
into another worker's in-flight handler. That step finishes, and its completion lands on a
workflow that has already failed, where it goes no further. Use a per-step `timeoutMs` to
bound the handler itself.
:::

### Choosing between them

| You want | Use |
|---|---|
| "this HTTP call can't take more than 30s" | step `timeoutMs` ([retry & timeout](/core/retry-and-timeout/)) |
| "this approval expires after 2 days" | wait-step `timeoutMs` + `onTimeout` |
| "this whole order must clear within an hour or it's void" | `StartOptions.timeoutMs` |
| "stop this specific run now" | [`cancelWorkflow`](/running/cancellation-and-recovery/) |

The Postgres store adds `deadline_at` to `flow_workflow`. `flowStoreDdl()` emits an
`ALTER TABLE … ADD COLUMN IF NOT EXISTS` alongside the `CREATE TABLE`, so re-applying the
DDL migrates an existing database; if you host the tables in your own migrations, add the
column there.
