---
title: Heartbeats
description: Let a long step prove it is alive, so a dead one can be caught in seconds.
---

Without a heartbeat, `startedAt` is the only liveness signal the engine has. The
[sweeper](/running/cancellation-and-recovery/) asks "did this step start longer ago
than the stuck threshold", and has to read that as "is the worker dead". Those are different
questions, and one number cannot answer both:

- **A short threshold** condemns a legitimately long step — a transcode, a big export, a slow
  model call — while it is happily working.
- **A long threshold** (the 15-minute default) leaves a step whose pod was evicted after three
  seconds sitting in `running` for a quarter of an hour.

A heartbeat replaces *started a while ago* with *hasn't spoken recently*, which is the question
you actually wanted asked. Then the window can be short.

```ts
const transcode = defineStep({
  type: 'transcode',
  heartbeatTimeoutMs: 2 * 60 * 1000,   // silence for 2 minutes ⇒ presumed dead
  handler: async (ctx) => { /* … */ },
});
```

That is the whole opt-in. The engine beats automatically while the handler runs, so a step
gets fast crash detection without the handler being touched — if the process dies, the timer
dies with it, which is exactly the signal.

→ [`examples/16-deadlines-and-retry.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/16-deadlines-and-retry.ts)

It is **opt-in per step type**. A step that declares no `heartbeatTimeoutMs` is judged exactly
as it is today, by `stepExpirySeconds + stuckStepBufferSeconds` from when it started. Map items
get theirs from `defineMapStep`'s `itemHeartbeatTimeoutMs` — per-item work is the classic
long-running case.

## Why this matters more since crashed steps are re-queued

`recoverStuckWorkflows` now [re-queues a crashed step](/running/cancellation-and-recovery/)
rather than failing it. That makes a *false* positive more expensive than it used to be: the
sweeper marks the step `pending` and enqueues it while the original invocation is **still
running**, so a worker claims it and the handler executes twice, concurrently. The atomic claim
does not help — it stops two workers racing for one job, not the sweeper resurrecting a step
that was never dead.

A heartbeat is what removes the false positive, rather than just making it unlikely. And the
beat's return value closes the gap from the other end, below.

## `ctx.heartbeat()` — the cancellation channel

The beat is already a round trip to the database, so its answer carries more than an
acknowledgement:

```ts
handler: async (ctx) => {
  for (const chunk of chunks) {
    await process(chunk);
    if (!(await ctx.heartbeat())) return;   // no longer ours to run
  }
}
```

It resolves `false` when the step is no longer this invocation's, which means one of three
things, all of which mean *stop*:

- the workflow was **cancelled**;
- it blew its [deadline](/core/deadlines/);
- the **sweeper re-queued this step**, and someone else now owns it.

The engine does two things on a `false`, whether it came from your call or from the automatic
timer. It fires `ctx.signal`, so a handler that already respects the abort signal stops without
any code change — which is the first time cancelling a run has been able to interrupt a step
that was already executing. And it **discards the handler's outcome**, so a superseded
invocation cannot stamp its result over the new owner's work.

Calls are cheap: writes are throttled to roughly one per third of the window, so calling it per
loop iteration is fine.

:::note[Not a substitute for `timeoutMs`]
Under the default `heartbeat: 'auto'`, a handler stuck in an infinite loop still beats happily
forever — the engine's timer knows the process is alive, not that the work is going anywhere.
A per-step [`timeoutMs`](/core/retry-and-timeout/) is what bounds a hang. The two are
complementary: `timeoutMs` bounds total work, the heartbeat bounds silence.

Set `heartbeat: 'manual'` to suppress the automatic timer, and silence then means a hung
handler too — at the cost of having to place the calls yourself.
:::

## What it can't do

Heartbeats are cooperative. A handler making one uninterruptible ten-minute call has nowhere to
put a beat, and gets only the automatic timer's process-liveness from it — no hang detection,
no cancellation. Every engine has this limit; Temporal's activity heartbeats included.

## Choosing a window

`heartbeatTimeoutMs` should be comfortably longer than the slowest gap between beats you
expect. Under `'auto'` the engine beats every `heartbeatTimeoutMs / 3` (at least once a
second), so two beats can be lost before the sweeper draws a conclusion. Under `'manual'`,
size it against your slowest loop iteration, not the average.

The sweeper is what acts on the verdict, so its cadence is your real detection floor: a
30-second window with a sweep every five minutes still means a five-minute wait. Run the sweep
at least as often as your shortest window if you want the number to mean anything.

Each beat is one `UPDATE` on `flow_workflow_step`, and each needs a store connection for its
brief life — the same budgeting note as
[worker concurrency](/running/postgres-and-pg-boss/).

The Postgres store adds `heartbeat_at` to `flow_workflow_step`. `flowStoreDdl()` emits an
`ALTER TABLE … ADD COLUMN IF NOT EXISTS` alongside the `CREATE TABLE`, so re-applying the DDL
migrates an existing database; if you host the tables in your own migrations, add the column
there.
