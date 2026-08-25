---
title: Durable sleep
description: Hold a step in the queue so the delay survives a restart.
---

```ts
const cooldown = defineSleepStep({
  type: 'cooldown',
  sleepMs: 60 * 60 * 1000,     // an hour
  dependencies: { charge },
});
```
A no-op step held in the queue for `sleepMs` once ready — durable across restarts, because
the delay lives in the queue, not in memory. Nothing is blocked while it waits: no worker,
no connection, no process.
→ [`examples/04-durable-sleep.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/04-durable-sleep.ts)

`defineSleepStep` is just `defineStep` with an empty handler and `delayMs` set, so any step can
delay its own start the same way:

```ts
const poll = defineStep({ type: 'poll', workflowInputSchema, outputSchema,
  delayMs: 30_000,             // wait 30s after becoming ready, then run
  handler: async () => { … },
});
```

The difference is only intent: a sleep step models "wait, then continue"; `delayMs` on a real
step models "this work shouldn't start immediately".

## How the delay is applied

When a step becomes ready the engine enqueues it with `startAfterSeconds`, and the dispatcher
holds the job. Two consequences:

- **Resolution is whole seconds.** `sleepMs` is converted with `Math.ceil`, so `1500` sleeps
  2 s and anything under 1000 ms sleeps 1 s. Millisecond precision isn't available, and
  wouldn't survive a queue anyway.
- **It needs a dispatcher that honours `startAfterSeconds`.** pg-boss does. A naive in-process
  array dispatcher that ignores the option will run the step immediately — which is exactly
  what the in-memory examples do, so sleeps appear instant there unless the driver implements
  the delay.

The wait is a lower bound, not a schedule: the step becomes *eligible* after `sleepMs` and runs
whenever a worker next picks it up.

## Long sleeps

There is no ceiling — a sleep of days or weeks is a row in the queue, and costs nothing while
it waits. Two things to keep in mind at that timescale:

- A sleep step is an ordinary step, so it counts toward the workflow's step total and shows as
  `pending` (display state `pending`) for the whole duration. A workflow sleeping for a week
  sits in `running` for a week.
- It is **not** swept by [`recoverStuckWorkflows`](/running/cancellation-and-recovery/),
  which only looks at steps stuck in `running`. A queued-but-not-yet-due step is not stuck.
