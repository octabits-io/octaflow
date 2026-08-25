---
title: Retry & timeout
description: Attempt budgets, backoff, wall-clock timeouts — and how a failure is classified as transient.
---

```ts
const flaky = defineStep({
  type: 'call-api', workflowInputSchema, outputSchema,
  retry: { maxAttempts: 4, backoff: 'exponential', initialDelayMs: 500, maxDelayMs: 30_000 },
  timeoutMs: 10_000, // aborts + retries on expiry
  handler: async (ctx) => { /* throw a retryable error to retry within budget */ },
});
```
A failure is retried (with backoff via the dispatcher's `startAfterSeconds`) up to `maxAttempts`;
after that the step fails terminally. → [`examples/03-retry-timeout.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/03-retry-timeout.ts)

:::caution[Without a `retry` policy, a step is attempted once]
`maxAttempts` defaults to **1** — no retry at all. Retrying is opt-in per step; there is no
engine-wide default policy. `maxAttempts` counts *total* attempts including the first, so
`{ maxAttempts: 4 }` means one run plus three retries.
:::

| Field | Default | Meaning |
|---|---|---|
| `maxAttempts` | `1` | Total attempts including the first |
| `backoff` | `'fixed'` | `'fixed'` or `'exponential'` (`initialDelayMs × 2^(n-1)`) |
| `initialDelayMs` | `1000` | Delay before the 2nd attempt |
| `maxDelayMs` | `60000` | Cap on the computed delay |

Backoff is delivered as the dispatcher's `startAfterSeconds`, so it is rounded **up to whole
seconds** — an `initialDelayMs` under 1000 still waits a second, and a dispatcher that ignores
the option retries immediately.

`timeoutMs` is a wall-clock bound on one attempt. On expiry the engine fires `ctx.signal` and
produces a **retryable** failure, so the step retries within its budget; a handler that ignores
the signal keeps running in the background, so pass `ctx.signal` to anything that accepts one.

**Which failures count as retryable** is decided in this order:

1. **An explicit marker on the error** — always wins.
   ```ts
   import { retryableError, nonRetryableError, markRetryable } from 'octaflow';

   throw retryableError('encoder busy');              // retry, whatever the message says
   throw nonRetryableError('timeout must be > 0');    // never retry — a bug, not a blip
   throw markRetryable(await client.readError(), true); // tag an error you didn't construct
   ```
   A marker is found **through `cause`**, so wrapping doesn't lose it:
   `new Error('upstream failed', { cause: retryableError('busy') })` still retries.
2. **The step's own predicate**, for classifying a whole family of errors at once:
   ```ts
   isRetryable: (e) => e instanceof HttpError && e.status >= 500,
   ```
   (`defineMapStep` takes `itemIsRetryable` for its per-item children.)
3. **`isRetryableError`** — the zero-config default. It reads **structured fields first**:
   `code` (`ECONNRESET`, `ECONNREFUSED`, `ETIMEDOUT`, `EAI_AGAIN`, …) and HTTP status from
   `status` / `statusCode` / `response.status` (408, 425, 429 and 5xx except 501/505). Only
   then does it fall back to matching the **message** against a small vocabulary
   (`rate limit`, `timeout`, `fetch failed`, `service unavailable`, …).

That last fallback is a convenience, not a classifier — it can only judge wording. A genuine
bug reading `'timeout must be > 0'` looks transient to it. When the answer matters, mark the
error rather than phrasing it to suit the heuristic.

To stop guessing entirely, give the engine a `defaultRetryable`:

```ts
createWorkflowEngine({ …, config: { defaultRetryable: false } }); // strict: never guess
```

It replaces the classifier's answer **only where the classifier guessed**. Explicitly marked
errors, steps with their own `isRetryable`, and engine-generated failures like a step timeout
are unaffected.

## The other retry layer

The step's `retry` policy is not the only thing that re-runs work. With pg-boss there are
**two independent mechanisms**, and they trigger on different things:

| | Step retry (`RetryPolicy`) | Queue retry (`StepQueueConfig`) |
|---|---|---|
| Owned by | the engine | pg-boss |
| Fires when | the handler fails *and the failure is retryable* | `executeStep` **throws out** of the worker |
| Budget | `maxAttempts`, default 1 | `retryLimit`, default 2 |
| Delay | `backoff` / `initialDelayMs` | `retryDelay`, default 30 s |
| Exhaustion | step marked `failed`, workflow cascades | job → dead-letter queue → your DLQ worker calls `handleStepExhausted` |

A handler that throws is caught by `defineStep` and turned into a step failure, so it goes down
the **first** column — the queue never sees it. The second column is for failures *around* the
handler: the store is unreachable, the process is killed, the worker itself throws. That's why
production wiring needs a
[DLQ worker](/running/postgres-and-pg-boss/): without one, a job that exhausts its
queue retries leaves its step stuck in `running` until the
[stuck-step sweeper](/running/cancellation-and-recovery/) picks it up.

Being deferred by the [step gate](/core/concurrency-and-rate-limits/) is neither: a
throttled step is re-enqueued without consuming an attempt from either budget.
