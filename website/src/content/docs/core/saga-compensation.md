---
title: Saga compensation
description: Run rollback handlers in reverse dependency order when a workflow fails.
---

```ts
const reserve = defineStep({
  type: 'reserve', workflowInputSchema, outputSchema: z.object({ ticketId: z.string() }),
  handler: async () => ({ ticketId: await reserveSeat() }),
  compensate: async (ctx) => { await releaseSeat(ctx.output.ticketId); }, // undo on later failure
});
```
On workflow failure the engine runs each completed step's `compensate` in **reverse dependency
order** (`compensating` → `compensated`).
→ [`examples/10-saga-compensation.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/10-saga-compensation.ts)

## When it runs

Only on the **failure** path — the moment the workflow is finalized as `failed`, after every
step has reached a terminal state. Specifically:

| Ending | Compensation |
|---|---|
| a step fails terminally (retries exhausted) | **yes** |
| a map item fails, failing its parent | **yes** |
| a sub-workflow child fails, failing the parent step | **yes**, in both parent and child |
| `engine.cancelWorkflow(id)` | **no** — cancel is not failure |
| workflow completes | **no** |

Cancellation deliberately does not compensate. If you need rollback on a cancel, do it yourself
after `cancelWorkflow` returns.

## Which steps, and in what order

Only steps that are **`completed`** and whose type actually registered a `compensate` handler.
A step that failed, was skipped, or never ran has nothing to undo.

Order is a reverse topological sort over *that subset* — each step is compensated before the
dependencies it was built on, so effects come apart in the opposite order they were made.
Steps are compensated **one at a time, sequentially**, not in parallel; a diamond's two
branches unwind in a deterministic but unspecified relative order.

## The handler

`compensate` receives the same typed context as the step's handler, plus `output` — the thing
to undo:

```ts
compensate: async (ctx) => {
  ctx.output.ticketId;    // this step's own output
  ctx.deps.findSeat.row;  // dependency outputs, still available
  ctx.workflowInput;      // and the workflow input
}
```

The context is re-derived best-effort: if a schema no longer matches what was persisted, the
raw stored value is passed through rather than failing the rollback. `buildStepContext` runs
again too, so `ctx.context` is live — a compensation handler can use your DI scope and domain
services exactly like a step handler.

## Best-effort, by design

- **One attempt per step.** `retry` does not apply to compensation.
- **A throw is caught, logged, and surfaced** on the step as `compensated` with a
  `Compensation failed: …` error — and the remaining steps still compensate. One failed
  rollback never blocks the others.
- **There is no "compensation failed" workflow state.** The workflow is already `failed`;
  inspect the individual steps (or the `step.compensated` events, which carry `error`) to find
  rollbacks that didn't take.
- **It runs outside the engine's transaction.** Rollback handlers do network I/O, and a
  transaction must never be held open across that — so compensation is not atomic with the
  failure that triggered it.

Make handlers **idempotent**. A crash mid-compensation is not resumed: the sweep is driven by
the failure finalization, and a step already marked `compensating` is not retried by anything.

## How it surfaces

`compensating` folds to the display state `running` and `compensated` to `skipped` in the
[public view](/extending/http/) — a rolled-back step reads as "didn't happen", with the
failure reported by the step that actually failed. Watch the `step.compensating` /
`step.compensated` [events](/running/observability/) if you need the real states.
