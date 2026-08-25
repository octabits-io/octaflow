---
title: Conditional branching
description: Guards that skip a step and its branch, and join rules that let the branches converge again.
---

The DAG is fixed at definition time. Which parts of it *run* is not.

A `when` guard decides whether a step executes at all. A step whose guard says no is
marked `skipped`, and so is everything reachable only through it. That gives you if/else
without giving up the graph you can inspect before anything runs.

```ts
const triage = defineStep({
  type: 'triage',
  workflowInputSchema: input,
  outputSchema: z.object({ priority: z.enum(['high', 'normal']) }),
  handler: async (ctx) => ({ priority: ctx.workflowInput.amount >= 1000 ? 'high' : 'normal' }),
});

const expedite = defineStep({
  type: 'expedite',
  dependencies: { triage },
  when: (ctx) => ctx.deps.triage.priority === 'high',
  handler: async () => ({ handledBy: 'priority-desk' }),
  // …
});
```

→ [`examples/15-conditional-branching.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/15-conditional-branching.ts)

## What the guard sees, and when

The guard runs on the worker, after the step has been claimed and its dependency outputs
resolved — so it receives **exactly the context the handler would**: validated
`workflowInput`, parsed `deps`, `stepInput`, and your `context`.

That costs a queue round trip for a step that turns out to be skipped. It buys two
things: the guard can do I/O (look up a feature flag, ask a service) without the engine
holding a database transaction open, and a guard that fails is a step failure rather
than a silent decision.

:::caution[A throwing guard is not "false"]
If your guard throws, the step is **failed**, not skipped — classified exactly like a
failing handler, so a transient error is retried under the step's `retry` policy and a
permanent one fails the run.

That is deliberate. The alternative — treating an error as "branch not taken" — turns a
flaky lookup into a workflow that quietly skipped the branch that mattered. If you *do*
want a lookup failure to mean "don't run", catch it inside the guard and return `false`.
:::

## Joins: `join: 'all'` and `join: 'any'`

Here is the trap. By default a step needs **every** dependency to have completed
(`join: 'all'`), and a dependency that was skipped makes it unreachable. So the obvious
convergence doesn't work:

```
        ┌── expedite ──┐
 triage                ├── notify      ← skipped, because one arm was skipped
        └── standard ──┘
```

`join: 'any'` is the fix: run once **every dependency has settled and at least one
completed**.

```ts
const notify = defineStep({
  type: 'notify',
  dependencies: { expedite, standard },
  join: 'any',
  handler: async (ctx) => {
    // Exactly one arm ran, so both are typed as possibly-absent.
    const handledBy = ctx.deps.expedite?.handledBy ?? ctx.deps.standard?.handledBy;
    // …
  },
});
```

| | `join: 'all'` (default) | `join: 'any'` |
|---|---|---|
| every dependency completed | runs | runs |
| some completed, some **skipped** | skipped | runs |
| **all** skipped | skipped | skipped |
| any dependency **failed** | skipped | skipped |
| any dependency still in flight | waits | waits |

Two consequences worth reading twice:

- **`'any'` is not "as soon as one lands".** It waits for the others to settle first, so
  the step runs exactly once with a stable view of which branch won. A step that fired on
  the first completion could run while the other arm was still going.
- **A *failed* dependency still poisons an `'any'` join.** The workflow is failing
  regardless, and running a notification step on the way down is rarely what you want.
  Error branches are not what `'any'` is for.

Under `join: 'any'` the `deps` type becomes possibly-absent per branch — the type system
knows a skipped arm has no output to offer, so you have to handle it.

## Where the decision lives

`when` and `join` are properties of the step's **type**, registered alongside its handler
and retry policy — the same as `timeoutMs` or `delayMs`. Two steps in the same workflow
that should branch differently need two step types.

The readiness rules themselves are a pure function (`computeReadiness`), which is why a
skip cascades correctly through a chain in one pass: skipping `a` skips `b`, which skips
`c`, in a single fixpoint rather than one transition at a time.

## What this is not

Flow can pick a branch. It cannot invent a step. There is still no loop, no
`continue-as-new`, and no way to add a node to the graph at runtime — for a runtime-sized
*number* of parallel items use [fan-out & map](/core/fan-out-and-map/), and for a
genuinely different shape use a [sub-workflow](/core/sub-workflows/).

If your process is "loop until a human approves, branching on whatever they typed", an
imperative durable function will express it more naturally than a static DAG will.
