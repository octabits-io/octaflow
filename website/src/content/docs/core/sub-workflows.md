---
title: Sub-workflows
description: A step starts a child workflow and awaits its result.
---

```ts
const enrich = defineSubWorkflowStep({
  type: 'enrich',
  workflowInputSchema,
  childWorkflow: enrichmentWorkflow,                 // a built workflow
  input: (ctx) => ({ listingId: ctx.workflowInput.id }),
  outputSchema: enrichmentOutputSchema,              // see below — it's the child's step map
  dependencies: { fetchListing },
});
```
Starts the child workflow (same partition), suspends the parent step as `waiting`, and resumes
it with the child's output when the child terminates. A failed or cancelled child fails the
parent step, which cascades normally.
→ [`examples/09-sub-workflows.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/09-sub-workflows.ts)

## The output is the child's *whole step map*

This is the part that surprises people. A workflow has no "return value" — its output is an
object keyed by **step key**, holding each completed step's output. That object, verbatim, is
what the parent step completes with:

```ts
const enrichmentWorkflow = buildWorkflow({
  type: 'enrichment', inputSchema,
  steps: { geocode, classify },        // ← these keys
});

// so the parent step's output is:
// { geocode: { lat, lng }, classify: { category } }
```

Which makes the parent's `outputSchema` a schema over that map:

```ts
const enrichmentOutputSchema = z.object({
  geocode:  z.object({ lat: z.number(), lng: z.number() }),
  classify: z.object({ category: z.string() }),
});

// downstream:
ctx.deps.enrich.geocode.lat
```

Omit `outputSchema` and the step's output is typed as an opaque `Record<string, unknown>` —
fine when nothing downstream reads it, awkward when something does.

Two details that follow from "it's the completed-step map": **skipped steps are absent** from
it (so a child with a conditional branch produces a partial object — make those keys optional),
and adding a step to the child workflow changes the parent's output shape.

## Registration is automatic

Pass the built child workflow and its step handlers are registered alongside the parent's when
you call `wf.register(registry)` — you do not register the child separately. That is why
`childWorkflow` is typed as `{ definition, register }` rather than just a definition, and why
the child must share the parent's `TContext`.

## Lifecycle

1. The parent step's `input(ctx)` runs like an ordinary handler — its deps are validated and
   typed. Its "output" is the child's input.
2. The engine marks the parent step `waiting` **first**, then starts the child. (In that order,
   so a child that finishes immediately always finds its parent suspended.)
3. The child runs as a full workflow of its own: its own row, its own steps, its own retries,
   its own event stream. It carries `parentWorkflowId` / `parentStepId` linkage — which the
   [public view](/extending/http/) strips.
4. When the child reaches a terminal state, the engine bridges it: `completed` completes the
   parent step with the child's output; `failed` or `cancelled` fails it with a
   `Sub-workflow failed: …` message.

The bridge is idempotent — it does nothing unless the parent step is still `waiting`.

## What it does not give you

- **No recursion guard.** A workflow that starts itself will keep starting itself. Bound the
  depth yourself (pass a counter through the child's input and stop at a limit).
- **Cancelling the parent does not cancel the child.** `cancelWorkflow` skips the parent's
  `waiting` step and finishes the parent; the child keeps running and its eventual bridge finds
  a non-`waiting` parent step and does nothing. Cancel children explicitly if you need that.
- **No shared transaction.** Parent and child are separate workflows; a child's completed side
  effects survive a parent failure unless you compensate them.
- **Sub-workflow starts are not yet transactional.** Unlike `startWorkflow` and step completion,
  this path still writes then enqueues — see
  [transactional dispatch](/extending/interfaces/#transactional-dispatch).
