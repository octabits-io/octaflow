---
title: Fan-out & map
description: Spawn one child step per item of a runtime-sized list.
---

The DAG is static, but its *width* doesn't have to be. `defineMapStep` computes a list at run
time and spawns one child step per item.

```ts
const resizeAll = defineMapStep({
  type: 'resize-all',
  workflowInputSchema,
  itemOutputSchema: z.object({ url: z.string() }),
  dependencies: { listImages },
  items: (ctx) => ctx.deps.listImages.urls,          // runtime-sized list
  each: async (url, info) => ({ url: await resize(url, info.index) }),
});
// downstream reads resizeAll.items: { url: string }[]
```
→ [`examples/07-dynamic-map.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/07-dynamic-map.ts)

## The two halves

A map step registers **two** handlers: the parent (which produces the item list) and a per-item
child. `each` receives the item plus `info`:

```ts
each: async (item, info) => { … }
// info: { index: number; context: TContext; workflowId: number; partitionKey: string }
```

Note what `info` does *not* carry: no `deps`. An item handler cannot read dependency outputs —
close over what it needs in `items`, or fold it into the item itself.

## Lifecycle

1. The parent runs `items(ctx)` with a normal typed context (deps validated), producing the list.
2. The engine inserts one child step per item, keyed `parentKey#0`, `parentKey#1`, … with step
   type `parentType__item`, and marks the parent **`mapping`**.
3. Every child is enqueued — see [scale](#scale) — and each runs `each` with its own attempt
   budget (`itemRetry`), timeout (`itemTimeoutMs`), retryability predicate (`itemIsRetryable`)
   and gate admission.
4. When all children complete, the parent completes with `{ items: [...] }` in **index order**,
   validated per item against `itemOutputSchema`.

An **empty list short-circuits**: the parent completes immediately with `{ items: [] }` and no
children are created. Downstream steps run normally with an empty array — worth handling, since
"no work to do" is a success, not a skip.

## Failure

A failed item fails the whole map. The child's terminal failure fails the parent step, still-
`pending` siblings are marked `skipped` ("sibling map item failed"), and the workflow cascades
to `failed` once the already-running siblings settle.

There is no partial-success mode. If you want per-item error tolerance, make it part of the item
output and never throw:

```ts
itemOutputSchema: z.object({ url: z.string().nullable(), error: z.string().nullable() }),
each: async (url) => {
  try { return { url: await resize(url), error: null }; }
  catch (e) { return { url: null, error: String(e) }; }
},
```

## Scale

**Every child is enqueued at once.** There is no built-in batching or concurrency limit on the
fan-out itself — a 10,000-item list means 10,000 queue rows, immediately. What throttles actual
execution is the [step gate](/core/concurrency-and-rate-limits/), keyed on the child's
step type:

```ts
const gate = createInMemoryStepGate({
  concurrency: { 'resize-all__item': { maxConcurrent: 8 } },
});
```

The child's type is the parent's type plus `__item` (`resize-all` → `resize-all__item`), so cap
that type rather than the parent's — capping the parent only limits how often the *list* is
computed. The same suffix is what you match on for per-item rate limits and metrics. For
lists in the tens of thousands, prefer chunking — make each item a batch of 100 — over relying
on the gate to hold back a queue that large.

Two more limits worth knowing: the aggregated `{ items: [...] }` output is stored as a single
row value, so a very large fan-out with large per-item outputs makes a very large row; and map
fan-out still writes-then-enqueues rather than committing
[transactionally](/extending/interfaces/#transactional-dispatch), relying on job
redelivery as its repair path.
