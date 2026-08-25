---
title: Concepts
description: Step, workflow, registry, store, dispatcher, engine, partition — and how the engine self-advances.
---

- **Step** — a unit of work with a Zod `workflowInputSchema`, an `outputSchema`, optional
  `dependencies`, and a `handler`. Created with `defineStep` (or the variants below).
- **Workflow** — a named DAG of steps. `buildWorkflow({ type, inputSchema, steps })` derives the
  dependency graph and validates at build time that every dependency key references a real step
  (throwing if not). The deeper checks — duplicate keys, self-dependencies, cycles, missing
  handlers — run on start, as a returned `Result`.
- **Step key vs. step type** — the **key** is the property name under `steps:`; it identifies the
  step within one workflow and is what `ctx.deps`, `resumeStep` and the workflow output are keyed
  by. The **type** names the handler in the registry and may repeat across workflows.
- **Registry** — maps a step's `type` string to its handler + policy. `wf.register(registry)`
  populates it; the engine looks handlers up by type at run time.
- **Store** (`WorkflowStore`) — persistence for workflow + step rows. `createInMemoryWorkflowStore()`
  for tests/single-process; `createPgWorkflowStore()` for production.
- **Dispatcher** (`Dispatcher`) — enqueues a step to run. The engine calls it to schedule ready
  steps (and retries/sleeps via `startAfterSeconds`). In-process array for tests;
  `createPgBossDispatcher()` for a durable queue.
- **Engine** — `createWorkflowEngine({ store, dispatcher, registry, partitionKey, ... })`. Orchestrates
  readiness, parallelism, retries, failure cascade, and crash recovery. Bound to one **partition**.
- **Partition** — a tenancy boundary (`partitionKey`, e.g. a tenant id) stamped on every row and
  job. One engine instance serves one partition.
- **`Result<T, E>`** — every fallible call returns `{ ok: true, value }` or `{ ok: false, error }`;
  expected failures are values, not exceptions.

The engine is **self-advancing**: starting a workflow enqueues its dependency-free roots; as each
step completes the engine enqueues newly-ready steps — so parallelism and fan-in happen
automatically.

## How a workflow ends

A workflow is finalized when **every** step has reached a terminal state (`completed`, `failed`
or `skipped`) and nothing new became ready. Then:

- all completed → `completed`, with output `{ [stepKey]: stepOutput }` for each completed step
  (skipped steps are absent from it)
- any failed → `failed`, carrying the first failure's message

A failed step (after its retries) **cascades**: dependents still `pending` are marked `skipped`,
iterating to a fixpoint so the skip propagates down a whole chain — `a` fails, `b` is skipped,
then `c` which depended on `b`.

:::note[Failure is not immediate]
Because finalization waits for *every* step to settle, a failure on one branch does not stop
independent branches — they run to completion first, and only then does the workflow flip to
`failed`. A long-running parallel step will hold a doomed workflow in `running` until it
finishes.
:::

Once `failed`, the engine runs [saga compensation](/core/saga-compensation/) over the
completed steps, then bridges the failure to a parent step if this workflow is a
[sub-workflow](/core/sub-workflows/) child. A workflow can also be ended deliberately
with [`cancelWorkflow`](/running/cancellation-and-recovery/), which skips pending and
waiting steps but does **not** compensate.
