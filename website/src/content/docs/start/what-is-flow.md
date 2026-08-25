---
title: What is Flow?
description: A declarative DAG workflow engine for TypeScript — the design choice, and what you get.
---

**Durable workflows for TypeScript that run on the Postgres you already have.**

Declare a DAG of Zod-typed steps. The engine runs each step as soon as its dependencies
complete, persists every transition, retries failures, and picks up where it left off after a
restart. There is
no workflow server to operate, no control plane, and no vendor — it's a library you import,
not a platform you adopt.

```ts
const wf = buildWorkflow({
  type: 'publish-article',
  inputSchema: z.object({ draftId: z.string() }),
  steps: { fetchDraft, summarize, translate, publish },
});
```

`summarize` and `translate` both depend on `fetchDraft`, and `publish` waits for both — so the
engine runs the middle pair **concurrently** and fans back in, without you scheduling anything:

```
              ┌── summarize ──┐
 fetchDraft ──┤               ├── publish
              └── translate ──┘
```

That shape isn't inferred from a trace — it *is* the workflow. The DAG is a plain value you can
walk before anything runs:

```ts
for (const s of wf.definition.steps) {
  console.log(`${s.key.padEnd(12)} type=${s.type.padEnd(10)} deps: ${s.dependencies?.join(', ') || '—'}`);
}
```

```text
fetchDraft   type=fetch      deps: —
summarize    type=summarize  deps: fetchDraft
translate    type=translate  deps: fetchDraft
publish      type=publish    deps: summarize, translate
```

This is the core design choice. Flow is **declarative**, where Temporal, Inngest, Trigger.dev
and DBOS are **imperative**: there you write a function, and the graph exists only as the trace
of what it did. Both models are durable — they trade off differently, and
[the tradeoff is spelled out below](/start/how-it-compares/).

## Features

| | Capability |
|---|---|
| 🧩 | **Typed DAG** — Zod-validated input/output per step; dependency outputs are typed |
| ⚡ | **Auto-parallelism** — dependency-free steps run concurrently; a step starts when all its deps complete |
| 🔁 | **Retry & timeout** — per-step `maxAttempts`, fixed/exponential backoff, wall-clock timeout |
| 💤 | **Durable sleep** — hold a step in the queue for N ms (survives restarts) |
| 🚦 | **Concurrency & rate limiting** — per-step-type caps and token buckets via a pluggable gate |
| ⏰ | **Cron / scheduled starts** — fire workflows on a schedule (pg-boss) |
| 🔑 | **Start idempotency** — a dedup key collapses double-clicks / overlapping ticks |
| 🗺️ | **Dynamic fan-out / map** — spawn one child step per item of a runtime-sized list |
| ⏸️ | **Signals / waitForEvent** — suspend a step until an external event (`resumeStep`) |
| 🪆 | **Sub-workflows** — a step starts a child workflow and awaits its result |
| ↩️ | **Saga compensation** — run rollback handlers in reverse order on failure |
| 🔭 | **Observability** — lifecycle events (run history) + per-step spans, both pluggable |
| 🤖 | **AI add-on** — instrumented models, token/cost capture, quota, daily rollups |
| 🧱 | **Pluggable everything** — `WorkflowStore`, `Dispatcher`, `StepGate`, `FlowObserver`, hooks |
