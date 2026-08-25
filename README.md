# octaflow

[![CI](https://github.com/octabits-io/octaflow/actions/workflows/ci.yml/badge.svg)](https://github.com/octabits-io/octaflow/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/octaflow.svg)](https://www.npmjs.com/package/octaflow)
[![docs](https://img.shields.io/badge/docs-octaflow.octabits.io-16794a.svg)](https://octaflow.octabits.io/)
[![license](https://img.shields.io/npm/l/octaflow.svg)](./LICENSE)
[![types](https://img.shields.io/badge/types-included-blue.svg)](https://www.npmjs.com/package/octaflow)

**Durable workflows for TypeScript that run on the Postgres you already have.**

Declare a DAG of Zod-typed steps. The engine runs each step as soon as its dependencies
complete, persists every transition, retries failures, and picks up where it left off after a
restart. There is
no workflow server to operate, no control plane, and no vendor — it's a library you import,
not a platform you adopt.

### 📖 [Read the docs →](https://octaflow.octabits.io/)

[Quick start](https://octaflow.octabits.io/start/quick-start/) ·
[Concepts](https://octaflow.octabits.io/core/concepts/) ·
[Postgres & pg-boss](https://octaflow.octabits.io/running/postgres-and-pg-boss/) ·
[API reference](https://octaflow.octabits.io/reference/api/)

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
[the tradeoff is spelled out below](#how-it-compares).

---

## Watch it run

A fuller pipeline than the snippet above — dynamic fan-out over two `map` steps at once, a
concurrency cap so only 2 of 6 images encode at a time, a flaky step retried, a sub-workflow,
a step suspended on an external event under a deadline, a durable sleep, and a branch where
the arm the review didn't choose is skipped but the join still fires. Then a second workflow
fails and its completed steps roll back in reverse.

<p align="center">
  <img src="./docs/demo.svg" alt="Terminal trace of a publishing pipeline: parallel fan-out over locales and images with a concurrency cap, a retried step, a sub-workflow, a suspend-and-resume on an external event with a deadline armed, a durable sleep, a skipped branch converging on a join, then a saga rollback in reverse order" width="760">
</p>

Every line is an engine transition emitted through the [`FlowObserver`](https://octaflow.octabits.io/running/observability/) seam —
the same one you'd point at OpenTelemetry or an events table — not a `console.log` in a handler.

```bash
npx tsx scripts/demo.ts    # reproduce it
```

---

## Contents

- [How it compares](#how-it-compares)
- [When not to use Flow](#when-not-to-use-flow)
- [Performance](#performance)
- [Features](#features)
- [Installation](#installation)
- [Documentation](#documentation)
- [Examples](#examples)

---

## How it compares

All of these give you durable execution. They differ in **what you operate** and **how the
workflow is expressed**.

| | **Flow** | **Temporal** | **Inngest** | **Trigger.dev** | **BullMQ** |
|---|---|---|---|---|---|
| **Infra you run** | Postgres | a cluster (frontend, history, matching, worker) + a DB | none — engine is hosted | Postgres + Redis (+ ClickHouse recommended at volume) | Redis |
| **Self-host** | it's a library | yes, MIT | no — engine and dashboard are Inngest-hosted | yes, Apache-2.0 | it's a library |
| **Model** | declarative — a static DAG value | imperative workflow code | imperative step functions | imperative tasks | job queue; flows are **trees** |
| **Fan-in / diamond deps** | yes | yes | yes | yes | no — a job can't be shared by two branches |
| **Inspect before running** | yes — the DAG is a value | no — the graph is the execution trace | no | no | yes — the flow tree is data |
| **Web dashboard** | **none** — [build one](https://octaflow.octabits.io/running/live-progress/) | yes | yes | yes | via third-party UIs |
| **Languages** | TypeScript | polyglot SDKs | TS, Python, Go, Kotlin | TS | Node (+ ports) |
| **Maturity** | **pre-1.0** | mature | mature | mature | mature, widely deployed |

The honest summary: **Flow is the smallest thing that is still a real workflow engine.** If
you already run Postgres, it adds no infrastructure — the queue ([pg-boss](https://github.com/timgit/pg-boss))
is Postgres too. You give up the dashboards, the polyglot SDKs, and the operational maturity
that the others have earned.

## When not to use Flow

Reach for something else if:

- **Your control flow is genuinely dynamic.** A declarative DAG is fixed at definition time.
  Flow softens this a lot — [`when` guards and joins](https://octaflow.octabits.io/core/branching/) (if/else over a static graph),
  [`defineMapStep`](https://octaflow.octabits.io/core/fan-out-and-map/) (runtime-sized fan-out),
  [sub-workflows](https://octaflow.octabits.io/core/sub-workflows/), and [`waitForEvent`](https://octaflow.octabits.io/core/signals/) — but the
  set of steps is still fixed up front. If your process is "**loop** until a human approves,
  branching on whatever they typed," an imperative durable function will express it more
  naturally: Flow can pick a branch, not invent a step.
- **You want a UI out of the box.** Flow ships a wire-safe projection
  ([`toPublicWorkflow`](https://octaflow.octabits.io/extending/http/)) and lifecycle events, not a
  dashboard. You build it.
- **You need non-TypeScript workers.** The DAG and its schemas are TypeScript values.
- **You can't run Postgres**, or you need throughput past what a Postgres-backed queue gives you.
- **You need a support contract**, or an API frozen by a 1.0 promise. This is pre-1.0 and 0.x
  minors can break.

Flow fits best when the work is a **known pipeline** — ingest → enrich → summarize → publish —
that must survive crashes, retry sanely, and stay legible to the next person who reads it.

---

## Performance

Reproduce with `npx tsx scripts/bench.ts` (Docker required — it starts Postgres 17 via
Testcontainers). Workload: 200 workflows × 6 steps in a `root → 4 parallel → join` diamond.
**Handlers are no-ops**, so this measures what the *engine* costs per step — claiming it,
reading dependency outputs, persisting the transition, recomputing readiness — not your work.

**Engine + Postgres store** (in-process dispatcher), per-step latency:

| concurrency | steps/sec | p50 | p95 | p99 |
|---|---|---|---|---|
| 1 | 1,031 | 1.0 ms | 2.1 ms | 2.9 ms |
| 4 | 1,932 | 2.1 ms | 3.9 ms | 4.8 ms |
| 16 | 2,108 | 7.1 ms | 12.7 ms | 15.9 ms |
| 64 | 2,270 | 26.8 ms | 46.8 ms | 64.0 ms |

**End-to-end through pg-boss workers** — the full production path, batch 25:

| workers | `burstWhenBatchFull` | `concurrency` | steps/sec |
|---|---|---|---|
| 1 | off | 1 | 50 |
| 1 | **on** | 1 | 274 |
| 1 | **on** | 8 | 646 |
| 4 | **on** | 8 | 902 |

That first row is not a ceiling, it's a *polling artifact* — and the fix is configuration, not
architecture. A worker drains a batch in milliseconds, then waits out the 0.5 s interval, so
**`burstWhenBatchFull` is the setting that matters**: it keeps fetching while batches come back
full. `concurrency` (steps run at once from one batch) then compounds on top — but on its own,
without burst, it changes nothing at all, because the wait, not the work, is the bottleneck.

Budget connections before raising `concurrency`: each in-flight step holds one, so
`workers × concurrency` must fit your pool and Postgres `max_connections`.

**How to read this.** Measured on an M-series Mac with Postgres in Docker, which has markedly
slower disk I/O than a Linux host — expect better on a real server. These are an order of
magnitude and a scaling shape, not a score. A Redis-backed job queue will beat these numbers
outright, because it isn't writing a durable transition per step to a relational database;
that write is the feature. And in any real workflow, handler time dwarfs the 1–3 ms of engine
overhead, so the practical question is usually whether ~1 ms per transition is acceptable
next to what your steps actually do.

---

## Features

| | Capability |
|---|---|
| 🧩 | **Typed DAG** — Zod-validated input/output per step; dependency outputs are typed |
| ⚡ | **Auto-parallelism** — dependency-free steps run concurrently; a step starts when all its deps complete |
| 🔁 | **Retry & timeout** — per-step `maxAttempts`, fixed/exponential backoff, wall-clock timeout |
| 💤 | **Durable sleep** — hold a step in the queue for N ms (survives restarts) |
| 🔀 | **Conditional branching** — `when` guards skip a step and its branch; `join: 'any'` converges |
| ⏳ | **Deadlines** — a budget on a suspended step (fail, or continue with a stand-in answer) and on a whole run |
| ♻️ | **Retry a failed run** — `retryWorkflow` resumes from the failure point; completed steps keep their output |
| 🚦 | **Concurrency & rate limiting** — per-step-type caps and token buckets via a pluggable gate |
| ⏰ | **Cron / scheduled starts** — fire workflows on a schedule (pg-boss) |
| 🔑 | **Start idempotency** — a dedup key collapses double-clicks / overlapping ticks |
| 🗺️ | **Dynamic fan-out / map** — spawn one child step per item of a runtime-sized list |
| ⏸️ | **Signals / waitForEvent** — suspend a step until an external event (`resumeStep`), with an optional deadline |
| 🪆 | **Sub-workflows** — a step starts a child workflow and awaits its result |
| ↩️ | **Saga compensation** — run rollback handlers in reverse order on failure |
| 🚑 | **Crash recovery** — a step whose worker died is re-queued while its attempt budget lasts |
| 💓 | **Heartbeats** — a long step proves it is alive, so a dead one is caught in seconds, not minutes |
| 🔭 | **Observability** — lifecycle events (run history) + per-step spans, both pluggable |
| 🤖 | **AI add-on** — instrumented models, token/cost capture, quota, daily rollups |
| 🧱 | **Pluggable everything** — `WorkflowStore`, `Dispatcher`, `StepGate`, `FlowObserver`, hooks |

---

## Installation

```bash
pnpm add octaflow zod
```

`zod` is a **required** peer. The heavy dependencies are **optional peers** — install only
what the layers you import need:

```bash
# Postgres store / gate / event sink
pnpm add pg

# pg-boss dispatcher, workers, cron scheduler
pnpm add pg-boss

# the AI add-on
pnpm add ai @ai-sdk/provider
```

> Pure in-memory usage (great for tests and single-process apps) needs **nothing** beyond
> `zod` — the engine, `defineStep`/`buildWorkflow`, and the in-memory store are all in the core.

---

---

## Documentation

Full docs — concepts, every feature, production wiring and the API reference — live at
**[octaflow.octabits.io](https://octaflow.octabits.io/)**.

| | |
|---|---|
| [Quick start](https://octaflow.octabits.io/start/quick-start/) | a runnable workflow in one file, no database |
| [Concepts](https://octaflow.octabits.io/core/concepts/) | step, workflow, registry, store, dispatcher, engine, partition |
| [Defining steps](https://octaflow.octabits.io/core/defining-steps/) | `defineStep` and its variants |
| [Retry & timeout](https://octaflow.octabits.io/core/retry-and-timeout/) | attempt budgets, backoff, and how a failure is classified |
| [Fan-out & map](https://octaflow.octabits.io/core/fan-out-and-map/) | one child step per item of a runtime list |
| [Branching](https://octaflow.octabits.io/core/branching/) | `when` guards and join rules — if/else over a static DAG |
| [Signals](https://octaflow.octabits.io/core/signals/) · [Sub-workflows](https://octaflow.octabits.io/core/sub-workflows/) · [Saga](https://octaflow.octabits.io/core/saga-compensation/) | suspend, nest, and roll back |
| [Deadlines](https://octaflow.octabits.io/core/deadlines/) | budgets for a suspended step and for a whole run |
| [Heartbeats](https://octaflow.octabits.io/core/heartbeats/) | liveness for long steps, and interrupting one that was cancelled |
| [Postgres & pg-boss](https://octaflow.octabits.io/running/postgres-and-pg-boss/) | production wiring, workers, DLQ, cron |
| [Cancellation & recovery](https://octaflow.octabits.io/running/cancellation-and-recovery/) | cancelling a run, sweeping steps a crash left behind, and retrying a failed run |
| [Observability](https://octaflow.octabits.io/running/observability/) · [Live progress](https://octaflow.octabits.io/running/live-progress/) | lifecycle events, spans, and streaming them to a browser |
| [The AI add-on](https://octaflow.octabits.io/extending/ai/) | token/cost capture, quota, usage rollups |
| [Extending](https://octaflow.octabits.io/extending/interfaces/) | custom stores, dispatchers and gates |
| [API reference](https://octaflow.octabits.io/reference/api/) | every export, by entry point |

---

## Examples

Runnable, focused examples live in [`examples/`](./examples) — see [`examples/README.md`](./examples/README.md).

| # | File | Shows |
|---|---|---|
| 01 | `01-in-memory-quickstart.ts` | minimal setup + run loop |
| 02 | `02-dag-parallel-fan-in.ts` | parallel branches + fan-in (diamond DAG) |
| 03 | `03-retry-timeout.ts` | per-step retry + timeout |
| 04 | `04-durable-sleep.ts` | durable delay between steps |
| 05 | `05-concurrency-rate-limit.ts` | in-memory `StepGate` |
| 06 | `06-start-idempotency.ts` | dedup key collapses duplicate starts |
| 07 | `07-dynamic-map.ts` | runtime fan-out / map |
| 08 | `08-wait-for-event.ts` | suspend + `resumeStep` |
| 09 | `09-sub-workflows.ts` | child workflow compose + await |
| 10 | `10-saga-compensation.ts` | reverse-order rollback on failure |
| 11 | `11-observability.ts` | observer events + tracer spans |
| 12 | `12-postgres-pgboss-production.ts` | full pg store + gate + event sink + pg-boss + cron |
| 13 | `13-ai-workflow.ts` | AI add-on (instrumented model + cost) |
| 14 | `14-live-progress.ts` | `FlowObserver` → SSE fan-out (build your own dashboard) |
| 15 | `15-conditional-branching.ts` | `when` guards + a `join: 'any'` convergence |
| 16 | `16-deadlines-and-retry.ts` | wait deadlines, run deadlines, `retryWorkflow`, heartbeats |

The in-memory examples (01–11, 14–16) share a small driver, [`examples/runtime.ts`](./examples/runtime.ts),
that builds an engine over the in-memory store and an in-process queue you drain.

---

## Contributing

Bug reports with a runnable reproduction are the most useful thing you can send; PRs are
welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) for the setup, the layer rules the lint
enforces, and the correctness requirements a custom `WorkflowStore` has to meet.

---

## Status

Pre-1.0 — developed in [octabits-io/octaflow](https://github.com/octabits-io/octaflow) (extracted from the
[octabits platform monorepo](https://github.com/octabits-io/platform), where its earlier history lives).
The API is stable but may still see breaking changes in 0.x minors.
