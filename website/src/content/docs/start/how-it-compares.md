---
title: How it compares
description: Flow against Temporal, Inngest, Trigger.dev and BullMQ — and when to pick something else.
---

All of these give you durable execution. They differ in **what you operate** and **how the
workflow is expressed**.

| | **Flow** | **Temporal** | **Inngest** | **Trigger.dev** | **BullMQ** |
|---|---|---|---|---|---|
| **Infra you run** | Postgres | a cluster (frontend, history, matching, worker) + a DB | none — engine is hosted | Postgres + Redis (+ ClickHouse recommended at volume) | Redis |
| **Self-host** | it's a library | yes, MIT | no — engine and dashboard are Inngest-hosted | yes, Apache-2.0 | it's a library |
| **Model** | declarative — a static DAG value | imperative workflow code | imperative step functions | imperative tasks | job queue; flows are **trees** |
| **Fan-in / diamond deps** | yes | yes | yes | yes | no — a job can't be shared by two branches |
| **Inspect before running** | yes — the DAG is a value | no — the graph is the execution trace | no | no | yes — the flow tree is data |
| **Web dashboard** | **none** — [build one](/running/live-progress/) | yes | yes | yes | via third-party UIs |
| **Languages** | TypeScript | polyglot SDKs | TS, Python, Go, Kotlin | TS | Node (+ ports) |
| **Maturity** | **pre-1.0** | mature | mature | mature | mature, widely deployed |

The honest summary: **Flow is the smallest thing that is still a real workflow engine.** If
you already run Postgres, it adds no infrastructure — the queue ([pg-boss](https://github.com/timgit/pg-boss))
is Postgres too. You give up the dashboards, the polyglot SDKs, and the operational maturity
that the others have earned.

Reach for something else if:

- **Your control flow is genuinely dynamic.** A declarative DAG is fixed at definition time.
  Flow softens this with [`defineMapStep`](/core/fan-out-and-map/) (runtime-sized fan-out),
  [sub-workflows](/core/sub-workflows/), and [`waitForEvent`](/core/signals/) — but if your
  process is "loop until a human approves, branching on whatever they typed," an imperative
  durable function will express it more naturally.
- **You want a UI out of the box.** Flow ships a wire-safe projection
  ([`toPublicWorkflow`](/extending/http/)) and lifecycle events, not a
  dashboard — see [Live progress](/running/live-progress/) for the recipe.
- **You need non-TypeScript workers.** The DAG and its schemas are TypeScript values.
- **You can't run Postgres**, or you need throughput past what a Postgres-backed queue gives you.
- **You need a support contract**, or an API frozen by a 1.0 promise. This is pre-1.0 and 0.x
  minors can break.

Flow fits best when the work is a **known pipeline** — ingest → enrich → summarize → publish —
that must survive crashes, retry sanely, and stay legible to the next person who reads it.
