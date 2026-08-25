---
title: Examples
description: Runnable examples, 01–16.
---

Runnable, focused examples live in [`examples/`](https://github.com/octabits-io/octaflow/blob/main/examples) — see [`examples/README.md`](https://github.com/octabits-io/octaflow/blob/main/examples/README.md).

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
| 14 | `14-live-progress.ts` | `FlowObserver` → SSE fan-out ([build your own dashboard](/running/live-progress/)) |
| 15 | `15-conditional-branching.ts` | [`when` guards + a `join: 'any'` convergence](/core/branching/) |
| 16 | `16-deadlines-and-retry.ts` | [wait deadlines, run deadlines](/core/deadlines/), [`retryWorkflow`](/running/cancellation-and-recovery/#retrying-a-failed-run), and [heartbeats](/core/heartbeats/) |

The in-memory examples (01–11, 14–16) share a small driver, [`examples/runtime.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/runtime.ts),
that builds an engine over the in-memory store and an in-process queue you drain. Its queue
runs on a virtual clock, so delays — retry backoff, durable sleep, wait deadlines — behave as
they would on a real queue without costing you the wall-clock wait.
