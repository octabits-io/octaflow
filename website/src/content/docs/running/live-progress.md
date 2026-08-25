---
title: Live progress
description: Stream workflow transitions to a browser over SSE by composing the FlowObserver seam — the live half of building your own dashboard.
---

Octaflow ships no dashboard. It ships the two seams you need to build one:

- **[`toPublicWorkflow()`](/extending/http/)** — the read side. Fetch the current
  state of a workflow, wire-safe.
- **`FlowObserver`** — the live side. Every transition, as it happens.

This page wires the live side end to end. The whole hub is about twenty lines; the
interesting part is the deployment constraint at the bottom.

→ Runnable version: [`examples/14-live-progress.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/14-live-progress.ts)

## The shape

```
engine ──record()──▶ hub ──filter by partitionKey──▶ subscribers ──SSE──▶ browser
```

The engine emits a `FlowEvent` on every transition and **never awaits the observer**, so
a slow or throwing subscriber can't stall a step. That also means delivery is
best-effort — see [durability](#durability-catching-up-after-a-disconnect).

## A fan-out hub

```ts
import type { FlowEvent, FlowObserver } from 'octaflow';

interface Subscriber {
  partitionKey: string;   // only this partition's events reach this subscriber
  send(frame: string): void;
}

function createEventHub() {
  const subscribers = new Set<Subscriber>();

  function toSseFrame(event: FlowEvent): string {
    const { type, workflowId, stepKey, at, durationMs, error } = event;
    // partitionKey is a routing concern, not the client's business.
    const data = { workflowId, stepKey, at, durationMs, error };
    return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  }

  return {
    subscribe(sub: Subscriber): () => void {
      subscribers.add(sub);
      return () => subscribers.delete(sub);
    },
    observer: {
      record(event) {
        const frame = toSseFrame(event);
        for (const sub of subscribers) {
          if (sub.partitionKey !== event.partitionKey) continue;
          try {
            sub.send(frame);
          } catch {
            // One broken subscriber must not affect the others.
          }
        }
      },
    } satisfies FlowObserver,
  };
}
```

Hand the observer to the engine:

```ts
const hub = createEventHub();
const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey, observer: hub.observer });
```

## Serving it

A plain fetch handler — no framework needed, works on any web-standard runtime:

```ts
function streamProgress(partitionKey: string): Response {
  const encoder = new TextEncoder();
  let unsubscribe = () => {};

  const body = new ReadableStream({
    start(controller) {
      unsubscribe = hub.subscribe({
        partitionKey,
        send: (frame) => controller.enqueue(encoder.encode(frame)),
      });
    },
    cancel() {
      unsubscribe();   // the client went away — stop fanning out to it
    },
  });

  return new Response(body, {
    headers: {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    },
  });
}
```

**Resolve `partitionKey` from the session, never from the query string** — it is the
only thing separating one tenant's stream from another's.

What the browser reads:

```text
event: workflow.started
data: {"workflowId":1,"at":"2026-08-13T13:43:49.138Z"}

event: step.started
data: {"workflowId":1,"stepKey":"fetchDraft","at":"2026-08-13T13:43:49.139Z"}

event: step.completed
data: {"workflowId":1,"stepKey":"fetchDraft","at":"2026-08-13T13:43:49.139Z","durationMs":0}
```

## The catch: this hub is per-process

Subscribers only receive events emitted by an engine in the **same process**. A real
deployment runs the HTTP server and the step workers separately, so a transition on a
worker never reaches a browser attached to the server.

Bridging that needs a shared channel, and Postgres LISTEN/NOTIFY is the obvious one
since the database is already there. Two things bite:

- **Use a dedicated connection, never a pooled one.** Pools reset connections between
  checkouts and silently drop the `LISTEN` registration. The channel then looks healthy
  and delivers nothing.
- **Bypass transaction-mode poolers (PgBouncer).** `LISTEN` does not survive transaction
  pooling — connect directly, exactly as pg-boss already requires.

## Durability: catching up after a disconnect

`NOTIFY` is not durable, and neither is this hub: a client that was disconnected missed
those events for good. If catching up matters, pair the live stream with the persisted
history from [`octaflow/store-pg`](/running/postgres-and-pg-boss/):

```ts
import { createPgEventSink, readFlowEvents } from 'octaflow/store-pg';

const sink = createPgEventSink({ pool, partitionKey });
const engine = createWorkflowEngine({
  // …
  observer: { record: (e) => { sink.record(e); hub.observer.record(e); } },
});
```

On reconnect the client asks for the workflow's history, then switches to the stream.
Same `FlowEvent` envelope on both paths, so the client needs one renderer rather than two.

Note the shape of the read side: `readFlowEvents(pool, { workflowId, partitionKey })`
returns **one workflow's complete timeline**, in insertion order. There is no `since`
filter and no way to ask for "everything in the partition" — so the catch-up is
per-workflow, and dropping events the client already saw is the client's job (compare
against its last-seen `at`, or just re-render idempotently from the full history).
