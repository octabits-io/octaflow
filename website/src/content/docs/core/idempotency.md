---
title: Start idempotency
description: Collapse double-clicks and overlapping ticks with a dedup key.
---

```ts
await wf.start(engine, input, { idempotencyKey: `import:${fileId}` });
// a second start with the same key returns the existing workflow instead of duplicating it
```
→ [`examples/06-start-idempotency.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/06-start-idempotency.ts)

## What a hit looks like

`start` returns the same `Result` shape either way — a hit is not an error:

```ts
const started = await wf.start(engine, input, { idempotencyKey: 'import:42' });
// { ok: true, value: { workflowId, totalSteps, enqueuedSteps } }
```

On a hit you get the **existing** `workflowId`, and `enqueuedSteps` is **empty** — nothing
was re-dispatched, because the original start already enqueued the roots. That empty array
is the only way to tell a hit from a miss, so branch on it if you care:

```ts
if (started.ok && started.value.enqueuedSteps.length === 0) {
  // already running (or already finished) — don't tell the user "started"
}
```

## Scope and lifetime

- **Per partition.** The key is unique within a `partitionKey`, not globally, so two tenants
  can both use `import:42` without colliding.
- **Forever.** There is no TTL. A key is consumed for the lifetime of the workflow row, and
  a completed or failed workflow still holds it. Reusing a key later returns the old
  terminal workflow rather than starting fresh — so build keys from something that shouldn't
  recur: `import:${fileId}:${uploadedAt}`, not `import:daily`.
- **Also honoured for sub-workflows and cron starts**, since both go through `startWorkflow`.

## Store support

Idempotency lives in the store, not the engine, so a custom `WorkflowStore` has to implement
it in `createWorkflow`: when `idempotencyKey` is set and already present, return the existing
workflow with `alreadyExisted: true` instead of inserting.

Both bundled stores do. `store-pg` relies on a partial unique index that
[`flowStoreDdl`](/running/postgres-and-pg-boss/) creates for you:

```sql
CREATE UNIQUE INDEX flow_workflow_idempotency_idx
  ON flow_workflow (partition_key, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
```

The insert is `ON CONFLICT … DO NOTHING` followed by a lookup, so two concurrent starts with
the same key resolve to one workflow at the database level — not merely "usually".

:::note[On a cron schedule, use a key *prefix*]
A schedule stores its payload once, so a key fixed at schedule time would ride every tick and —
since keys never expire — collapse them all into the first workflow. Pass
`idempotencyKeyPrefix` instead and the start worker resolves it per delivery. See
[cron idempotency](/running/postgres-and-pg-boss/#cron-idempotency).
:::
