---
title: Defining steps
description: defineStep and its variants for waits, maps, sub-workflows and durable sleep.
---

| Helper | Use it for |
|---|---|
| `defineStep({ type, workflowInputSchema, outputSchema, dependencies?, handler, retry?, isRetryable?, timeoutMs?, delayMs?, waitForEvent?, compensate? })` | a normal step |
| `defineSleepStep({ type, sleepMs, dependencies? })` | a [durable no-op delay](/core/durable-sleep/) |
| `defineWaitStep({ type, outputSchema, dependencies? })` | [suspend until `engine.resumeStep`](/core/signals/) |
| `defineMapStep({ type, workflowInputSchema, itemOutputSchema, items, each, dependencies?, itemRetry?, itemIsRetryable?, itemTimeoutMs? })` | [runtime-sized fan-out](/core/fan-out-and-map/) |
| `defineSubWorkflowStep({ type, workflowInputSchema, childWorkflow, input, outputSchema?, dependencies? })` | [start + await a child workflow](/core/sub-workflows/) |
| `defineAiStep({ ... })` | a step whose `ctx.context` is an instrumented `AiContext` ([AI add-on](/extending/ai/)) |

## Keys and types are different things

```ts
const wf = buildWorkflow({ type: 'publish', inputSchema, steps: { draft, approval } });
//                                                                ^^^^^  ^^^^^^^^
//                                                                these are the step KEYS
```

- **`type`** (on the step) names the *handler*. It is what the registry looks up, what gate
  rules and metrics are keyed on, and it may repeat across workflows.
- **The key** is the property name under `steps:` in `buildWorkflow`. It identifies the step
  *within this workflow*: it's what `ctx.deps` is keyed by, what `engine.resumeStep` takes, and
  what the workflow's output object is keyed by.

The same rule applies to `dependencies`. The property name — not the variable — is the
dependency key, and it must match the step's key in `buildWorkflow`:

```ts
const shout = defineStep({ …, dependencies: { greet } });     // key 'greet'
buildWorkflow({ …, steps: { greet, shout } });                // ✅ matches

buildWorkflow({ …, steps: { greeting: greet, shout } });      // ❌ throws:
// [buildWorkflow] Step 'shout' depends on 'greet', which is not a valid step key.
```

Note that `buildWorkflow` **throws** here rather than returning a `Result` — a malformed DAG is
a programming error, caught at module load. The deeper checks (cycles, duplicate keys, missing
handlers) run at start and *are* returned as a `Result`; see
[`validateDefinition`](/extending/interfaces/#validating-a-definition).

## The handler context

A handler receives a **typed context**:

```ts
handler: async (ctx) => {
  ctx.workflowInput;   // validated against workflowInputSchema
  ctx.deps.greet;      // each dependency's output, validated against ITS outputSchema
  ctx.stepInput;       // static per-step input from the definition
  ctx.context;         // host value from the `buildStepContext` hook
  ctx.signal;          // AbortSignal — fires on timeout
  ctx.workflowId; ctx.stepId; ctx.stepKey; ctx.partitionKey;
  return { … };        // validated against outputSchema
}
```

**To fail a step, throw.** The typed handler returns the step's output; there is no
error-return form (the `Result`-returning `StepHandler` is the untyped internal shape that
`defineStep` wraps for you). Whether a throw is retried is decided by
[retryability](/core/retry-and-timeout/) — throw `retryableError(…)` or
`nonRetryableError(…)` to say so outright.

## Validation is a permanent failure

`defineStep` runs a five-phase pipeline: parse `workflowInput` → parse each dependency output →
build the typed context → call the handler → parse the output. A mismatch at any of those
phases fails the step with `retryable: false` — **it is never retried, whatever the step's
`retry` policy says**, because a schema mismatch is a bug rather than a blip.

The error message names the phase, which is usually enough to locate it:

```
[shout] Invalid dependency output 'greet': …
[shout] Invalid step output: …
```

`ctx.signal` is worth wiring through to anything that accepts one (`fetch`, database drivers) —
`timeoutMs` aborts it on expiry, but only a handler that observes it actually stops early.
