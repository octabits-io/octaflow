import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { createWorkflowEngine } from './engine';
import { createInMemoryWorkflowStore } from './in-memory-store';
import { createStepHandlerRegistry } from './registry';
import { defineStep, buildWorkflow, defineSleepStep, defineWaitStep, defineMapStep, defineSubWorkflowStep } from './defineStep';
import { retryableError, nonRetryableError } from './retry';
import type { Dispatcher, DispatchStepPayload } from './dispatcher';
import type { StepGate } from './gate';
import type { WorkflowHooks } from './hooks';
import type { FlowObserver, FlowTracer } from './observability';
import { createRecordingObserver, createRecordingTracer } from './observability';
import type { TypedStep } from './defineStep';
import type { WorkflowDefinition } from './types';

// ---------------------------------------------------------------------------
// Test harness: an in-memory store + a drainable dispatcher that mimics a
// durable queue (enqueue schedules; drain() processes FIFO, including the
// re-enqueues the engine performs as steps complete).
// ---------------------------------------------------------------------------

type Ctx = { tag: string } | undefined;

function harness(opts?: { hooks?: WorkflowHooks<Ctx>; now?: () => Date; observer?: FlowObserver; tracer?: FlowTracer }) {
  const store = createInMemoryWorkflowStore();
  const registry = createStepHandlerRegistry<Ctx>();
  const queue: DispatchStepPayload[] = [];
  const delays: Array<number | undefined> = [];
  const dispatcher: Dispatcher = {
    async enqueueStep(p, options) {
      queue.push(p);
      delays.push(options?.startAfterSeconds);
      return { ok: true, value: undefined };
    },
  };
  const engine = createWorkflowEngine<Ctx>({
    store,
    registry,
    dispatcher,
    partitionKey: 'test',
    hooks: opts?.hooks,
    observer: opts?.observer,
    tracer: opts?.tracer,
    now: opts?.now,
  });

  async function drain() {
    let guard = 0;
    while (queue.length > 0) {
      if (++guard > 1000) throw new Error('drain runaway');
      const p = queue.shift()!;
      // A real dispatcher retries on throw; the engine has already marked the
      // step failed + cascaded before re-throwing, so swallowing is faithful.
      try {
        await engine.handleStepJob(p);
      } catch {
        /* simulated DLQ */
      }
    }
  }

  return { store, registry, engine, queue, delays, drain };
}

// Build a simple A → B → C linear workflow with recorded call order.
function linearWorkflow(order: string[]) {
  const input = z.object({ seed: z.string() });
  const a = defineStep<{ seed: string }, { a: string }, Ctx>({
    type: 't:a',
    workflowInputSchema: input,
    outputSchema: z.object({ a: z.string() }),
    handler: async (ctx) => {
      order.push(`a:${ctx.context?.tag ?? 'none'}`);
      return { a: ctx.workflowInput.seed + '-a' };
    },
  });
  const b = defineStep<{ seed: string }, { b: string }, Ctx, { a: typeof a }>({
    type: 't:b',
    workflowInputSchema: input,
    outputSchema: z.object({ b: z.string() }),
    dependencies: { a },
    handler: async (ctx) => {
      order.push('b');
      return { b: ctx.deps.a.a + '-b' };
    },
  });
  const c = defineStep<{ seed: string }, { c: string }, Ctx, { b: typeof b }>({
    type: 't:c',
    workflowInputSchema: input,
    outputSchema: z.object({ c: z.string() }),
    dependencies: { b },
    handler: async (ctx) => {
      order.push('c');
      return { c: ctx.deps.b.b + '-c' };
    },
  });
  return buildWorkflow<{ seed: string }, Ctx>({ type: 'linear', inputSchema: input, steps: { a, b, c } });
}

describe('createWorkflowEngine', () => {
  it('runs a linear DAG in dependency order and aggregates output', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);

    const started = await wf.start(h.engine, { seed: 'x' });
    expect(started.ok).toBe(true);
    if (!started.ok) return;
    // only the dependency-free root is enqueued initially
    expect(h.queue.length).toBe(1);

    await h.drain();
    expect(order).toEqual(['a:none', 'b', 'c']);

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    expect(status.ok).toBe(true);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.completedSteps).toBe(3);
    expect(status.value.output).toEqual({
      a: { a: 'x-a' },
      b: { b: 'x-a-b' },
      c: { c: 'x-a-b-c' },
    });
  });

  it('parallelizes dependency-free roots and waits at a fan-in join', async () => {
    const input = z.object({});
    const a = defineStep<{}, { v: number }, Ctx>({
      type: 'fan:a',
      workflowInputSchema: input,
      outputSchema: z.object({ v: z.number() }),
      handler: async () => ({ v: 1 }),
    });
    const b = defineStep<{}, { v: number }, Ctx>({
      type: 'fan:b',
      workflowInputSchema: input,
      outputSchema: z.object({ v: z.number() }),
      handler: async () => ({ v: 2 }),
    });
    const join = defineStep<{}, { sum: number }, Ctx, { a: typeof a; b: typeof b }>({
      type: 'fan:join',
      workflowInputSchema: input,
      outputSchema: z.object({ sum: z.number() }),
      dependencies: { a, b },
      handler: async (ctx) => ({ sum: ctx.deps.a.v + ctx.deps.b.v }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'fan', inputSchema: input, steps: { a, b, join } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    expect(started.ok).toBe(true);
    if (!started.ok) return;

    // both roots enqueued immediately; join is not
    expect(h.queue.length).toBe(2);
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.output).toMatchObject({ join: { sum: 3 } });
  });

  it('rejects a workflow definition with a cycle', async () => {
    const h = harness();
    h.registry.register('c:a', async () => ({ ok: true, value: {} }));
    h.registry.register('c:b', async () => ({ ok: true, value: {} }));
    const cyclic: WorkflowDefinition = {
      type: 'cyclic',
      steps: [
        { key: 'a', type: 'c:a', dependencies: ['b'] },
        { key: 'b', type: 'c:b', dependencies: ['a'] },
      ],
    };
    const res = await h.engine.startWorkflow(cyclic, {});
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.key).toBe('invalid_workflow_definition');
    expect(res.error.message).toMatch(/circular/i);
  });

  it('cascades a failed step: dependents are skipped and the workflow fails', async () => {
    const input = z.object({});
    const a = defineStep<{}, { ok: boolean }, Ctx>({
      type: 'f:a',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      handler: async () => {
        return Promise.reject(new Error('boom in a'));
      },
    });
    const b = defineStep<{}, { ok: boolean }, Ctx, { a: typeof a }>({
      type: 'f:b',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      dependencies: { a },
      handler: async () => ({ ok: true }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'failcascade', inputSchema: input, steps: { a, b } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    const stepA = status.value.steps.find((s) => s.key === 'a')!;
    const stepB = status.value.steps.find((s) => s.key === 'b')!;
    expect(stepA.status).toBe('failed');
    expect(stepA.error).toMatch(/boom in a/);
    expect(stepB.status).toBe('skipped');
  });

  it('finalizes as failed when a parallel branch completes after another branch already failed', async () => {
    // Regression: fail root `a` while independent root `img` is still queued.
    // The failure path can't finalize (img not terminal); when img later
    // completes, the completion path must route through the failure check —
    // previously it treated the failed step as non-terminal and the workflow
    // was stranded in `running` forever.
    const input = z.object({});
    const a = defineStep<{}, { ok: boolean }, Ctx>({
      type: 'pf:a',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      handler: async () => Promise.reject(new Error('boom in a')),
    });
    const img = defineStep<{}, { ok: boolean }, Ctx>({
      type: 'pf:img',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      handler: async () => ({ ok: true }),
    });
    const b = defineStep<{}, { ok: boolean }, Ctx, { a: typeof a }>({
      type: 'pf:b',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      dependencies: { a },
      handler: async () => ({ ok: true }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'parallelfail', inputSchema: input, steps: { a, img, b } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    expect(started.ok).toBe(true);
    if (!started.ok) return;
    // FIFO drain: `a` fails first (img still queued), then `img` completes.
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    expect(status.ok).toBe(true);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'a')!.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'img')!.status).toBe('completed');
    expect(status.value.steps.find((s) => s.key === 'b')!.status).toBe('skipped');
  });

  it('fails a step whose output violates its schema', async () => {
    const input = z.object({});
    const a = defineStep<{}, { n: number }, Ctx>({
      type: 'badout:a',
      workflowInputSchema: input,
      outputSchema: z.object({ n: z.number() }),
      // handler returns a wrong shape; defineStep phase-5 validation rejects it
      handler: async () => ({ n: 'not-a-number' } as unknown as { n: number }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'badout', inputSchema: input, steps: { a } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps[0]!.error).toMatch(/Invalid step output/);
  });

  it('threads context from buildStepContext into handlers and onAfterStep', async () => {
    const order: string[] = [];
    const seen: Array<{ key: string; output: unknown }> = [];
    const hooks: WorkflowHooks<Ctx> = {
      buildStepContext: () => ({ tag: 'injected' }),
      onAfterStep: async ({ step, output }) => {
        seen.push({ key: step.key, output });
      },
    };
    const h = harness({ hooks });
    const wf = linearWorkflow(order);
    wf.register(h.registry);

    const started = await wf.start(h.engine, { seed: 'y' });
    if (!started.ok) return;
    await h.drain();

    expect(order[0]).toBe('a:injected');
    expect(seen.map((s) => s.key)).toEqual(['a', 'b', 'c']);
    expect(seen[0]!.output).toEqual({ a: 'y-a' });
  });

  it('lets onBeforeStart reject a start (e.g. quota) without creating a workflow', async () => {
    const hooks: WorkflowHooks<Ctx> = {
      onBeforeStart: async () => ({ ok: false, error: { key: 'quota_exceeded', message: 'over limit' } }),
    };
    const order: string[] = [];
    const h = harness({ hooks });
    const wf = linearWorkflow(order);
    wf.register(h.registry);

    const res = await wf.start(h.engine, { seed: 'z' });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.key).toBe('quota_exceeded');
    const all = await h.engine.listWorkflows();
    if (!all.ok) return;
    expect(all.value.length).toBe(0);
  });

  it('persists onBeforeStart metadata onto the workflow row', async () => {
    const hooks: WorkflowHooks<Ctx> = {
      onBeforeStart: async () => ({ ok: true, value: { metadata: { keySource: 'platform' } } }),
    };
    const order: string[] = [];
    const h = harness({ hooks });
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'm' }, { metadata: { origin: 'test' } });
    if (!started.ok) return;
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.metadata).toEqual({ origin: 'test', keySource: 'platform' });
  });

  it('cancels a running workflow: pending steps become skipped', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'c' });
    if (!started.ok) return;

    // cancel before draining: all steps still pending
    const res = await h.engine.cancelWorkflow(started.value.workflowId);
    expect(res.ok).toBe(true);

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('cancelled');
    expect(status.value.steps.every((s) => s.status === 'skipped')).toBe(true);
  });

  it('recovers a step stuck in running past the expiry and fails the workflow', async () => {
    // freeze time; the step will be marked running "now", recovery runs "later"
    let current = new Date('2026-01-01T00:00:00.000Z');
    const input = z.object({});
    const a = defineStep<{}, { v: number }, Ctx>({
      type: 'stuck:a',
      workflowInputSchema: input,
      outputSchema: z.object({ v: z.number() }),
      // never resolves within the test; we simulate a crash by not draining it
      handler: async () => ({ v: 1 }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'stuck', inputSchema: input, steps: { a } });

    const store = createInMemoryWorkflowStore();
    const registry = createStepHandlerRegistry<Ctx>();
    wf.register(registry);
    const queue: DispatchStepPayload[] = [];
    const dispatcher: Dispatcher = { async enqueueStep(p) { queue.push(p); return { ok: true, value: undefined }; } };
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test', now: () => current });

    const started = await wf.start(engine, {});
    if (!started.ok) return;

    // simulate a worker that marked the step running then crashed
    const step = queue.shift()!;
    await store.markStepRunning(step.stepId, current.toISOString());

    // advance time well past expiry (600 + 300 + slack)
    current = new Date(current.getTime() + (600 + 300 + 60) * 1000);
    const recovered = await engine.recoverStuckWorkflows();
    expect(recovered.recoveredSteps).toBe(1);
    expect(recovered.recoveredWorkflows).toBe(1);

    const status = await engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps[0]!.status).toBe('failed');
  });

  it('treats a re-delivered job for an already-completed step as a no-op', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'd' });
    if (!started.ok) return;
    await h.drain();

    // replay the very first step job; it must not run the handler again
    const before = order.length;
    const res = await h.engine.executeStep(started.value.workflowId, 1);
    expect(res.ok).toBe(true);
    expect(order.length).toBe(before);
  });

  it('runs the handler once when the same job is delivered to two workers concurrently', async () => {
    const h = harness();
    let runs = 0;
    const input = z.object({});
    const once = defineStep<{}, { n: number }, Ctx>({
      type: 'claim:once',
      workflowInputSchema: input,
      outputSchema: z.object({ n: z.number() }),
      handler: async () => {
        runs++;
        // Yield the event loop so the two deliveries genuinely overlap: without
        // an atomic claim, both would have read the step as `pending` by now.
        await new Promise((r) => setTimeout(r, 5));
        return { n: runs };
      },
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'claim', inputSchema: input, steps: { once } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    const job = h.queue.shift()!;

    // two workers handed the same job by an at-least-once dispatcher
    const [a, b] = await Promise.all([
      h.engine.executeStep(job.workflowId, job.stepId),
      h.engine.executeStep(job.workflowId, job.stepId),
    ]);
    expect(a.ok).toBe(true);
    expect(b.ok).toBe(true);
    expect(runs).toBe(1);

    await h.drain();
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    // the loser must not have burned an attempt or double-counted the step
    expect(status.value.completedSteps).toBe(1);
    const steps = await h.store.listSteps(started.value.workflowId);
    expect(steps[0]?.attempts).toBe(1);
  });

  it('re-drives the advance when a completed step is redelivered after a lost dispatch', async () => {
    // The engine commits `completeStep` and then enqueues the newly-ready steps as
    // separate operations. A crash in that window leaves the dependents `pending`
    // with no job behind them — invisible to the stuck-step sweeper, which only
    // looks at `running`. The queue redelivering the completed step's job is the
    // only signal left, so that redelivery has to re-drive readiness.
    const order: string[] = [];
    const store = createInMemoryWorkflowStore();
    const registry = createStepHandlerRegistry<Ctx>();
    const queue: DispatchStepPayload[] = [];
    let loseB = true;
    const dispatcher: Dispatcher = {
      async enqueueStep(p) {
        if (p.stepKey === 'b' && loseB) {
          loseB = false; // reports success, job never arrives — as a crash would look
          return { ok: true, value: undefined };
        }
        queue.push(p);
        return { ok: true, value: undefined };
      },
    };
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test' });
    const wf = linearWorkflow(order); // a → b → c
    wf.register(registry);

    const drain = async () => {
      let guard = 0;
      while (queue.length) {
        if (++guard > 100) throw new Error('runaway');
        const p = queue.shift()!;
        try { await engine.executeStep(p.workflowId, p.stepId); } catch { /* DLQ */ }
      }
    };

    const started = await wf.start(engine, { seed: 'x' });
    if (!started.ok) return;
    await drain();

    // `a` ran, `b`'s job was lost — the workflow is stalled, and nothing reports it.
    expect(order).toEqual(['a:none']);
    const stalled = await engine.getWorkflowStatus(started.value.workflowId);
    if (!stalled.ok) return;
    expect(stalled.value.status).toBe('running');
    expect(stalled.value.steps.find((s) => s.key === 'b')!.status).toBe('pending');

    // The worker died before acking, so the queue redelivers `a`'s job.
    const aStep = (await store.listSteps(started.value.workflowId)).find((s) => s.key === 'a')!;
    await engine.executeStep(started.value.workflowId, aStep.id);
    await drain();

    // `a` must not run twice, but the DAG must advance.
    expect(order).toEqual(['a:none', 'b', 'c']);
    const done = await engine.getWorkflowStatus(started.value.workflowId);
    if (!done.ok) return;
    expect(done.value.status).toBe('completed');
    // Re-driving must not re-write the completion: the counter would inflate to 4.
    expect(done.value.completedSteps).toBe(3);
    expect(done.value.steps.filter((s) => s.key === 'a')).toHaveLength(1);
  });

  it('handleStepExhausted marks the step failed and cascades the workflow', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'e' });
    if (!started.ok) return;

    // the root step job exhausted its retries in the dispatcher and hit the DLQ
    const job = h.queue.shift()!;
    await h.engine.handleStepExhausted(started.value.workflowId, job.stepId, 'Exhausted all retries');

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    const failed = status.value.steps.find((s) => s.id === job.stepId)!;
    expect(failed.status).toBe('failed');
    expect(failed.error).toMatch(/Exhausted/);
    // transitive cascade: a→b→c, so both downstream steps are skipped
    expect(status.value.steps.filter((s) => s.status === 'skipped').map((s) => s.key).sort()).toEqual(['b', 'c']);
  });

  it('handleStepExhausted is a no-op for an already-terminal step', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'f' });
    if (!started.ok) return;
    await h.drain(); // everything completes
    await h.engine.handleStepExhausted(started.value.workflowId, 1, 'late');
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed'); // unchanged
  });

  it('lists workflows filtered by type, entityRef, and status', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const s1 = await wf.start(h.engine, { seed: '1' }, { entityRef: 'listing:1' });
    const s2 = await wf.start(h.engine, { seed: '2' }, { entityRef: 'listing:2' });
    if (!s1.ok || !s2.ok) return;
    await h.drain();

    const byType = await h.engine.listWorkflows({ type: 'linear' });
    if (!byType.ok) return;
    expect(byType.value.length).toBe(2);

    const byEntity = await h.engine.listWorkflows({ entityRef: 'listing:1' });
    if (!byEntity.ok) return;
    expect(byEntity.value.map((w) => w.entityRef)).toEqual(['listing:1']);

    const byStatus = await h.engine.listWorkflows({ status: 'completed' });
    if (!byStatus.ok) return;
    expect(byStatus.value.length).toBe(2);
  });

  it('rejects executeStep when the step belongs to a different workflow', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const a = await wf.start(h.engine, { seed: 'a' });
    const b = await wf.start(h.engine, { seed: 'b' });
    if (!a.ok || !b.ok) return;
    // steps 1..3 belong to workflow a; ask workflow b to run step 1
    const res = await h.engine.executeStep(b.value.workflowId, 1);
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.key).toBe('workflow_not_found');
  });

  it('start succeeds but enqueues nothing when the dispatcher rejects', async () => {
    const store = createInMemoryWorkflowStore();
    const registry = createStepHandlerRegistry<Ctx>();
    const order: string[] = [];
    const wf = linearWorkflow(order);
    wf.register(registry);
    const failing: Dispatcher = { async enqueueStep() { return { ok: false, error: { key: 'queue_error', message: 'down' } }; } };
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher: failing, partitionKey: 'test' });
    const started = await wf.start(engine, { seed: 'x' });
    expect(started.ok).toBe(true);
    if (!started.ok) return;
    expect(started.value.enqueuedSteps).toEqual([]);
  });

  it('fires the onWorkflowCompleted hook once on completion', async () => {
    const completed: number[] = [];
    const h = harness({ hooks: { onWorkflowCompleted: async ({ workflowId }) => { completed.push(workflowId); } } });
    const order: string[] = [];
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'h' });
    if (!started.ok) return;
    await h.drain();
    await new Promise((r) => setTimeout(r, 0)); // hook is fire-and-forget
    expect(completed).toEqual([started.value.workflowId]);
  });
});

// ---------------------------------------------------------------------------
// Per-step retry & timeout policy
// ---------------------------------------------------------------------------

describe('per-step retry policy', () => {
  const empty = z.object({});
  const okOut = z.object({ ok: z.boolean() });

  function singleStepWorkflow<S extends TypedStep<any, any, any, Ctx>>(step: S) {
    return buildWorkflow<Record<string, never>, Ctx>({
      type: 'retry-wf',
      inputSchema: empty,
      steps: { only: step },
    });
  }

  it('retries an explicitly-marked error the message heuristic would reject', async () => {
    const calls = { n: 0 };
    const step = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'marked:retry',
      workflowInputSchema: empty,
      outputSchema: okOut,
      retry: { maxAttempts: 3, backoff: 'fixed', initialDelayMs: 1 },
      handler: async () => {
        calls.n++;
        // "encoder busy" is not in the heuristic's vocabulary — the marker decides.
        if (calls.n < 3) throw retryableError('encoder busy');
        return { ok: true };
      },
    });
    const h = harness();
    const wf = singleStepWorkflow(step);
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();
    expect(calls.n).toBe(3);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
  });

  it('does not retry an explicitly non-retryable error the heuristic would accept', async () => {
    const calls = { n: 0 };
    const step = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'marked:permanent',
      workflowInputSchema: empty,
      outputSchema: okOut,
      retry: { maxAttempts: 5, backoff: 'fixed', initialDelayMs: 1 },
      handler: async () => {
        calls.n++;
        // contains "timeout", so the heuristic alone would retry this forever
        throw nonRetryableError('bug: timeout must be positive');
      },
    });
    const h = harness();
    const wf = singleStepWorkflow(step);
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();
    expect(calls.n).toBe(1);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
  });

  it("uses the step's isRetryable predicate instead of the heuristic", async () => {
    const calls = { n: 0 };
    const step = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'predicate:retry',
      workflowInputSchema: empty,
      outputSchema: okOut,
      retry: { maxAttempts: 3, backoff: 'fixed', initialDelayMs: 1 },
      isRetryable: (e) => (e as { status?: number }).status === 503,
      handler: async () => {
        calls.n++;
        if (calls.n < 2) throw Object.assign(new Error('upstream said no'), { status: 503 });
        throw Object.assign(new Error('upstream said no'), { status: 400 });
      },
    });
    const h = harness();
    const wf = singleStepWorkflow(step);
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();
    // attempt 1 retried (503), attempt 2 terminal (400) — the predicate decided both
    expect(calls.n).toBe(2);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
  });

  it('an explicit marker outranks the step predicate', async () => {
    const calls = { n: 0 };
    const step = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'precedence',
      workflowInputSchema: empty,
      outputSchema: okOut,
      retry: { maxAttempts: 4, backoff: 'fixed', initialDelayMs: 1 },
      isRetryable: () => true, // predicate says "always retry"…
      handler: async () => {
        calls.n++;
        throw nonRetryableError('give up'); // …the marker says otherwise, and wins
      },
    });
    const h = harness();
    const wf = singleStepWorkflow(step);
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();
    expect(calls.n).toBe(1);
  });

  it('defaultRetryable: false stops the classifier guessing, without silencing explicit decisions', async () => {
    const calls = { guessed: 0, marked: 0 };
    const empty2 = z.object({});

    // A failure the classifier would guess is transient (message says "timeout").
    const guessed = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'strict:guessed',
      workflowInputSchema: empty2,
      outputSchema: okOut,
      retry: { maxAttempts: 4, backoff: 'fixed', initialDelayMs: 1 },
      handler: async () => {
        calls.guessed++;
        throw new Error('request timeout after 30s');
      },
    });
    // The same failure, but explicitly marked.
    const marked = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'strict:marked',
      workflowInputSchema: empty2,
      outputSchema: okOut,
      retry: { maxAttempts: 3, backoff: 'fixed', initialDelayMs: 1 },
      handler: async () => {
        calls.marked++;
        if (calls.marked < 3) throw retryableError('encoder busy');
        return { ok: true };
      },
    });

    function strictHarness() {
      const store = createInMemoryWorkflowStore();
      const registry = createStepHandlerRegistry<Ctx>();
      const queue: DispatchStepPayload[] = [];
      const dispatcher: Dispatcher = { async enqueueStep(p) { queue.push(p); return { ok: true, value: undefined }; } };
      const engine = createWorkflowEngine<Ctx>({
        store, registry, dispatcher, partitionKey: 'test',
        config: { defaultRetryable: false },
      });
      return { registry, engine, queue, async drain() {
        let g = 0;
        while (queue.length) {
          if (++g > 500) throw new Error('runaway');
          const p = queue.shift()!;
          try { await engine.executeStep(p.workflowId, p.stepId); } catch { /* DLQ */ }
        }
      } };
    }

    const h1 = strictHarness();
    const wf1 = buildWorkflow<Record<string, never>, Ctx>({ type: 'strict-a', inputSchema: empty2, steps: { only: guessed } });
    wf1.register(h1.registry);
    const s1 = await wf1.start(h1.engine, {});
    if (!s1.ok) return;
    await h1.drain();
    expect(calls.guessed).toBe(1); // the guess was overridden — no retry

    const h2 = strictHarness();
    const wf2 = buildWorkflow<Record<string, never>, Ctx>({ type: 'strict-b', inputSchema: empty2, steps: { only: marked } });
    wf2.register(h2.registry);
    const s2 = await wf2.start(h2.engine, {});
    if (!s2.ok) return;
    await h2.drain();
    expect(calls.marked).toBe(3); // an explicit marker still retries under strict mode
  });

  it('retries a retryable failure with backoff, then succeeds', async () => {
    const calls = { n: 0 };
    const flaky = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'retry:flaky',
      workflowInputSchema: empty,
      outputSchema: okOut,
      retry: { maxAttempts: 3, backoff: 'exponential', initialDelayMs: 10 },
      handler: async () => {
        calls.n += 1;
        if (calls.n < 3) throw new Error('429 rate limit exceeded'); // retryable
        return { ok: true };
      },
    });
    const h = harness();
    const wf = singleStepWorkflow(flaky);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(calls.n).toBe(3); // 1 initial + 2 retries
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.steps[0]!.attempts).toBe(3);
    // root enqueue has no delay; the two retries carry backoff delays (ceil(ms/1000) = 1s each)
    expect(h.delays).toEqual([undefined, 1, 1]);
  });

  it('fails terminally after exhausting the attempt budget', async () => {
    const calls = { n: 0 };
    const always = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'retry:always',
      workflowInputSchema: empty,
      outputSchema: okOut,
      retry: { maxAttempts: 2, initialDelayMs: 1 },
      handler: async () => { calls.n += 1; throw new Error('connection timeout'); }, // retryable
    });
    const h = harness();
    const wf = singleStepWorkflow(always);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(calls.n).toBe(2); // 1 initial + 1 retry, then terminal
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps[0]!.status).toBe('failed');
    expect(status.value.steps[0]!.attempts).toBe(2);
  });

  it('does not retry a non-retryable failure even with a generous budget', async () => {
    const calls = { n: 0 };
    const badOutput = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'retry:bad-output',
      workflowInputSchema: empty,
      outputSchema: okOut,
      retry: { maxAttempts: 5, initialDelayMs: 1 },
      // schema mismatch → non-retryable (programming error), should fail on attempt 1
      handler: async () => { calls.n += 1; return { ok: 'nope' } as unknown as { ok: boolean }; },
    });
    const h = harness();
    const wf = singleStepWorkflow(badOutput);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(calls.n).toBe(1);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps[0]!.attempts).toBe(1);
    expect(h.delays).toEqual([undefined]); // no retry enqueue
  });

  it('times out a slow step (retryable) and succeeds on the next attempt', async () => {
    const calls = { n: 0 };
    const slow = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'retry:slow',
      workflowInputSchema: empty,
      outputSchema: okOut,
      timeoutMs: 20,
      retry: { maxAttempts: 2, initialDelayMs: 1 },
      handler: async () => {
        calls.n += 1;
        if (calls.n === 1) await new Promise((r) => setTimeout(r, 100)); // exceeds the 20ms timeout
        return { ok: true };
      },
    });
    const h = harness();
    const wf = singleStepWorkflow(slow);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(calls.n).toBe(2);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.steps[0]!.attempts).toBe(2);
  });

});

// ---------------------------------------------------------------------------
// Durable sleep / delayed steps
// ---------------------------------------------------------------------------

describe('durable sleep / delayed steps', () => {
  const input = z.object({});

  it('holds a delayed root step in the queue with startAfterSeconds', async () => {
    const ran: string[] = [];
    const delayed = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'delay:root',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      delayMs: 5000,
      handler: async () => { ran.push('delayed'); return { ok: true }; },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'delay-wf', inputSchema: input, steps: { delayed } });
    const h = harness();
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    expect(h.delays).toEqual([5]); // ceil(5000 / 1000)

    await h.drain();
    expect(ran).toEqual(['delayed']);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
  });

  it('runs a sleep step between two steps and advances the DAG', async () => {
    const order: string[] = [];
    const a = defineStep<Record<string, never>, { v: number }, Ctx>({
      type: 'sleep:a', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }),
      handler: async () => { order.push('a'); return { v: 1 }; },
    });
    const nap = defineSleepStep<Ctx, { a: typeof a }>({ type: 'sleep:nap', sleepMs: 30000, dependencies: { a } });
    const c = defineStep<Record<string, never>, { v: number }, Ctx, { nap: typeof nap }>({
      type: 'sleep:c', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), dependencies: { nap },
      handler: async () => { order.push('c'); return { v: 2 }; },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'sleep-wf', inputSchema: input, steps: { a, nap, c } });
    const h = harness();
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    expect(h.delays).toEqual([undefined]); // root 'a' has no delay

    await h.drain();
    expect(order).toEqual(['a', 'c']); // the sleep is a no-op interposed between a and c
    expect(h.delays).toContain(30); // the nap step was enqueued with a 30s durable delay
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.completedSteps).toBe(3);
  });
});

describe('per-step retry policy (continued)', () => {
  const empty = z.object({});
  const okOut = z.object({ ok: z.boolean() });

  function singleStepWorkflow<S extends TypedStep<any, any, any, Ctx>>(step: S) {
    return buildWorkflow<Record<string, never>, Ctx>({ type: 'retry-wf2', inputSchema: empty, steps: { only: step } });
  }

  it('defaults to a single attempt (no retry) when no policy is set', async () => {
    const calls = { n: 0 };
    const noPolicy = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'retry:none',
      workflowInputSchema: empty,
      outputSchema: okOut,
      handler: async () => { calls.n += 1; throw new Error('429 rate limit'); }, // retryable, but no policy
    });
    const h = harness();
    const wf = singleStepWorkflow(noPolicy);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(calls.n).toBe(1); // retryable error but maxAttempts defaults to 1
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps[0]!.attempts).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Admission gate: concurrency / rate limiting
// ---------------------------------------------------------------------------

describe('admission gate', () => {
  it('defers a denied step without consuming an attempt, then runs it', async () => {
    const input = z.object({});
    const ran = { n: 0 };
    const step = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'gate:one',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      handler: async () => { ran.n += 1; return { ok: true }; },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'gate-wf', inputSchema: input, steps: { step } });

    // gate denies the first acquire (retry in 7s), admits afterwards
    let calls = 0;
    const gate: StepGate = {
      async acquire() {
        calls += 1;
        if (calls === 1) return { admitted: false, retryAfterSeconds: 7 };
        return { admitted: true, release() {} };
      },
    };

    const store = createInMemoryWorkflowStore();
    const registry = createStepHandlerRegistry<Ctx>();
    wf.register(registry);
    const queue: DispatchStepPayload[] = [];
    const delays: Array<number | undefined> = [];
    const dispatcher: Dispatcher = {
      async enqueueStep(p, o) { queue.push(p); delays.push(o?.startAfterSeconds); return { ok: true, value: undefined }; },
    };
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, gate, partitionKey: 'test' });

    const started = await wf.start(engine, {});
    if (!started.ok) return;

    let guard = 0;
    while (queue.length > 0) {
      if (++guard > 100) throw new Error('runaway');
      const p = queue.shift()!;
      await engine.executeStep(p.workflowId, p.stepId);
    }

    expect(calls).toBe(2);       // denied once, admitted once
    expect(ran.n).toBe(1);       // handler ran exactly once
    expect(delays).toContain(7); // the deferral re-enqueue carried the gate's delay
    const status = await engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.steps[0]!.attempts).toBe(1); // deferral did NOT consume an attempt
  });
});

// ---------------------------------------------------------------------------
// Workflow-start idempotency
// ---------------------------------------------------------------------------

describe('workflow-start idempotency', () => {
  it('a repeated start with the same idempotencyKey returns the existing workflow, no re-run', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order); // a → b → c
    wf.register(h.registry);

    const first = await wf.start(h.engine, { seed: 'x' }, { idempotencyKey: 'job-1' });
    expect(first.ok).toBe(true);
    if (!first.ok) return;
    await h.drain();
    expect(order).toEqual(['a:none', 'b', 'c']);

    // same key → existing workflow, nothing re-enqueued or re-run
    const second = await wf.start(h.engine, { seed: 'x' }, { idempotencyKey: 'job-1' });
    expect(second.ok).toBe(true);
    if (!second.ok) return;
    expect(second.value.workflowId).toBe(first.value.workflowId);
    expect(second.value.enqueuedSteps).toEqual([]);
    expect(h.queue.length).toBe(0);
    await h.drain();
    expect(order).toEqual(['a:none', 'b', 'c']);

    // a different key starts a fresh workflow
    const third = await wf.start(h.engine, { seed: 'y' }, { idempotencyKey: 'job-2' });
    expect(third.ok).toBe(true);
    if (!third.ok) return;
    expect(third.value.workflowId).not.toBe(first.value.workflowId);
    expect(third.value.enqueuedSteps.length).toBeGreaterThan(0);
  });

  it('an unkeyed start always creates a new workflow', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);

    const a = await wf.start(h.engine, { seed: 'x' });
    const b = await wf.start(h.engine, { seed: 'x' });
    if (!a.ok || !b.ok) return;
    expect(b.value.workflowId).not.toBe(a.value.workflowId);
  });
});

// ---------------------------------------------------------------------------
// waitForEvent / signals
// ---------------------------------------------------------------------------

describe('waitForEvent / signals', () => {
  const input = z.object({});

  it('suspends a wait step until resumeStep, then advances the DAG', async () => {
    const order: string[] = [];
    const a = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'wait:a', workflowInputSchema: input, outputSchema: z.object({ ok: z.boolean() }),
      handler: async () => { order.push('a'); return { ok: true }; },
    });
    const approval = defineWaitStep<{ approved: boolean }, Ctx, { a: typeof a }>({
      type: 'wait:approval', outputSchema: z.object({ approved: z.boolean() }), dependencies: { a },
    });
    const c = defineStep<Record<string, never>, { done: boolean }, Ctx, { approval: typeof approval }>({
      type: 'wait:c', workflowInputSchema: input, outputSchema: z.object({ done: z.boolean() }), dependencies: { approval },
      handler: async (ctx) => { order.push(`c:${ctx.deps.approval.approved}`); return { done: true }; },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'wait-wf', inputSchema: input, steps: { a, approval, c } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    // a done, approval suspended (not enqueued), workflow still running
    let status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('running');
    expect(status.value.steps.find((s) => s.key === 'approval')!.status).toBe('waiting');
    expect(order).toEqual(['a']);
    expect(h.queue.length).toBe(0);

    // deliver the external event
    const resumed = await h.engine.resumeStep(started.value.workflowId, 'approval', { approved: true });
    expect(resumed.ok).toBe(true);
    await h.drain();

    status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(order).toEqual(['a', 'c:true']);
    expect(status.value.output).toMatchObject({ approval: { approved: true }, c: { done: true } });

    // resuming again is an idempotent no-op
    const again = await h.engine.resumeStep(started.value.workflowId, 'approval', { approved: false });
    expect(again.ok).toBe(true);
    expect(order).toEqual(['a', 'c:true']);
  });

  it('resumeStep errors for an unknown step and is a no-op for a non-waiting one', async () => {
    const order: string[] = [];
    const h = harness();
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'x' });
    if (!started.ok) return;

    const unknown = await h.engine.resumeStep(started.value.workflowId, 'nope', {});
    expect(unknown.ok).toBe(false);

    await h.drain();
    const noop = await h.engine.resumeStep(started.value.workflowId, 'a', {}); // 'a' is completed, not waiting
    expect(noop.ok).toBe(true);
  });

  it('cancel skips a waiting step', async () => {
    const gate = defineWaitStep<{ x: number }, Ctx>({ type: 'wcancel:gate', outputSchema: z.object({ x: z.number() }) });
    const after = defineStep<Record<string, never>, { ok: boolean }, Ctx, { gate: typeof gate }>({
      type: 'wcancel:after', workflowInputSchema: input, outputSchema: z.object({ ok: z.boolean() }), dependencies: { gate },
      handler: async () => ({ ok: true }),
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'wcancel', inputSchema: input, steps: { gate, after } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    let status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.steps.find((s) => s.key === 'gate')!.status).toBe('waiting');

    await h.engine.cancelWorkflow(started.value.workflowId);
    status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('cancelled');
    expect(status.value.steps.every((s) => s.status === 'skipped')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// dynamic fan-out / map
// ---------------------------------------------------------------------------

describe('dynamic fan-out / map', () => {
  const input = z.object({});

  it('fans out over a runtime list, runs children, aggregates ordered outputs', async () => {
    const processed: number[] = [];
    const mapper = defineMapStep<number, { doubled: number }, Record<string, never>, Ctx>({
      type: 'map:double', workflowInputSchema: input, itemOutputSchema: z.object({ doubled: z.number() }),
      items: () => [1, 2, 3, 4],
      each: async (n) => { processed.push(n); return { doubled: n * 2 }; },
    });
    const sum = defineStep<Record<string, never>, { total: number }, Ctx, { mapper: typeof mapper }>({
      type: 'map:sum', workflowInputSchema: input, outputSchema: z.object({ total: z.number() }), dependencies: { mapper },
      handler: async (ctx) => ({ total: ctx.deps.mapper.items.reduce((a, x) => a + x.doubled, 0) }),
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'map-wf', inputSchema: input, steps: { mapper, sum } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(processed).toEqual([1, 2, 3, 4]);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.output).toMatchObject({
      mapper: { items: [{ doubled: 2 }, { doubled: 4 }, { doubled: 6 }, { doubled: 8 }] },
      sum: { total: 20 },
    });
    // 4 child steps were materialized at runtime
    expect(status.value.steps.filter((s) => s.parentStepId != null).length).toBe(4);
    expect(status.value.steps.find((s) => s.key === 'mapper')!.status).toBe('completed');
  });

  it('completes immediately with an empty result when there are no items', async () => {
    const mapper = defineMapStep<number, { x: number }, Record<string, never>, Ctx>({
      type: 'mapempty:m', workflowInputSchema: input, itemOutputSchema: z.object({ x: z.number() }),
      items: () => [], each: async (n) => ({ x: n }),
    });
    const after = defineStep<Record<string, never>, { empty: boolean }, Ctx, { mapper: typeof mapper }>({
      type: 'mapempty:after', workflowInputSchema: input, outputSchema: z.object({ empty: z.boolean() }), dependencies: { mapper },
      handler: async (ctx) => ({ empty: ctx.deps.mapper.items.length === 0 }),
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'map-empty', inputSchema: input, steps: { mapper, after } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.output).toMatchObject({ mapper: { items: [] }, after: { empty: true } });
    expect(status.value.steps.filter((s) => s.parentStepId != null).length).toBe(0);
  });

  it('fails the map and cascades downstream when an item errors', async () => {
    const mapper = defineMapStep<number, { x: number }, Record<string, never>, Ctx>({
      type: 'mapfail:m', workflowInputSchema: input, itemOutputSchema: z.object({ x: z.number() }),
      items: () => [1, 2, 3],
      each: async (n) => { if (n === 2) throw new Error('item 2 boom'); return { x: n }; },
    });
    const after = defineStep<Record<string, never>, { ok: boolean }, Ctx, { mapper: typeof mapper }>({
      type: 'mapfail:after', workflowInputSchema: input, outputSchema: z.object({ ok: z.boolean() }), dependencies: { mapper },
      handler: async () => ({ ok: true }),
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'map-fail', inputSchema: input, steps: { mapper, after } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'mapper')!.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'after')!.status).toBe('skipped');
  });

  it('retries a flaky item then succeeds (per-item retry)', async () => {
    const attempts: Record<number, number> = {};
    const mapper = defineMapStep<number, { x: number }, Record<string, never>, Ctx>({
      type: 'mapretry:m', workflowInputSchema: input, itemOutputSchema: z.object({ x: z.number() }),
      items: () => [1, 2],
      itemRetry: { maxAttempts: 3, initialDelayMs: 1 },
      each: async (n) => {
        attempts[n] = (attempts[n] ?? 0) + 1;
        if (n === 2 && attempts[n] < 2) throw new Error('429 rate limit');
        return { x: n };
      },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'map-retry', inputSchema: input, steps: { mapper } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(attempts[2]).toBe(2);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.output).toMatchObject({ mapper: { items: [{ x: 1 }, { x: 2 }] } });
  });
});

// ---------------------------------------------------------------------------
// child / sub-workflows
// ---------------------------------------------------------------------------

describe('child / sub-workflows', () => {
  const input = z.object({});

  it('runs a child workflow and resumes the parent step with its output', async () => {
    const order: string[] = [];
    const childInput = z.object({ n: z.number() });
    const double = defineStep<{ n: number }, { doubled: number }, Ctx>({
      type: 'sub:double', workflowInputSchema: childInput, outputSchema: z.object({ doubled: z.number() }),
      handler: async (ctx) => { order.push('child'); return { doubled: ctx.workflowInput.n * 2 }; },
    });
    const childWf = buildWorkflow<{ n: number }, Ctx>({ type: 'sub-child', inputSchema: childInput, steps: { double } });

    const callChild = defineSubWorkflowStep<{ double: { doubled: number } }, Record<string, never>, Ctx>({
      type: 'sub:call', workflowInputSchema: input, childWorkflow: childWf,
      input: () => ({ n: 21 }),
      outputSchema: z.object({ double: z.object({ doubled: z.number() }) }),
    });
    const after = defineStep<Record<string, never>, { result: number }, Ctx, { callChild: typeof callChild }>({
      type: 'sub:after', workflowInputSchema: input, outputSchema: z.object({ result: z.number() }), dependencies: { callChild },
      handler: async (ctx) => { order.push('after'); return { result: ctx.deps.callChild.double.doubled }; },
    });
    const parentWf = buildWorkflow<Record<string, never>, Ctx>({ type: 'sub-parent', inputSchema: input, steps: { callChild, after } });

    const h = harness();
    parentWf.register(h.registry);
    const started = await parentWf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(order).toEqual(['child', 'after']);
    expect(status.value.output).toMatchObject({
      callChild: { double: { doubled: 42 } },
      after: { result: 42 },
    });
    // the parent 'callChild' step was suspended then completed
    expect(status.value.steps.find((s) => s.key === 'callChild')!.status).toBe('completed');
  });

  it('fails the parent step (and cascades) when the child workflow fails', async () => {
    const childInput = z.object({});
    const boom = defineStep<Record<string, never>, { x: number }, Ctx>({
      type: 'subfail:boom', workflowInputSchema: childInput, outputSchema: z.object({ x: z.number() }),
      handler: async () => { throw new Error('child boom'); },
    });
    const childWf = buildWorkflow<Record<string, never>, Ctx>({ type: 'subfail-child', inputSchema: childInput, steps: { boom } });

    const call = defineSubWorkflowStep<Record<string, unknown>, Record<string, never>, Ctx>({
      type: 'subfail:call', workflowInputSchema: input, childWorkflow: childWf, input: () => ({}),
    });
    const after = defineStep<Record<string, never>, { ok: boolean }, Ctx, { call: typeof call }>({
      type: 'subfail:after', workflowInputSchema: input, outputSchema: z.object({ ok: z.boolean() }), dependencies: { call },
      handler: async () => ({ ok: true }),
    });
    const parentWf = buildWorkflow<Record<string, never>, Ctx>({ type: 'subfail-parent', inputSchema: input, steps: { call, after } });

    const h = harness();
    parentWf.register(h.registry);
    const started = await parentWf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'call')!.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'after')!.status).toBe('skipped');
  });
});

// ---------------------------------------------------------------------------
// compensation / saga
// ---------------------------------------------------------------------------

describe('compensation / saga', () => {
  const input = z.object({});

  it('compensates completed steps in reverse order when a later step fails', async () => {
    const events: string[] = [];
    const a = defineStep<Record<string, never>, { a: number }, Ctx>({
      type: 'saga:a', workflowInputSchema: input, outputSchema: z.object({ a: z.number() }),
      handler: async () => { events.push('run:a'); return { a: 1 }; },
      compensate: (ctx) => { events.push(`undo:a:${ctx.output.a}`); },
    });
    const b = defineStep<Record<string, never>, { b: number }, Ctx, { a: typeof a }>({
      type: 'saga:b', workflowInputSchema: input, outputSchema: z.object({ b: z.number() }), dependencies: { a },
      handler: async (ctx) => { events.push('run:b'); return { b: ctx.deps.a.a + 1 }; },
      compensate: (ctx) => { events.push(`undo:b:${ctx.output.b}`); },
    });
    const c = defineStep<Record<string, never>, { c: number }, Ctx, { b: typeof b }>({
      type: 'saga:c', workflowInputSchema: input, outputSchema: z.object({ c: z.number() }), dependencies: { b },
      handler: async () => { events.push('run:c'); throw new Error('c boom'); },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'saga-wf', inputSchema: input, steps: { a, b, c } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    // a and b ran and were rolled back in reverse order; c failed (no compensate)
    expect(events).toEqual(['run:a', 'run:b', 'run:c', 'undo:b:2', 'undo:a:1']);
    expect(status.value.steps.find((s) => s.key === 'a')!.status).toBe('compensated');
    expect(status.value.steps.find((s) => s.key === 'b')!.status).toBe('compensated');
    expect(status.value.steps.find((s) => s.key === 'c')!.status).toBe('failed');
  });

  it('is best-effort: a throwing compensate is logged + surfaced, others still run', async () => {
    const events: string[] = [];
    const a = defineStep<Record<string, never>, { a: number }, Ctx>({
      type: 'saga2:a', workflowInputSchema: input, outputSchema: z.object({ a: z.number() }),
      handler: async () => ({ a: 1 }),
      compensate: () => { events.push('undo:a'); },
    });
    const b = defineStep<Record<string, never>, { b: number }, Ctx, { a: typeof a }>({
      type: 'saga2:b', workflowInputSchema: input, outputSchema: z.object({ b: z.number() }), dependencies: { a },
      handler: async () => ({ b: 2 }),
      compensate: () => { events.push('undo:b'); throw new Error('rollback b failed'); },
    });
    const c = defineStep<Record<string, never>, { c: number }, Ctx, { b: typeof b }>({
      type: 'saga2:c', workflowInputSchema: input, outputSchema: z.object({ c: z.number() }), dependencies: { b },
      handler: async () => { throw new Error('c boom'); },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'saga2-wf', inputSchema: input, steps: { a, b, c } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    // b's rollback threw but a's still ran
    expect(events).toEqual(['undo:b', 'undo:a']);
    const bStep = status.value.steps.find((s) => s.key === 'b')!;
    expect(bStep.status).toBe('compensated');
    expect(bStep.error).toContain('rollback b failed');
    expect(status.value.steps.find((s) => s.key === 'a')!.status).toBe('compensated');
  });
});

// ---------------------------------------------------------------------------
// observability — events + spans
// ---------------------------------------------------------------------------

describe('observability', () => {
  const input = z.object({});

  it('emits a run-history event stream for a successful workflow', async () => {
    const observer = createRecordingObserver();
    const order: string[] = [];
    const h = harness({ observer });
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'x' });
    if (!started.ok) return;
    await h.drain();

    const types = observer.events.map((e) => e.type);
    expect(types[0]).toBe('workflow.started');
    expect(types[types.length - 1]).toBe('workflow.completed');
    expect(types.filter((t) => t === 'step.started').length).toBe(3);
    expect(types.filter((t) => t === 'step.completed').length).toBe(3);
    // events carry partition + a duration on completion
    expect(observer.events.every((e) => e.partitionKey === 'test')).toBe(true);
    const aCompleted = observer.events.find((e) => e.type === 'step.completed' && e.stepKey === 'a')!;
    expect(aCompleted.attempt).toBe(1);
    expect(aCompleted.durationMs).toBeGreaterThanOrEqual(0);
  });

  it('emits step.failed + workflow.failed + step.skipped on failure', async () => {
    const observer = createRecordingObserver();
    const a = defineStep<Record<string, never>, { a: number }, Ctx>({
      type: 'obs:a', workflowInputSchema: input, outputSchema: z.object({ a: z.number() }),
      handler: async () => ({ a: 1 }),
    });
    const b = defineStep<Record<string, never>, { b: number }, Ctx, { a: typeof a }>({
      type: 'obs:b', workflowInputSchema: input, outputSchema: z.object({ b: z.number() }), dependencies: { a },
      handler: async () => { throw new Error('b boom'); },
    });
    const c = defineStep<Record<string, never>, { c: number }, Ctx, { b: typeof b }>({
      type: 'obs:c', workflowInputSchema: input, outputSchema: z.object({ c: z.number() }), dependencies: { b },
      handler: async () => ({ c: 3 }),
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'obs-fail', inputSchema: input, steps: { a, b, c } });

    const h = harness({ observer });
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const failed = observer.events.find((e) => e.type === 'step.failed')!;
    expect(failed.stepKey).toBe('b');
    expect(failed.error).toContain('b boom');
    expect(observer.events.some((e) => e.type === 'step.skipped' && e.stepKey === 'c')).toBe(true);
    expect(observer.events.some((e) => e.type === 'workflow.failed')).toBe(true);
  });

  it('wraps each step execution in a span', async () => {
    const tracer = createRecordingTracer();
    const order: string[] = [];
    const h = harness({ tracer });
    const wf = linearWorkflow(order);
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'x' });
    if (!started.ok) return;
    await h.drain();

    expect(tracer.spans.length).toBe(3); // one per step run
    expect(tracer.spans.every((s) => s.ended)).toBe(true);
    expect(tracer.spans[0]!.name).toBe('flow.step');
    expect(tracer.spans.map((s) => s.attributes['flow.step_key'])).toEqual(['a', 'b', 'c']);
  });

  it('records the error on the span of a failed step (and still ends it)', async () => {
    const tracer = createRecordingTracer();
    const boom = defineStep<Record<string, never>, { x: number }, Ctx>({
      type: 'obsspan:boom', workflowInputSchema: input, outputSchema: z.object({ x: z.number() }),
      handler: async () => { throw new Error('span boom'); },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'obs-span-fail', inputSchema: input, steps: { boom } });

    const h = harness({ tracer });
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(tracer.spans.length).toBe(1);
    expect(tracer.spans[0]!.ended).toBe(true);
    expect(tracer.spans[0]!.error).toContain('span boom');
  });
});

describe('atomic dispatch (capability negotiation)', () => {
  const empty = z.object({});
  const okOut = z.object({ ok: z.boolean() });

  function singleStep(type: string) {
    const step = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type, workflowInputSchema: empty, outputSchema: okOut, handler: async () => ({ ok: true }),
    });
    return buildWorkflow<Record<string, never>, Ctx>({ type: `wf-${type}`, inputSchema: empty, steps: { only: step } });
  }

  /** Wraps a store with a transaction capability, delegating to it (never to itself). */
  function withTransaction(inner: ReturnType<typeof createInMemoryWorkflowStore>) {
    let open = false;
    const store = {
      ...inner,
      isOpen: () => open,
      async runInTransaction<T>(fn: (scope: { store: typeof inner; handle: unknown }) => Promise<T>): Promise<T> {
        open = true;
        try {
          return await fn({ store: inner, handle: { query: async () => ({ rows: [] }) } });
        } finally {
          open = false;
        }
      },
    };
    return store;
  }

  it('enqueues through the transaction when both halves are capable', async () => {
    const inner = createInMemoryWorkflowStore();
    const store = withTransaction(inner);
    const registry = createStepHandlerRegistry<Ctx>();
    const usedTransactionalPath: boolean[] = [];
    const dispatcher: Dispatcher = {
      async enqueueStep() { usedTransactionalPath.push(false); return { ok: true, value: undefined }; },
      async enqueueStepIn() { usedTransactionalPath.push(store.isOpen()); return { ok: true, value: undefined }; },
    };
    const wf = singleStep('atomic:both');
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test' });
    wf.register(registry);

    await wf.start(engine, {});
    // enqueueStepIn was used, and the transaction was open while it ran.
    expect(usedTransactionalPath).toEqual([true]);
  });

  it('runs the dispatcher\'s prepare once, before the first transaction opens, and shares it across dispatches', async () => {
    const inner = createInMemoryWorkflowStore();
    const store = withTransaction(inner);
    const registry = createStepHandlerRegistry<Ctx>();
    const prepareCalls: boolean[] = [];
    const dispatcher: Dispatcher = {
      async prepare() { prepareCalls.push(store.isOpen()); return { ok: true, value: undefined }; },
      async enqueueStep() { return { ok: true, value: undefined }; },
      async enqueueStepIn() { return { ok: true, value: undefined }; },
    };
    const wf = singleStep('atomic:prepare');
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test' });
    wf.register(registry);

    await Promise.all([wf.start(engine, {}), wf.start(engine, {})]);
    await wf.start(engine, {});

    // Once for the engine's lifetime, and with no transaction open — the whole
    // point: queue DDL on a single-connection database would deadlock inside one.
    expect(prepareCalls).toEqual([false]);
  });

  it('fails the dispatch, and retries prepare next time, when prepare fails', async () => {
    const store = withTransaction(createInMemoryWorkflowStore());
    const registry = createStepHandlerRegistry<Ctx>();
    let attempts = 0;
    const dispatcher: Dispatcher = {
      async prepare() {
        attempts += 1;
        return attempts === 1
          ? { ok: false, error: { key: 'queue_error', message: 'queue DDL refused' } }
          : { ok: true, value: undefined };
      },
      async enqueueStep() { return { ok: true, value: undefined }; },
      async enqueueStepIn() { return { ok: true, value: undefined }; },
    };
    const wf = singleStep('atomic:prepare-fails');
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test' });
    wf.register(registry);

    await expect(wf.start(engine, {})).rejects.toThrow('queue DDL refused');
    await expect(wf.start(engine, {})).resolves.toMatchObject({ ok: true });
    expect(attempts).toBe(2);
  });

  it('surfaces an enqueue failure instead of stranding the workflow', async () => {
    const store = withTransaction(createInMemoryWorkflowStore());
    const registry = createStepHandlerRegistry<Ctx>();
    const dispatcher: Dispatcher = {
      async enqueueStep() { return { ok: true, value: undefined }; },
      async enqueueStepIn() { throw new Error('queue unreachable'); },
    };
    const wf = singleStep('atomic:throws');
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test' });
    wf.register(registry);

    // The throw propagates so the caller knows, rather than returning ok with a
    // workflow nobody will ever run. (That the row is *rolled back* is proven
    // against real Postgres — see store-pg's atomic-dispatch integration test.)
    await expect(wf.start(engine, {})).rejects.toThrow('queue unreachable');
  });

  it('falls back to the plain path when only the dispatcher is capable', async () => {
    const registry = createStepHandlerRegistry<Ctx>();
    const seen: string[] = [];
    const dispatcher: Dispatcher = {
      async enqueueStep(p) { seen.push(`plain:${p.stepKey}`); return { ok: true, value: undefined }; },
      async enqueueStepIn(_h, p) { seen.push(`tx:${p.stepKey}`); return { ok: true, value: undefined }; },
    };
    const wf = singleStep('atomic:fallback');
    const engine = createWorkflowEngine<Ctx>({
      store: createInMemoryWorkflowStore(), registry, dispatcher, partitionKey: 'test',
    });
    wf.register(registry);
    await wf.start(engine, {});
    expect(seen).toEqual(['plain:only']);
  });

  it('falls back to the plain path when only the store is capable', async () => {
    const store = withTransaction(createInMemoryWorkflowStore());
    const registry = createStepHandlerRegistry<Ctx>();
    const seen: string[] = [];
    const dispatcher: Dispatcher = {
      async enqueueStep(p) { seen.push(`plain:${p.stepKey}`); return { ok: true, value: undefined }; },
    };
    const wf = singleStep('atomic:storeonly');
    const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test' });
    wf.register(registry);
    await wf.start(engine, {});
    expect(seen).toEqual(['plain:only']);
  });
});
