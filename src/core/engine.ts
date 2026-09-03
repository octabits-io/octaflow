import type { Result } from './result';
import type { Logger } from './logger';
import { noopLogger } from './logger';
import type { WorkflowStore, TransactionalScope } from './store';
import type { Dispatcher, DispatchStepPayload } from './dispatcher';
import type { JoinRule } from './readiness';
import { computeReadiness, isTerminalStepStatus } from './readiness';
import type { StepGate } from './gate';
import type { WorkflowHooks } from './hooks';
import type { FlowObserver, FlowTracer, FlowEvent } from './observability';
import { noopObserver, noopTracer } from './observability';
import type {
  WorkflowId,
  StepId,
  WorkflowDefinition,
  WorkflowStatus,
  WorkflowRecord,
  StepRecord,
  WorkflowWithSteps,
  StepHandlerRegistry,
  StepHandler,
  StepExecutionContext,
  StepError,
  RetryPolicy,
  StartOptions,
  WorkflowCreatedResult,
  FlowError,
} from './types';

// ============================================================================
// Retry / timeout helpers
// ============================================================================

/**
 * Run a handler under an optional wall-clock timeout. On expiry the abort signal
 * is fired (cooperative handlers stop) and a retryable timeout error is returned —
 * the engine then applies the step's retry policy.
 */
async function runWithTimeout<TContext>(
  handler: StepHandler<TContext>,
  ctx: StepExecutionContext<TContext>,
  timeoutMs: number | undefined,
  abort: AbortController,
): Promise<Result<Record<string, unknown>, StepError>> {
  if (!timeoutMs || timeoutMs <= 0) return handler(ctx);
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<Result<Record<string, unknown>, StepError>>((resolve) => {
    timer = setTimeout(() => {
      abort.abort();
      resolve({
        ok: false,
        error: { key: 'step_error', message: `Step timed out after ${timeoutMs}ms`, retryable: true, retryableFrom: 'explicit' },
      });
    }, timeoutMs);
  });
  try {
    return await Promise.race([handler(ctx), timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Backoff delay (in whole seconds) before the next attempt, given the just-failed attempt number. */
function backoffDelaySeconds(retry: RetryPolicy | undefined, failedAttemptNo: number): number {
  const initialMs = retry?.initialDelayMs ?? 1000;
  const maxMs = retry?.maxDelayMs ?? 60_000;
  const baseMs = retry?.backoff === 'exponential' ? initialMs * 2 ** (failedAttemptNo - 1) : initialMs;
  return Math.max(0, Math.ceil(Math.min(baseMs, maxMs) / 1000));
}

/**
 * Reverse topological order: a step appears before its dependencies, so saga
 * compensation undoes effects in the opposite order they were produced. Pure / deterministic.
 */
function reverseTopologicalOrder(steps: StepRecord[]): StepRecord[] {
  const inSet = new Set(steps.map((s) => s.key));
  const placed = new Set<string>();
  const topo: StepRecord[] = [];
  let guard = 0;
  while (placed.size < steps.length && guard++ <= steps.length) {
    for (const s of steps) {
      if (placed.has(s.key)) continue;
      const deps = (s.dependencies ?? []).filter((d) => inSet.has(d));
      if (deps.every((d) => placed.has(d))) {
        topo.push(s);
        placed.add(s.key);
      }
    }
  }
  // Any leftover (a cycle — shouldn't happen for a validated DAG) appended as-is.
  for (const s of steps) if (!placed.has(s.key)) topo.push(s);
  return topo.reverse();
}

/** Recorded on a step its `when` guard declined to run. */
const CONDITION_NOT_MET = 'Skipped: condition not met';

/**
 * How often to beat, given how long silence is tolerated. A third of the window
 * leaves room for two lost beats before the sweeper draws a conclusion; the
 * floor keeps a very short window from turning into a write storm.
 */
const HEARTBEAT_INTERVAL_DIVISOR = 3;
const MIN_HEARTBEAT_INTERVAL_MS = 1000;

/** The heartbeat attached to one step execution. */
interface Heartbeat {
  /** Beat now (subject to the write throttle) and report whether to keep going. */
  beat(): Promise<boolean>;
  /** True once a beat came back `false` — this invocation no longer owns the step. */
  readonly lost: boolean;
  /** Stop the automatic timer. Always call it, in a `finally`. */
  stop(): void;
}

/** A step type with no liveness window declared: nothing to report, nothing to lose. */
const inertHeartbeat: Heartbeat = {
  beat: async () => true,
  lost: false,
  stop() {},
};

// ============================================================================
// Dependencies
// ============================================================================

export interface WorkflowEngineConfig {
  /** The dispatcher's per-step expiry, in seconds. Used by the stuck-step sweeper. Default 600. */
  stepExpirySeconds?: number;
  /** Extra grace added to the stuck threshold beyond `stepExpirySeconds`. Default 300. */
  stuckStepBufferSeconds?: number;
  /**
   * What `recoverStuckWorkflows` does with a step whose worker died mid-run.
   *
   * - `'retry'` (default) — put it back on the queue if its attempt budget has
   *   room, and only fail it once that budget is spent. A crashed pod costs an
   *   attempt, not the whole run. **The handler must tolerate re-execution**
   *   after a partial run, which is the same contract retries already impose.
   * - `'fail'` — fail the step outright, whatever the budget says. Choose this
   *   when re-entering a half-finished step is worse than losing the run.
   */
  onStuckStep?: 'retry' | 'fail';
  /**
   * Retryability for failures nothing authoritative classified — i.e. where the
   * default classifier fell back to guessing from the error's shape and message.
   * Set `false` for strict mode: never guess, and treat an unmarked failure as
   * permanent. Explicitly marked errors ({@link markRetryable}) and steps with
   * their own `isRetryable` are unaffected, as are engine-generated failures like
   * a step timeout.
   *
   * Leave unset to keep the default classifier's answer.
   */
  defaultRetryable?: boolean;
}

export interface WorkflowEngineDeps<TContext = unknown> {
  store: WorkflowStore;
  dispatcher: Dispatcher;
  registry: StepHandlerRegistry<TContext>;
  /** Partition this engine instance is bound to (e.g. a tenant id). */
  partitionKey: string;
  logger?: Logger;
  hooks?: WorkflowHooks<TContext>;
  config?: WorkflowEngineConfig;
  /**
   * Optional admission gate (concurrency caps / rate limiting). Consulted before each
   * step runs; a denied step is deferred (re-enqueued) without consuming an attempt.
   */
  gate?: StepGate;
  /** Run-history / metrics sink. Receives a `FlowEvent` per transition. Default: no-op. */
  observer?: FlowObserver;
  /** Tracer: the engine wraps each `executeStep` in a span. Default: no-op. */
  tracer?: FlowTracer;
  /** Clock injection point for testability. Defaults to `() => new Date()`. */
  now?: () => Date;
}

// ============================================================================
// Result shapes
// ============================================================================

/** What one sweep of `recoverStuckWorkflows` did. */
export interface RecoverySweepResult {
  /** Stuck steps put back on the queue for another attempt. */
  retriedSteps: number;
  /** Stuck steps failed outright — attempt budget spent, or `onStuckStep: 'fail'`. */
  recoveredSteps: number;
  /** Workflows a failed step pushed towards `failed`. */
  recoveredWorkflows: number;
  /** Workflows failed for outliving `StartOptions.timeoutMs`. */
  expiredWorkflows: number;
}

/** What `retryWorkflow` put back in flight. */
export interface RetryWorkflowResult {
  workflowId: WorkflowId;
  /** Keys of the steps reset to `pending` — the work that will run again. */
  resetSteps: string[];
}

// ============================================================================
// Factory
// ============================================================================

export function createWorkflowEngine<TContext = unknown>(deps: WorkflowEngineDeps<TContext>) {
  const { store, dispatcher, registry, partitionKey } = deps;
  const gate = deps.gate;
  const logger = deps.logger ?? noopLogger;
  const hooks = deps.hooks ?? {};
  const observer = deps.observer ?? noopObserver;
  const tracer = deps.tracer ?? noopTracer;
  const now = deps.now ?? (() => new Date());
  const stepExpirySeconds = deps.config?.stepExpirySeconds ?? 600;
  const stuckStepBufferSeconds = deps.config?.stuckStepBufferSeconds ?? 300;
  const defaultRetryable = deps.config?.defaultRetryable;
  const onStuckStep = deps.config?.onStuckStep ?? 'retry';
  /**
   * Both halves must opt in: the store has to offer a transaction and the
   * dispatcher has to be able to enqueue inside it. When they do, a state change
   * and the dispatches it unlocks commit atomically — there is no window in
   * which a step is `pending` with no job behind it. When either cannot, the
   * engine writes and then enqueues, exactly as before.
   */
  const atomicDispatch =
    typeof store.runInTransaction === 'function' && typeof dispatcher.enqueueStepIn === 'function';

  /**
   * The dispatcher's one-time setup (`Dispatcher.prepare`), run before the first
   * transaction opens and never inside one. Memoised as a promise so concurrent
   * first dispatches share it; cleared on failure so the next attempt retries.
   */
  let prepared: Promise<void> | null = null;
  function ensurePrepared(): Promise<void> {
    if (!dispatcher.prepare) return Promise.resolve();
    prepared ??= dispatcher.prepare().then((result) => {
      if (!result.ok) throw new Error(`Dispatcher failed to prepare: ${result.error.message}`);
    }).catch((error: unknown) => {
      prepared = null;
      throw error;
    });
    return prepared;
  }

  /**
   * Run `fn` atomically when both halves support it, otherwise run it directly.
   * `fn` receives the scope to write through — pass it to `dispatchReadyStep`.
   *
   * Only writes and enqueues belong in here. Anything that can run user code
   * (saga compensation, hooks) must stay outside, or a rollback handler making a
   * network call would hold a database transaction open while it does. The
   * dispatcher's own setup is the same kind of thing — it runs before the
   * transaction, not in it (see `Dispatcher.prepare`).
   */
  async function withDispatchScope<T>(fn: (scope?: TransactionalScope) => Promise<T>): Promise<T> {
    if (!atomicDispatch) return fn(undefined);
    await ensurePrepared();
    return store.runInTransaction!((scope) => fn(scope));
  }

  const nowIso = () => now().toISOString();

  /** Emit a run-history / metrics event. Guarded — a faulty observer never breaks a run. */
  function emit(event: Omit<FlowEvent, 'at' | 'partitionKey'>): void {
    try {
      observer.record({ ...event, partitionKey, at: nowIso() });
    } catch (e) {
      logger.error('Observer threw', e instanceof Error ? e : new Error(String(e)), { workflowId: event.workflowId });
    }
  }

  // --------------------------------------------------------------------------
  // Heartbeats
  // --------------------------------------------------------------------------

  /**
   * Attach a heartbeat to one step execution.
   *
   * While the handler runs, the beat answers two questions with one write: *this
   * worker is alive*, and *does this invocation still own the step*. A beat that
   * comes back `false` — cancelled workflow, blown deadline, or a sweeper that
   * gave the step to someone else — aborts the handler and latches `lost`, which
   * is what stops this invocation writing its outcome over the new owner's.
   *
   * The automatic timer is the part that needs no cooperation from the handler:
   * if the process dies, the timer dies with it, which is exactly the signal the
   * sweeper is looking for. `heartbeat: 'manual'` turns it off for handlers that
   * want silence to mean "hung", not just "dead".
   */
  function startHeartbeat(workflowId: WorkflowId, step: StepRecord, abort: AbortController): Heartbeat {
    const registration = registry.getRegistration(step.type);
    const windowMs = registration?.heartbeatTimeoutMs;
    if (!windowMs || windowMs <= 0) return inertHeartbeat;

    const intervalMs = Math.max(MIN_HEARTBEAT_INTERVAL_MS, Math.floor(windowMs / HEARTBEAT_INTERVAL_DIVISOR));
    let lost = false;
    let lastWriteMs = now().getTime();
    let inFlight: Promise<boolean> | undefined;

    async function write(): Promise<boolean> {
      try {
        const stillOurs = await store.heartbeatStep(step.id, nowIso());
        lastWriteMs = now().getTime();
        if (!stillOurs && !lost) {
          lost = true;
          logger.warn('Step lost its claim mid-run — aborting', { workflowId, stepId: step.id, stepKey: step.key });
          abort.abort();
        }
        return stillOurs;
      } catch (error) {
        // A failed beat is not a verdict. Treating a blip in the database as
        // "you have been cancelled" would kill healthy work; the sweeper is the
        // backstop if the worker really is gone.
        logger.error('Heartbeat failed', error instanceof Error ? error : new Error(String(error)), {
          workflowId,
          stepId: step.id,
        });
        return !lost;
      }
    }

    async function beat(): Promise<boolean> {
      if (lost) return false;
      // Throttle: the handler may call this per loop iteration, but only one
      // write per interval lands. Concurrent callers share the in-flight write.
      if (inFlight) return inFlight;
      if (now().getTime() - lastWriteMs < intervalMs) return true;
      inFlight = write().finally(() => {
        inFlight = undefined;
      });
      return inFlight;
    }

    const timer = registration?.heartbeat === 'manual' ? undefined : setInterval(() => void beat(), intervalMs);
    // Never hold the process open for a heartbeat.
    timer?.unref?.();

    return {
      beat,
      get lost() {
        return lost;
      },
      stop() {
        if (timer) clearInterval(timer);
      },
    };
  }

  // --------------------------------------------------------------------------
  // Dispatch helpers
  // --------------------------------------------------------------------------

  /** Durable start delay for a ready step, from its registered `delayMs`. */
  function readyStepDelay(stepType: string): { startAfterSeconds: number } | undefined {
    const delayMs = registry.getRegistration(stepType)?.delayMs;
    if (!delayMs || delayMs <= 0) return undefined;
    return { startAfterSeconds: Math.ceil(delayMs / 1000) };
  }

  /** Whether a step type suspends on readiness instead of dispatching. */
  function isWaitStep(stepType: string): boolean {
    return registry.getRegistration(stepType)?.waitForEvent === true;
  }

  /**
   * The shortest liveness window in play: the engine-wide stuck threshold, or a
   * shorter `heartbeatTimeoutMs` if any registered step type asks for one. Used
   * to widen the sweeper's query so short-window steps are actually seen.
   */
  function narrowestLivenessWindowMs(defaultMs: number): number {
    let narrowest = defaultMs;
    for (const type of registry.types()) {
      const windowMs = registry.getRegistration(type)?.heartbeatTimeoutMs;
      if (windowMs && windowMs > 0 && windowMs < narrowest) narrowest = windowMs;
    }
    return narrowest;
  }

  /** A step type's join rule — how its dependencies gate it. */
  function joinOf(stepType: string): JoinRule {
    return registry.getRegistration(stepType)?.join ?? 'all';
  }

  /** Readiness over a workflow's steps, resolving join rules through the registry. */
  function readinessOf(steps: StepRecord[]) {
    return computeReadiness(steps, joinOf);
  }

  /**
   * Suspend a step and start its wait deadline (if the type declares one).
   * Shared by the `waitForEvent` and sub-workflow paths — both park a step until
   * something outside the engine settles it.
   */
  async function suspendStep(
    workflowId: WorkflowId,
    stepId: StepId,
    stepKey: string,
    stepType: string,
    st: WorkflowStore = store,
  ): Promise<void> {
    await st.markStepWaiting(stepId, nowIso());
    logger.info('Step is waiting for an event', { workflowId, stepId, stepKey });
    emit({ type: 'step.waiting', workflowId, stepId, stepKey, stepType });
    await scheduleWaitTimeout(workflowId, stepId, stepKey, stepType);
  }

  /**
   * Enqueue the job that settles a suspended step if nothing else does first.
   * It rides the same durable delay a sleep step uses, so the deadline survives
   * a restart. Firing early, or after the step already resumed, is harmless —
   * {@link timeoutStep} re-checks both.
   */
  async function scheduleWaitTimeout(
    workflowId: WorkflowId,
    stepId: StepId,
    stepKey: string,
    stepType: string,
    afterMs?: number,
  ): Promise<void> {
    const budgetMs = afterMs ?? registry.getRegistration(stepType)?.timeoutMs;
    if (!budgetMs || budgetMs <= 0) return;
    const result = await dispatcher.enqueueStep(
      { workflowId, stepId, stepKey, stepType, kind: 'timeout' },
      { startAfterSeconds: Math.ceil(budgetMs / 1000) },
    );
    if (!result.ok) {
      // The step stays suspended; without its deadline job it simply waits
      // indefinitely, so this is loud rather than fatal.
      logger.error('Failed to schedule wait timeout', new Error(result.error.message), { workflowId, stepId, stepKey });
    }
  }

  /**
   * Make a newly-ready step runnable: a `waitForEvent` step suspends (status `waiting`)
   * and awaits `resumeStep`; everything else is enqueued (with any durable delay).
   * Returns whether the step was enqueued.
   *
   * A wait step carrying a `when` guard is the exception — it is dispatched like
   * any other step so the guard gets a chance to skip it, and `executeStep`
   * suspends it from there once the guard passes.
   */
  async function dispatchReadyStep(
    workflowId: WorkflowId,
    stepId: StepId,
    stepKey: string,
    stepType: string,
    scope?: TransactionalScope,
  ): Promise<boolean> {
    const st = scope?.store ?? store;
    if (isWaitStep(stepType) && !registry.getRegistration(stepType)?.condition) {
      await suspendStep(workflowId, stepId, stepKey, stepType, st);
      return false;
    }
    const payload: DispatchStepPayload = { workflowId, stepId, stepKey, stepType };
    const delay = readyStepDelay(stepType);
    // Inside a transaction the job is written through the same handle as the
    // state change, so both commit or neither does.
    const result = scope
      ? await dispatcher.enqueueStepIn!(scope.handle, payload, delay)
      : await dispatcher.enqueueStep(payload, delay);
    if (!result.ok) {
      logger.error('Failed to enqueue step', new Error(result.error.message), { workflowId, stepKey });
      return false;
    }
    logger.info('Enqueued step', { workflowId, stepKey });
    return true;
  }

  // --------------------------------------------------------------------------
  // Validation (pure)
  // --------------------------------------------------------------------------

  function validateDefinition(definition: WorkflowDefinition): Result<void, FlowError> {
    const { steps } = definition;

    if (steps.length === 0) {
      return { ok: false, error: { key: 'invalid_workflow_definition', message: 'Workflow must have at least one step' } };
    }

    const keys = new Set<string>();
    for (const step of steps) {
      if (keys.has(step.key)) {
        return { ok: false, error: { key: 'invalid_workflow_definition', message: `Duplicate step key: ${step.key}` } };
      }
      keys.add(step.key);
    }

    for (const step of steps) {
      for (const dep of step.dependencies ?? []) {
        if (!keys.has(dep)) {
          return { ok: false, error: { key: 'invalid_workflow_definition', message: `Step '${step.key}' depends on unknown step '${dep}'` } };
        }
        if (dep === step.key) {
          return { ok: false, error: { key: 'invalid_workflow_definition', message: `Step '${step.key}' cannot depend on itself` } };
        }
      }
    }

    // Cycle detection via Kahn's algorithm
    const inDegree = new Map<string, number>();
    const adjacency = new Map<string, string[]>();
    for (const step of steps) {
      inDegree.set(step.key, (step.dependencies ?? []).length);
      adjacency.set(step.key, []);
    }
    for (const step of steps) {
      for (const dep of step.dependencies ?? []) {
        adjacency.get(dep)!.push(step.key);
      }
    }
    const queue: string[] = [];
    for (const [key, deg] of inDegree) {
      if (deg === 0) queue.push(key);
    }
    let visited = 0;
    while (queue.length > 0) {
      const current = queue.shift()!;
      visited++;
      for (const neighbor of adjacency.get(current) ?? []) {
        const newDeg = inDegree.get(neighbor)! - 1;
        inDegree.set(neighbor, newDeg);
        if (newDeg === 0) queue.push(neighbor);
      }
    }
    if (visited !== steps.length) {
      return { ok: false, error: { key: 'invalid_workflow_definition', message: 'Workflow contains circular dependencies' } };
    }

    for (const step of steps) {
      if (!registry.has(step.type)) {
        return { ok: false, error: { key: 'step_handler_not_found', message: `No handler registered for step type '${step.type}'`, stepType: step.type } };
      }
    }

    return { ok: true, value: undefined };
  }

  // --------------------------------------------------------------------------
  // Start
  // --------------------------------------------------------------------------

  async function startWorkflow(
    definition: WorkflowDefinition,
    input: Record<string, unknown>,
    options?: StartOptions,
  ): Promise<Result<WorkflowCreatedResult, FlowError>> {
    const validation = validateDefinition(definition);
    if (!validation.ok) return validation;

    let metadata = options?.metadata;
    if (hooks.onBeforeStart) {
      const guard = await hooks.onBeforeStart({ definition, input, options });
      if (!guard.ok) return guard;
      if (guard.value.metadata) {
        metadata = { ...(metadata ?? {}), ...guard.value.metadata };
      }
    }

    const { steps } = definition;

    const { created, enqueuedStepKeys } = await withDispatchScope(async (scope) => {
      const st = scope?.store ?? store;
      const created = await st.createWorkflow({
      type: definition.type,
      input,
      entityRef: options?.entityRef,
      metadata,
      idempotencyKey: options?.idempotencyKey,
      parentWorkflowId: options?.parentWorkflowId,
      parentStepId: options?.parentStepId,
      startedAt: nowIso(),
      deadlineAt:
        options?.timeoutMs && options.timeoutMs > 0
          ? new Date(now().getTime() + options.timeoutMs).toISOString()
          : undefined,
      steps: steps.map((s) => ({
        key: s.key,
        type: s.type,
        dependencies: s.dependencies ?? [],
        input: s.input ?? null,
      })),
      });
      if (created.alreadyExisted) return { created, enqueuedStepKeys: [] as string[] };

      // Enqueue dependency-free roots (run immediately / in parallel). Inside the
      // same transaction as the insert when the adapters allow it, so a queue
      // failure rolls the workflow back rather than stranding it with no jobs.
      const readySteps = steps.filter((s) => !s.dependencies || s.dependencies.length === 0);
      const keys: string[] = [];
      for (const readyStep of readySteps) {
        const dbStep = created.steps.find((s) => s.key === readyStep.key);
        if (!dbStep) continue;
        const enqueued = await dispatchReadyStep(created.workflowId, dbStep.id, readyStep.key, readyStep.type, scope);
        if (enqueued) keys.push(readyStep.key);
      }
      return { created, enqueuedStepKeys: keys };
    });

    const workflowId = created.workflowId;

    // Idempotency hit: the workflow already exists — return it without re-enqueuing.
    if (created.alreadyExisted) {
      logger.info('Idempotent start: returning existing workflow', { workflowId, idempotencyKey: options?.idempotencyKey });
      return { ok: true, value: { workflowId, totalSteps: created.steps.length, enqueuedSteps: [] } };
    }

    // After commit: an observer should never see a workflow that rolled back.
    emit({ type: 'workflow.started', workflowId, workflowType: definition.type });

    logger.info('Workflow started', { workflowId, type: definition.type, totalSteps: steps.length, enqueuedSteps: enqueuedStepKeys });

    return { ok: true, value: { workflowId, totalSteps: steps.length, enqueuedSteps: enqueuedStepKeys } };
  }

  /**
   * Route one dispatched job to what it asks for. Point your worker at this
   * rather than `executeStep` — a queue carries both step runs and wait
   * deadlines, and only the payload's `kind` tells them apart.
   */
  function handleStepJob(payload: DispatchStepPayload): Promise<Result<void, FlowError>> {
    return payload.kind === 'timeout'
      ? timeoutStep(payload.workflowId, payload.stepId)
      : executeStep(payload.workflowId, payload.stepId);
  }

  /** Type-safe start: duck-types on `{ inputSchema, definition }`. */
  function start<TInput extends Record<string, unknown>>(
    workflow: { inputSchema: { parse(v: unknown): TInput }; definition: WorkflowDefinition },
    input: TInput,
    options?: StartOptions,
  ): Promise<Result<WorkflowCreatedResult, FlowError>> {
    return startWorkflow(workflow.definition, workflow.inputSchema.parse(input), options);
  }

  // --------------------------------------------------------------------------
  // Execute
  // --------------------------------------------------------------------------

  async function executeStep(workflowId: WorkflowId, stepId: StepId): Promise<Result<void, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };

    if (workflow.status === 'cancelled' || workflow.status === 'failed') {
      logger.info('Skipping step for non-active workflow', { workflowId, stepId, status: workflow.status });
      return { ok: true, value: undefined };
    }

    // The run's own deadline, enforced at the last moment before work starts.
    if (await expireIfPastDeadline(workflow)) {
      logger.info('Skipping step for expired workflow', { workflowId, stepId });
      return { ok: true, value: undefined };
    }

    const step = await store.getStep(stepId);
    if (!step || step.workflowId !== workflowId) {
      return { ok: false, error: { key: 'workflow_not_found', message: `Step ${stepId} not found` } };
    }

    if (step.status !== 'pending') {
      // A redelivered job for a step that already COMPLETED is the one signal that
      // the advance which should have followed it may never have happened: the
      // engine commits the completion and enqueues the newly-ready steps as
      // separate operations, so a crash in that window leaves the dependents
      // `pending` with no job behind them — invisible to the stuck-step sweeper,
      // which only looks at `running`. Re-drive readiness rather than no-op.
      //
      // Safe to repeat: dispatching an already-queued step is harmless because
      // claiming it is atomic, so the duplicate delivery loses and does nothing.
      const workflowLive = workflow.status === 'running' || workflow.status === 'pending';
      if (step.status === 'completed' && workflowLive) {
        logger.info('Re-driving advance for a redelivered completed step', { workflowId, stepId, stepKey: step.key });
        if (step.parentStepId != null) await advanceAfterChildCompleted(workflowId, step);
        else await advanceAfterStepCompleted(workflowId);
        return { ok: true, value: undefined };
      }
      logger.info('Skipping already-processed step', { workflowId, stepId, stepKey: step.key, status: step.status });
      return { ok: true, value: undefined };
    }

    // Admission gate: if not admitted, defer by re-enqueueing with the gate's
    // delay. The step stays `pending` and no attempt is consumed.
    let releaseSlot: (() => void | Promise<void>) | undefined;
    if (gate) {
      const decision = await gate.acquire({ partitionKey, workflowId, stepId, stepKey: step.key, stepType: step.type });
      if (!decision.admitted) {
        logger.info('Step deferred by gate', { workflowId, stepId, stepKey: step.key, retryAfterSeconds: decision.retryAfterSeconds });
        const re = await dispatcher.enqueueStep(
          { workflowId, stepId, stepKey: step.key, stepType: step.type },
          { startAfterSeconds: decision.retryAfterSeconds },
        );
        if (!re.ok) logger.error('Failed to re-enqueue gated step', new Error(re.error.message), { workflowId, stepId });
        return { ok: true, value: undefined };
      }
      releaseSlot = decision.release;
    }

    const startMs = now().getTime();
    try {
      // Atomic claim. The `pending` read above is only a cheap pre-filter — this is
      // the authoritative check, because an at-least-once dispatcher may hand the
      // same job to two workers concurrently and both would pass the read. The
      // loser bails here (releasing its gate slot via the outer `finally`) instead
      // of running the handler a second time.
      const claimed = await store.markStepRunning(stepId, nowIso());
      if (!claimed) {
        logger.info('Lost the step claim to a concurrent worker', { workflowId, stepId, stepKey: step.key });
        return { ok: true, value: undefined };
      }

      // Resolve dependency outputs
      const dependencyOutputs: Record<string, unknown> = {};
      const stepDeps = step.dependencies ?? [];
      if (stepDeps.length > 0) {
        // A `join: 'any'` step runs on the branch that completed, so the arms
        // that were skipped are expected here — they just contribute no output.
        const tolerateSkipped = joinOf(step.type) === 'any';
        const allSteps = await store.listSteps(workflowId);
        for (const depKey of stepDeps) {
          const depStep = allSteps.find((s) => s.key === depKey);
          if (depStep?.status === 'completed') {
            dependencyOutputs[depKey] = depStep.output ?? undefined;
            continue;
          }
          if (tolerateSkipped && depStep?.status === 'skipped') continue;
          logger.error('Step dependency not completed', new Error(`Dependency '${depKey}' is ${depStep?.status ?? 'missing'}`), { workflowId, stepId });
          return { ok: false, error: { key: 'step_error', message: `Dependency '${depKey}' is not completed` } };
        }
      }

      const registration = registry.getRegistration(step.type);
      if (!registration) {
        await markStepFailed(stepId, workflowId, `No handler for step type '${step.type}'`);
        return { ok: false, error: { key: 'step_handler_not_found', message: `No handler for '${step.type}'`, stepType: step.type } };
      }
      const { handler, retry, timeoutMs } = registration;
      const maxAttempts = retry?.maxAttempts ?? 1;
      // markStepRunning already bumped the persisted counter; this run is attempt N.
      const attemptNo = step.attempts + 1;

      emit({ type: 'step.started', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo });
      const span = tracer.startSpan('flow.step', {
        'flow.workflow_id': workflowId,
        'flow.workflow_type': workflow.type,
        'flow.step_key': step.key,
        'flow.step_type': step.type,
        'flow.attempt': attemptNo,
        'flow.partition': partitionKey,
      });

      let heartbeat: Heartbeat = inertHeartbeat;
      try {
        const context = hooks.buildStepContext
          ? await hooks.buildStepContext({ workflowId, stepId, stepKey: step.key, partitionKey, workflow, step })
          : (undefined as TContext);

        const abort = new AbortController();
        heartbeat = startHeartbeat(workflowId, step, abort);
        const ctx: StepExecutionContext<TContext> = {
          workflowId,
          stepId,
          stepKey: step.key,
          partitionKey,
          workflowInput: workflow.input ?? {},
          stepInput: step.input ?? {},
          dependencyOutputs,
          signal: abort.signal,
          heartbeat: heartbeat.beat,
          context,
        };

        // Guard: `when` decides whether this step runs at all. Evaluated here —
        // after the claim, with dependency outputs resolved — so it sees exactly
        // what the handler would, and so a guard that throws is classified (and
        // retried) like a failing handler instead of silently pruning a branch.
        const guard = registration.condition ? await registration.condition(ctx) : undefined;

        if (guard?.ok && !guard.value) {
          await store.skipStep(stepId, CONDITION_NOT_MET);
          logger.info('Step skipped: condition not met', { workflowId, stepId, stepKey: step.key });
          emit({
            type: 'step.skipped',
            workflowId,
            workflowType: workflow.type,
            stepId,
            stepKey: step.key,
            stepType: step.type,
            attempt: attemptNo,
            durationMs: now().getTime() - startMs,
          });
          // Nothing completed, so nothing dispatched itself — and a join
          // downstream may be ready *because* of this skip.
          await settle(workflowId, { skippedKeys: [step.key] });
          return { ok: true, value: undefined };
        }

        // The guard passed on a suspending step — park it now. `resumeStep`, or
        // the wait deadline, settles it from here. (A wait step with no guard
        // never gets dispatched at all; it suspends at readiness.)
        if (guard?.ok && registration.waitForEvent) {
          await suspendStep(workflowId, stepId, step.key, step.type);
          return { ok: true, value: undefined };
        }

        const handlerResult: Result<Record<string, unknown>, StepError> =
          guard && !guard.ok ? guard : await runWithTimeout(handler, ctx, timeoutMs, abort);

        // A beat came back `false` while the handler was running: this step now
        // belongs to someone else (the sweeper re-queued it) or to nobody (the
        // run was cancelled or expired). Writing an outcome here would stamp
        // over the new owner's work, so this invocation goes quietly.
        if (heartbeat.lost) {
          logger.warn('Discarding the outcome of a superseded step', { workflowId, stepId, stepKey: step.key, attempt: attemptNo });
          return { ok: true, value: undefined };
        }

        if (handlerResult.ok) {
          // Map parent: the handler returned the item list — spawn children, don't complete.
          if (registration.map) {
            emit({ type: 'step.mapping', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo });
            await spawnMapChildren(workflowId, step, registration.childType ?? `${step.type}__item`, handlerResult.value);
            return { ok: true, value: undefined };
          }
          // Sub-workflow parent: the handler returned the child input — start it, then suspend.
          if (registration.subWorkflowDefinition) {
            await startSubWorkflow(workflowId, step, registration.subWorkflowDefinition, handlerResult.value);
            return { ok: true, value: undefined };
          }
          if (hooks.onAfterStep) {
            await hooks.onAfterStep({ workflowId, stepId, partitionKey, workflow, step, output: handlerResult.value, context });
          }
          emit({ type: 'step.completed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs });
          if (step.parentStepId != null) {
            await onChildCompleted(workflowId, step, handlerResult.value);
          } else {
            await onStepCompleted(workflowId, stepId, handlerResult.value);
          }
          return { ok: true, value: undefined };
        }

        // Retry transient failures within the attempt budget (re-enqueue with backoff).
        // A configured `defaultRetryable` replaces the classifier's guess, but never a
        // decision made explicitly (a marked error, a step predicate, an engine timeout).
        const guessed = handlerResult.error.retryableFrom === 'heuristic';
        const shouldRetry =
          guessed && defaultRetryable !== undefined ? defaultRetryable : handlerResult.error.retryable;
        if (shouldRetry && attemptNo < maxAttempts) {
          const delaySeconds = backoffDelaySeconds(retry, attemptNo);
          logger.info('Retrying step', { workflowId, stepId, stepKey: step.key, attempt: attemptNo, maxAttempts, delaySeconds, reason: handlerResult.error.message });
          emit({ type: 'step.retrying', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs, error: handlerResult.error.message });
          await store.markStepPending(stepId);
          const re = await dispatcher.enqueueStep(
            { workflowId, stepId, stepKey: step.key, stepType: step.type },
            { startAfterSeconds: delaySeconds },
          );
          if (!re.ok) {
            await failStepTerminal(workflowId, step, `${handlerResult.error.message} (retry enqueue failed: ${re.error.message})`);
          }
          return { ok: true, value: undefined };
        }

        span.recordError(new Error(handlerResult.error.message));
        emit({ type: 'step.failed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs, error: handlerResult.error.message });
        await failStepTerminal(workflowId, step, handlerResult.error.message);
        return { ok: true, value: undefined };
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown step execution error';
        logger.error('Step execution threw', error instanceof Error ? error : new Error(message), { workflowId, stepId });
        span.recordError(error instanceof Error ? error : new Error(message));
        emit({ type: 'step.failed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs, error: message });
        await failStepTerminal(workflowId, step, message);
        // Re-throw so the dispatcher can retry
        throw error;
      } finally {
        heartbeat.stop();
        span.end();
      }
    } finally {
      if (releaseSlot) await releaseSlot();
    }
  }

  /**
   * Deliver an external event to a `waiting` step: completes it with `payload`
   * as its output and advances the DAG. Idempotent — resuming a non-waiting step (e.g. a
   * re-delivered event) is a logged no-op. The host correlates the event to `stepKey`.
   */
  async function resumeStep(
    workflowId: WorkflowId,
    stepKey: string,
    payload: Record<string, unknown> = {},
  ): Promise<Result<void, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };

    if (workflow.status !== 'running' && workflow.status !== 'pending') {
      logger.info('Ignoring resume for non-active workflow', { workflowId, stepKey, status: workflow.status });
      return { ok: true, value: undefined };
    }

    const step = (await store.listSteps(workflowId)).find((s) => s.key === stepKey);
    if (!step) return { ok: false, error: { key: 'workflow_not_found', message: `Step '${stepKey}' not found in workflow ${workflowId}` } };

    if (step.status !== 'waiting') {
      logger.info('Ignoring resume for non-waiting step', { workflowId, stepKey, status: step.status });
      return { ok: true, value: undefined };
    }

    emit({ type: 'step.resumed', workflowId, stepId: step.id, stepKey, stepType: step.type });
    await onStepCompleted(workflowId, step.id, payload);
    logger.info('Resumed waiting step', { workflowId, stepKey });
    return { ok: true, value: undefined };
  }

  // --------------------------------------------------------------------------
  // Advancement
  // --------------------------------------------------------------------------

  async function onStepCompleted(workflowId: WorkflowId, stepId: StepId, output: Record<string, unknown>): Promise<void> {
    // The completion and the dispatches it unlocks go in one transaction when the
    // adapters support it — otherwise a crash between them strands the dependents.
    const snapshot = await withDispatchScope(async (scope) => {
      await (scope?.store ?? store).completeStep({ workflowId, stepId, output, completedAt: nowIso() });
      return dispatchNewlyReady(workflowId, scope);
    });
    await settle(workflowId, { snapshot });
  }

  /**
   * The readiness half of a completion, without the write. Split out so a
   * redelivered job for an already-completed step can re-drive it — see the
   * recovery branch in {@link executeStep}. Idempotent: dispatching a step that
   * is already queued is harmless, because claiming it is atomic.
   */
  async function advanceAfterStepCompleted(workflowId: WorkflowId): Promise<void> {
    const snapshot = await withDispatchScope((scope) => dispatchNewlyReady(workflowId, scope));
    await settle(workflowId, { snapshot });
  }

  /**
   * Transactional half: read readiness and dispatch whatever just became ready.
   * Contains only store writes and enqueues — never user code.
   *
   * Returns the step rows it read. They were read *after* the write that
   * prompted this call, so {@link settle} can reuse them instead of paying for a
   * second read of the same rows on the hot path.
   */
  async function dispatchNewlyReady(workflowId: WorkflowId, scope?: TransactionalScope): Promise<StepRecord[]> {
    const st = scope?.store ?? store;
    const steps = await st.listSteps(workflowId);
    for (const readyStep of readinessOf(steps).ready) {
      await dispatchReadyStep(workflowId, readyStep.id, readyStep.key, readyStep.type, scope);
    }
    return steps;
  }

  interface SettleOptions {
    /**
     * Step rows already read after the transition that prompted this settle.
     * Saves a re-read; omit when the caller has no fresh view.
     */
    snapshot?: StepRecord[];
    /**
     * Keys of steps the caller just skipped without completing — a `when` guard
     * declining its step. A skip can only unblock a `join: 'any'` step that
     * depends on it, so naming them lets the dispatch pass stay narrow instead
     * of re-driving the whole frontier.
     */
    skippedKeys?: string[];
  }

  /**
   * The one thing that happens after *any* step transition: prune what can no
   * longer run, dispatch what became runnable as a result, and finish the
   * workflow once nothing is left moving.
   *
   * Completion, failure, conditional skip and operator retry all funnel through
   * here, so they cannot disagree about when a run is over — an earlier split
   * between a "completion path" and a "failure path" could strand a workflow in
   * `running` when a parallel branch failed while its sibling was in flight,
   * because each path saw the other's step as non-terminal and waited.
   *
   * Deliberately outside any transaction: the failure route runs saga
   * compensation, i.e. user handlers that may do network I/O.
   */
  async function settle(workflowId: WorkflowId, options?: SettleOptions): Promise<void> {
    let steps = options?.snapshot ?? (await store.listSteps(workflowId));

    const skippedKeys = [...(options?.skippedKeys ?? []), ...(await cascadeSkips(workflowId, steps))];
    if (skippedKeys.length > 0) {
      // A skip can unblock a `join: 'any'` step — but only one that depends on
      // something just skipped. Dispatching the whole ready frontier here would
      // re-enqueue siblings that are already sitting in the queue: harmless,
      // because claiming is atomic, but a duplicate job for every skip.
      //
      // Re-read first: the snapshot still shows the skipped rows as pending.
      steps = await store.listSteps(workflowId);
      const unblocked = readinessOf(steps).ready.filter((s) =>
        (s.dependencies ?? []).some((dep) => skippedKeys.includes(dep)),
      );
      for (const step of unblocked) {
        await dispatchReadyStep(workflowId, step.id, step.key, step.type, undefined);
      }
    }

    await terminateIfSettled(workflowId, steps);
  }

  /**
   * Skip every step that can no longer run — a dependency failed, or the branch
   * it sits on was not taken. Cascades transitively (a→b→c: failing `a` skips
   * `b`, then `c`), respecting each step's join rule so a `join: 'any'` step is
   * not pruned just because one arm was skipped.
   *
   * On an ordinary completion this decides nothing and writes nothing — the
   * cascade already ran when whatever blocked those steps settled — so it costs
   * a pure pass over rows the caller had already read.
   */
  async function cascadeSkips(workflowId: WorkflowId, steps: StepRecord[]): Promise<string[]> {
    const { skip } = readinessOf(steps);
    for (const { step, reason } of skip) {
      await store.skipStep(step.id, reason);
      emit({ type: 'step.skipped', workflowId, stepId: step.id, stepKey: step.key, stepType: step.type });
    }
    return skip.map(({ step }) => step.key);
  }

  /** Finish the workflow — completed or failed — once every step has settled. */
  async function terminateIfSettled(workflowId: WorkflowId, steps: StepRecord[]): Promise<void> {
    // Cheapest check first: on all but the last transition of a run this is
    // false, and the method costs no I/O at all.
    //
    // Map children count here even though they never drive readiness: a map
    // parent can fail while one of its items is still in flight, and finishing
    // the workflow out from under that item would lose its outcome.
    if (!steps.every((s) => isTerminalStepStatus(s.status))) return;

    const workflow = await store.getWorkflow(workflowId);
    // Already terminal (cancelled, expired, or finished by a concurrent settle).
    if (!workflow || (workflow.status !== 'running' && workflow.status !== 'pending')) return;

    const keyedSteps = steps.filter((s) => s.parentStepId == null);
    const firstFailed = steps.find((s) => s.status === 'failed');

    if (firstFailed) {
      const error = firstFailed.error ?? 'One or more steps failed';
      await store.finishWorkflow({ workflowId, status: 'failed', error, completedAt: nowIso() });
      logger.info('Workflow failed', { workflowId });
      emit({ type: 'workflow.failed', workflowId, error });
      // Undo completed steps' side effects in reverse order, then…
      await compensateWorkflow(workflowId);
      // …propagate failure to the parent step if this is a sub-workflow child.
      await bridgeSubWorkflow(workflowId);
      return;
    }

    const aggregatedOutput: Record<string, unknown> = {};
    for (const s of keyedSteps) {
      if (s.status === 'completed') aggregatedOutput[s.key] = s.output ?? null;
    }
    await store.finishWorkflow({ workflowId, status: 'completed', output: aggregatedOutput, completedAt: nowIso() });
    logger.info('Workflow completed', { workflowId });
    emit({ type: 'workflow.completed', workflowId });

    if (hooks.onWorkflowCompleted) {
      const finalWorkflow = await store.getWorkflow(workflowId);
      if (finalWorkflow) {
        // Fire-and-forget: never block / fail completion on bookkeeping
        Promise.resolve(hooks.onWorkflowCompleted({ workflowId, partitionKey, workflow: finalWorkflow })).catch((e) =>
          logger.error('onWorkflowCompleted hook failed', e instanceof Error ? e : new Error(String(e)), { workflowId }),
        );
      }
    }

    // If this workflow is a sub-workflow child, settle its parent step.
    await bridgeSubWorkflow(workflowId);
  }

  // --------------------------------------------------------------------------
  // Dynamic fan-out / map
  // --------------------------------------------------------------------------

  /** A map parent produced its item list — spawn one child per item and suspend it. */
  async function spawnMapChildren(
    workflowId: WorkflowId,
    parentStep: StepRecord,
    childType: string,
    itemsOutput: Record<string, unknown>,
  ): Promise<void> {
    const items = Array.isArray(itemsOutput.items) ? (itemsOutput.items as unknown[]) : [];
    if (items.length === 0) {
      // Nothing to fan out over — complete the map immediately with an empty result.
      await onStepCompleted(workflowId, parentStep.id, { items: [] });
      return;
    }
    const children = items.map((item, index) => ({
      key: `${parentStep.key}#${index}`,
      type: childType,
      input: { item, index },
    }));
    const created = await store.addChildSteps(workflowId, parentStep.id, children);
    await store.markStepMapping(parentStep.id);
    for (const child of created) {
      const result = await dispatcher.enqueueStep(
        { workflowId, stepId: child.id, stepKey: child.key, stepType: child.type },
        readyStepDelay(child.type),
      );
      if (!result.ok) logger.error('Failed to enqueue map child', new Error(result.error.message), { workflowId, stepKey: child.key });
    }
    logger.info('Map step spawned children', { workflowId, parentKey: parentStep.key, count: created.length });
  }

  /** A map child completed — complete the parent (with aggregated outputs) once all siblings finish. */
  async function onChildCompleted(workflowId: WorkflowId, childStep: StepRecord, output: Record<string, unknown>): Promise<void> {
    await store.completeStep({ workflowId, stepId: childStep.id, output, completedAt: nowIso() });
    await advanceAfterChildCompleted(workflowId, childStep);
  }

  /** The aggregation half of a map child's completion, without the write. */
  async function advanceAfterChildCompleted(workflowId: WorkflowId, childStep: StepRecord): Promise<void> {
    const parentId = childStep.parentStepId!;
    const children = await store.listChildSteps(parentId);

    if (children.some((c) => c.status === 'failed')) {
      // The map already failed (a sibling errored) — re-check so the failure finalizes
      // the workflow once the remaining in-flight children settle.
      await settle(workflowId);
      return;
    }
    if (children.every((c) => c.status === 'completed')) {
      const aggregated = children.map((c) => c.output ?? null);
      await onStepCompleted(workflowId, parentId, { items: aggregated });
    }
  }

  /** Fail a map parent and skip its still-pending children (a sibling item errored). */
  async function failMapParent(workflowId: WorkflowId, parentStepId: StepId, message: string): Promise<void> {
    const parent = await store.getStep(parentStepId);
    if (!parent || parent.status === 'failed' || parent.status === 'completed') return;
    await markStepFailed(parentStepId, workflowId, message);
    for (const child of await store.listChildSteps(parentStepId)) {
      if (child.status === 'pending') await store.skipStep(child.id, 'Skipped: sibling map item failed');
    }
  }

  /** Terminal failure of a step — fails the parent map too if the step is a map child. */
  async function failStepTerminal(workflowId: WorkflowId, step: StepRecord, message: string): Promise<void> {
    await markStepFailed(step.id, workflowId, message);
    if (step.parentStepId != null) {
      await failMapParent(workflowId, step.parentStepId, `Map item failed: ${message}`);
    }
    await settle(workflowId);
  }

  // --------------------------------------------------------------------------
  // Child / sub-workflows
  // --------------------------------------------------------------------------

  /** A sub-workflow step ran — start the child workflow and suspend the parent step. */
  async function startSubWorkflow(
    workflowId: WorkflowId,
    parentStep: StepRecord,
    childDefinition: WorkflowDefinition,
    childInput: Record<string, unknown>,
  ): Promise<void> {
    // Suspend first so the child's terminal bridge always finds the parent step `waiting`.
    await suspendStep(workflowId, parentStep.id, parentStep.key, parentStep.type);
    const started = await startWorkflow(childDefinition, childInput, {
      parentWorkflowId: workflowId,
      parentStepId: parentStep.id,
    });
    if (!started.ok) {
      logger.error('Failed to start sub-workflow', new Error(started.error.message), { workflowId, stepKey: parentStep.key });
      await failStepTerminal(workflowId, parentStep, `Sub-workflow start failed: ${started.error.message}`);
      return;
    }
    logger.info('Sub-workflow started', { workflowId, parentKey: parentStep.key, childWorkflowId: started.value.workflowId });
  }

  /**
   * A workflow reached a terminal state — if it's a sub-workflow child, settle its parent
   * step: complete it with the child's output, or fail it (and cascade) on failure/cancel.
   * Idempotent — a no-op unless the parent step is still `waiting`.
   */
  async function bridgeSubWorkflow(childWorkflowId: WorkflowId): Promise<void> {
    const child = await store.getWorkflow(childWorkflowId);
    if (!child || child.parentWorkflowId == null || child.parentStepId == null) return;

    const parentStep = await store.getStep(child.parentStepId);
    if (!parentStep || parentStep.status !== 'waiting') return;

    if (child.status === 'completed') {
      await onStepCompleted(child.parentWorkflowId, child.parentStepId, child.output ?? {});
    } else {
      await markStepFailed(child.parentStepId, child.parentWorkflowId, `Sub-workflow ${child.status}${child.error ? `: ${child.error}` : ''}`);
      await settle(child.parentWorkflowId);
    }
    logger.info('Bridged sub-workflow to parent step', { childWorkflowId, parentWorkflowId: child.parentWorkflowId, status: child.status });
  }

  // --------------------------------------------------------------------------
  // Compensation / saga
  // --------------------------------------------------------------------------

  /**
   * Run each completed step's `compensate` handler in reverse dependency order to undo side
   * effects after the workflow failed. Best-effort: one attempt per step, throws are logged and
   * surfaced on the step (status `compensated` with the error). Steps without a handler are left.
   */
  async function compensateWorkflow(workflowId: WorkflowId): Promise<void> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return;
    const steps = await store.listSteps(workflowId);
    const compensatable = steps.filter((s) => s.status === 'completed' && registry.getRegistration(s.type)?.compensate);
    if (compensatable.length === 0) return;

    const byKey = new Map(steps.map((s) => [s.key, s] as const));
    for (const step of reverseTopologicalOrder(compensatable)) {
      const compensate = registry.getRegistration(step.type)?.compensate;
      if (!compensate) continue;
      await store.markStepCompensating(step.id);
      emit({ type: 'step.compensating', workflowId, workflowType: workflow.type, stepId: step.id, stepKey: step.key, stepType: step.type });
      try {
        const dependencyOutputs: Record<string, unknown> = {};
        for (const depKey of step.dependencies ?? []) dependencyOutputs[depKey] = byKey.get(depKey)?.output ?? undefined;
        const context = hooks.buildStepContext
          ? await hooks.buildStepContext({ workflowId, stepId: step.id, stepKey: step.key, partitionKey, workflow, step })
          : (undefined as TContext);
        await compensate({
          workflowId,
          stepId: step.id,
          stepKey: step.key,
          partitionKey,
          workflowInput: workflow.input ?? {},
          stepInput: step.input ?? {},
          dependencyOutputs,
          heartbeat: inertHeartbeat.beat,
          context,
          output: step.output ?? {},
        });
        await store.markStepCompensated(step.id);
        logger.info('Compensated step', { workflowId, stepKey: step.key });
        emit({ type: 'step.compensated', workflowId, workflowType: workflow.type, stepId: step.id, stepKey: step.key, stepType: step.type });
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown compensation error';
        logger.error('Compensation failed', error instanceof Error ? error : new Error(message), { workflowId, stepKey: step.key });
        await store.markStepCompensated(step.id, `Compensation failed: ${message}`);
        emit({ type: 'step.compensated', workflowId, workflowType: workflow.type, stepId: step.id, stepKey: step.key, stepType: step.type, error: message });
      }
    }
  }

  async function markStepFailed(stepId: StepId, workflowId: WorkflowId, errorMessage: string): Promise<void> {
    await store.failStep({ workflowId, stepId, error: errorMessage, completedAt: nowIso() });
  }

  // --------------------------------------------------------------------------
  // Deadlines
  // --------------------------------------------------------------------------

  /**
   * A suspended step's wait budget elapsed. Settles it per the type's
   * `onTimeout` policy — unless something already did, which is the common case
   * (the event arrived, the workflow was cancelled) and a plain no-op.
   *
   * Delivered by the dispatcher as a `kind: 'timeout'` job, so it is subject to
   * the same at-least-once redelivery as everything else: the elapsed-time
   * re-check below makes an early or duplicate delivery harmless.
   */
  async function timeoutStep(workflowId: WorkflowId, stepId: StepId): Promise<Result<void, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };
    if (workflow.status !== 'running' && workflow.status !== 'pending') return { ok: true, value: undefined };

    const step = await store.getStep(stepId);
    if (!step || step.workflowId !== workflowId) {
      return { ok: false, error: { key: 'workflow_not_found', message: `Step ${stepId} not found` } };
    }
    if (step.status !== 'waiting') {
      logger.info('Ignoring wait timeout for a step that already settled', { workflowId, stepId, stepKey: step.key, status: step.status });
      return { ok: true, value: undefined };
    }

    const budgetMs = registry.getRegistration(step.type)?.timeoutMs;
    if (!budgetMs || budgetMs <= 0) return { ok: true, value: undefined };

    // The suspension may be younger than the job that just fired — a redelivery,
    // or a step that was reset and suspended again after the deadline was
    // scheduled. Re-arm for what is actually left rather than firing early.
    const waitingSince = step.startedAt ? Date.parse(step.startedAt) : Number.NaN;
    if (!Number.isNaN(waitingSince)) {
      const remainingMs = waitingSince + budgetMs - now().getTime();
      if (remainingMs > 0) {
        logger.info('Wait timeout fired early — re-arming', { workflowId, stepId, stepKey: step.key, remainingMs });
        await scheduleWaitTimeout(workflowId, stepId, step.key, step.type, remainingMs);
        return { ok: true, value: undefined };
      }
    }

    const policy = registry.getRegistration(step.type)?.onTimeout ?? 'fail';
    const message = `Step waited longer than ${budgetMs}ms for an event`;
    logger.info('Wait timed out', { workflowId, stepId, stepKey: step.key, policy: policy === 'fail' ? 'fail' : 'continue' });
    emit({ type: 'step.timedOut', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, error: message });

    if (policy === 'fail') {
      emit({ type: 'step.failed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, error: message });
      await failStepTerminal(workflowId, step, message);
      return { ok: true, value: undefined };
    }

    // `{ output }`: the wait ends in a defined answer and the DAG carries on —
    // a downstream `when` guard is what turns that into an escalation branch.
    emit({ type: 'step.completed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type });
    await onStepCompleted(workflowId, stepId, policy.output);
    return { ok: true, value: undefined };
  }

  /**
   * Fail a workflow that has outlived its `StartOptions.timeoutMs`. Returns
   * whether it did, so callers can bail out of whatever they were about to do.
   *
   * A step already `running` is not interrupted — the engine cannot reach into
   * another worker's process — but its completion will find the workflow
   * terminal and go no further.
   */
  async function expireIfPastDeadline(workflow: WorkflowRecord): Promise<boolean> {
    if (!workflow.deadlineAt) return false;
    if (workflow.status !== 'running' && workflow.status !== 'pending') return false;
    if (now().getTime() < Date.parse(workflow.deadlineAt)) return false;

    const workflowId = workflow.id;
    const message = `Workflow exceeded its deadline of ${workflow.deadlineAt}`;
    await store.skipPendingSteps(workflowId, message);
    await store.finishWorkflow({ workflowId, status: 'failed', error: message, completedAt: nowIso() });
    logger.warn('Workflow deadline exceeded', { workflowId, deadlineAt: workflow.deadlineAt });
    emit({ type: 'workflow.failed', workflowId, error: message });
    await compensateWorkflow(workflowId);
    await bridgeSubWorkflow(workflowId);
    return true;
  }

  // --------------------------------------------------------------------------
  // Cancel / status / recovery
  // --------------------------------------------------------------------------

  async function cancelWorkflow(workflowId: WorkflowId): Promise<Result<void, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };
    if (workflow.status !== 'pending' && workflow.status !== 'running') return { ok: true, value: undefined };

    await store.skipPendingSteps(workflowId, 'Workflow cancelled');
    await store.finishWorkflow({ workflowId, status: 'cancelled', completedAt: nowIso() });
    logger.info('Workflow cancelled', { workflowId });
    emit({ type: 'workflow.cancelled', workflowId });
    // A cancelled sub-workflow child fails its parent step.
    await bridgeSubWorkflow(workflowId);
    return { ok: true, value: undefined };
  }

  async function getWorkflowStatus(workflowId: WorkflowId): Promise<Result<WorkflowWithSteps, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };
    const steps = await store.listSteps(workflowId);
    return { ok: true, value: { ...workflow, steps } };
  }

  async function listWorkflows(filters?: {
    status?: WorkflowStatus;
    type?: string;
    entityRef?: string;
    limit?: number;
  }): Promise<Result<WorkflowWithSteps[], FlowError>> {
    const workflows = await store.listWorkflows({ ...filters, limit: filters?.limit ?? 50 });
    return { ok: true, value: workflows };
  }

  async function handleStepExhausted(workflowId: WorkflowId, stepId: StepId, errorMessage: string): Promise<void> {
    const step = await store.getStep(stepId);
    if (!step || step.workflowId !== workflowId || ['completed', 'failed', 'skipped'].includes(step.status)) return;
    await markStepFailed(stepId, workflowId, errorMessage);
    await settle(workflowId);
  }

  /**
   * Sweep for runs that stopped moving on their own, and is the only thing that
   * notices two of them:
   *
   * - a step stuck in `running` past the dispatcher expiry, because the worker
   *   died between claiming it and finishing it. Under the default
   *   `onStuckStep: 'retry'` it goes back on the queue while its attempt budget
   *   lasts, so a crashed pod costs an attempt rather than the whole run.
   * - a workflow past its deadline, wherever it was — queued, suspended on an
   *   event that never came, or genuinely working.
   *
   * Run it on a schedule (a cron job, or the pg-boss scheduler).
   */
  async function recoverStuckWorkflows(): Promise<RecoverySweepResult> {
    const defaultThresholdMs = (stepExpirySeconds + stuckStepBufferSeconds) * 1000;
    // Candidates are fetched against the *widest* window any step type could
    // need, because a type with a 30s heartbeat window must not wait for the
    // 15-minute default before it is even looked at. Each candidate is then held
    // to its own threshold below.
    const cutoff = new Date(now().getTime() - narrowestLivenessWindowMs(defaultThresholdMs)).toISOString();

    const runningWorkflows = await store.listRunningWorkflows();
    const empty: RecoverySweepResult = { retriedSteps: 0, recoveredSteps: 0, recoveredWorkflows: 0, expiredWorkflows: 0 };
    if (runningWorkflows.length === 0) return empty;

    let retriedSteps = 0;
    let recoveredSteps = 0;
    let expiredWorkflows = 0;
    const affectedWorkflowIds = new Set<WorkflowId>();

    for (const workflow of runningWorkflows) {
      // Deadline first: a run that is over shouldn't have its steps re-queued.
      if (await expireIfPastDeadline(workflow)) {
        expiredWorkflows++;
        continue;
      }

      const stuckSteps = await store.findStuckSteps(workflow.id, cutoff);
      for (const step of stuckSteps) {
        const registration = registry.getRegistration(step.type);
        const heartbeatMs = registration?.heartbeatTimeoutMs;
        const thresholdMs = heartbeatMs && heartbeatMs > 0 ? heartbeatMs : defaultThresholdMs;

        // A step that reports in is judged on when it last spoke; one that never
        // has, on when it started. Anything still inside its own window is alive.
        const lastSeen = Date.parse(step.heartbeatAt ?? step.startedAt ?? '');
        if (!Number.isNaN(lastSeen) && now().getTime() - lastSeen < thresholdMs) continue;

        const maxAttempts = registration?.retry?.maxAttempts ?? 1;
        const reason = heartbeatMs
          ? `Step went silent for more than ${Math.round(thresholdMs / 1000)}s (worker likely crashed)`
          : `Step exceeded ${Math.round(thresholdMs / 1000)}s without completion (worker likely crashed)`;

        if (onStuckStep === 'retry' && step.attempts < maxAttempts) {
          logger.warn('Re-queueing stuck workflow step', { workflowId: workflow.id, stepId: step.id, stepKey: step.key, attempt: step.attempts, maxAttempts, startedAt: step.startedAt });
          await store.markStepPending(step.id);
          const enqueued = await dispatcher.enqueueStep(
            { workflowId: workflow.id, stepId: step.id, stepKey: step.key, stepType: step.type },
            { startAfterSeconds: backoffDelaySeconds(registration?.retry, step.attempts) },
          );
          if (enqueued.ok) {
            emit({ type: 'step.retrying', workflowId: workflow.id, workflowType: workflow.type, stepId: step.id, stepKey: step.key, stepType: step.type, attempt: step.attempts, error: reason });
            retriedSteps++;
            continue;
          }
          // Couldn't re-queue: fall through and fail it rather than leave the
          // step `pending` with nothing behind it.
          logger.error('Failed to re-queue stuck step', new Error(enqueued.error.message), { workflowId: workflow.id, stepId: step.id });
        }

        logger.warn('Failing stuck workflow step', { workflowId: workflow.id, stepId: step.id, stepKey: step.key, attempt: step.attempts, maxAttempts, startedAt: step.startedAt });
        await markStepFailed(step.id, workflow.id, reason);
        recoveredSteps++;
        affectedWorkflowIds.add(workflow.id);
      }
    }

    for (const workflowId of affectedWorkflowIds) {
      await settle(workflowId);
    }

    return { retriedSteps, recoveredSteps, recoveredWorkflows: affectedWorkflowIds.size, expiredWorkflows };
  }

  // --------------------------------------------------------------------------
  // Retry a failed run
  // --------------------------------------------------------------------------

  /**
   * Put a `failed` workflow back in flight from where it stopped: every step
   * that failed, was skipped in the fallout, or had its work compensated away
   * goes back to `pending` with a **fresh attempt budget**, while everything
   * that completed keeps its output and is not run again.
   *
   * This is the operator's answer to a bad deploy or a downstream outage that
   * burned through the retry budget — the alternative being a brand-new run
   * that repeats every side effect the first one already committed.
   *
   * Two things it deliberately does *not* do:
   *
   * - **Re-run completed work.** If a completed step's effects were undone by
   *   saga compensation, that step is `compensated`, not `completed`, so it is
   *   reset and runs again — which is exactly right.
   * - **Resurrect a sub-workflow child.** Retrying a child cannot un-fail the
   *   parent step waiting on it, which would leave the pair inconsistent; retry
   *   the parent instead, and it starts a fresh child.
   */
  async function retryWorkflow(workflowId: WorkflowId): Promise<Result<RetryWorkflowResult, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };

    if (workflow.status !== 'failed') {
      return {
        ok: false,
        error: {
          key: 'workflow_not_retryable',
          status: workflow.status,
          message: `Workflow ${workflowId} is '${workflow.status}'; only a failed workflow can be retried`,
        },
      };
    }
    if (workflow.parentWorkflowId != null) {
      return {
        ok: false,
        error: {
          key: 'workflow_not_retryable',
          status: workflow.status,
          message: `Workflow ${workflowId} is a sub-workflow of ${workflow.parentWorkflowId}; retry the parent instead`,
        },
      };
    }

    const steps = await store.listSteps(workflowId);
    const toReset = steps.filter(
      (s) => s.parentStepId == null && (s.status === 'failed' || s.status === 'skipped' || s.status === 'compensated'),
    );
    if (toReset.length === 0) {
      return {
        ok: false,
        error: { key: 'workflow_not_retryable', status: workflow.status, message: `Workflow ${workflowId} has no steps to retry` },
      };
    }

    for (const step of toReset) {
      // A map parent fans out afresh, so last attempt's children must not
      // linger — they would be counted, and aggregated, twice.
      if (registry.getRegistration(step.type)?.map) await store.deleteChildSteps(step.id);
      await store.resetStep(step.id);
    }

    const after = await store.listSteps(workflowId);
    await store.reopenWorkflow({
      workflowId,
      totalSteps: after.length,
      completedSteps: after.filter((s) => s.status === 'completed').length,
      failedSteps: 0,
    });

    const resetSteps = toReset.map((s) => s.key);
    logger.info('Retrying failed workflow', { workflowId, resetSteps });
    emit({ type: 'workflow.retried', workflowId, workflowType: workflow.type });

    // Nothing completed to trigger a dispatch, so drive the frontier directly;
    // settle is then only there to prune and to finish the run if it is already over.
    await settle(workflowId, { snapshot: await dispatchNewlyReady(workflowId) });

    return { ok: true, value: { workflowId, resetSteps } };
  }

  return {
    validateDefinition,
    startWorkflow,
    start,
    executeStep,
    timeoutStep,
    handleStepJob,
    resumeStep,
    handleStepExhausted,
    recoverStuckWorkflows,
    retryWorkflow,
    cancelWorkflow,
    getWorkflowStatus,
    listWorkflows,
  };
}

export type WorkflowEngine<TContext = unknown> = ReturnType<typeof createWorkflowEngine<TContext>>;

// Re-export for convenience: a record shape used by status helpers
export type { WorkflowRecord, StepRecord };
