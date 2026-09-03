import type { Result, FlowErrorShape } from './result';
import type { WorkflowId, StepId } from './types';

/**
 * What a step job is for. A queue carries two kinds of work for a step, and
 * they differ only in this field:
 *
 * - `'execute'` (default) — run the step. Every job before wait deadlines
 *   existed was this, which is why it is also what a payload without a `kind`
 *   means: an in-flight job enqueued by an older version still routes correctly.
 * - `'timeout'` — the step's wait budget has elapsed; settle it if it is still
 *   suspended. Delivered by the same durable delay that powers a sleep step.
 */
export type DispatchKind = 'execute' | 'timeout';

/** Payload handed to the dispatcher to schedule a single step for execution. */
export interface DispatchStepPayload {
  workflowId: WorkflowId;
  stepId: StepId;
  stepKey: string;
  stepType: string;
  /** Defaults to `'execute'` when absent. */
  kind?: DispatchKind;
}

export interface EnqueueOptions {
  /**
   * Delay before the step becomes eligible to run, in seconds. Durable — survives
   * restarts. Used for retry backoff (and, later, durable sleep). Default 0.
   */
  startAfterSeconds?: number;
}

/**
 * Schedules step execution. The default adapter is a pg-boss queue, but any
 * durable-job mechanism works: the only contract is "eventually call
 * `engine.executeStep(workflowId, stepId)` for this payload, with retries".
 */
export interface Dispatcher {
  enqueueStep(payload: DispatchStepPayload, options?: EnqueueOptions): Promise<Result<void, FlowErrorShape>>;

  /**
   * **Optional capability.** Whatever the dispatcher must do *outside* a store
   * transaction before it can enqueue inside one — queue DDL, warming a queue
   * cache. The engine calls it once, before its first transactional dispatch,
   * and never while a transaction is open.
   *
   * Why it exists: a dispatcher that lazily creates its queue on first enqueue
   * does so on its own connection. On a multi-connection database that merely
   * costs a second connection; on a single-connection database (an embedded
   * PGlite) it deadlocks — the transaction holds the only connection and waits
   * for a query that waits for the transaction. `prepare` moves that work to
   * where no transaction is open.
   */
  prepare?(): Promise<Result<void, FlowErrorShape>>;

  /**
   * **Optional capability.** Enqueue inside the caller's store transaction, so
   * the job and the state change that produced it commit together.
   *
   * `handle` comes from `WorkflowStore.runInTransaction` and is opaque to the
   * engine — the store and dispatcher agree on its meaning. Implement this only
   * when the queue lives in the same database as the store (pg-boss on the same
   * Postgres does; SQS or a separate Redis cannot). Without it the engine writes
   * state and then enqueues, which is at-least-once with a crash window.
   */
  enqueueStepIn?(
    handle: unknown,
    payload: DispatchStepPayload,
    options?: EnqueueOptions,
  ): Promise<Result<void, FlowErrorShape>>;
}
