import type { PgBoss, Job, WorkOptions, JobResult } from 'pg-boss';
import type { Dispatcher, DispatchStepPayload, EnqueueOptions, Result, FlowErrorShape, Logger } from '../core';
import { WIRE_STEP_PAYLOAD_SCHEMA, DEFAULT_STEP_QUEUE_CONFIG, type WireStepPayload, type StepQueueConfig } from './payload';

function dlqName(queueName: string): string {
  return `${queueName}-dlq`;
}

/**
 * Create the step queue and its dead-letter queue if they do not exist. Call
 * once at startup; the dispatcher also runs it from `prepare` and lazily on a
 * plain first enqueue.
 *
 * Only what is missing is created: pg-boss's `createQueue` evicts the queue's
 * entry from its per-instance cache, and a `send` that misses that cache reads
 * the queue on pg-boss's *own* connection — inside a store transaction on a
 * single-connection database that is a deadlock. An existing queue is left
 * alone, cache entry and all.
 */
export async function ensureStepQueue(boss: PgBoss, queueName: string, config: StepQueueConfig = {}): Promise<void> {
  const cfg = { ...DEFAULT_STEP_QUEUE_CONFIG, ...config };
  const dlq = dlqName(queueName);

  const swallowExists = (error: unknown) => {
    if (error instanceof Error && error.message.includes('already exists')) return;
    throw error;
  };

  if (!(await boss.getQueue(dlq))) {
    try {
      await boss.createQueue(dlq, { retryLimit: 0 });
    } catch (e) {
      swallowExists(e);
    }
  }
  if (!(await boss.getQueue(queueName))) {
    try {
      await boss.createQueue(queueName, {
        retryLimit: cfg.retryLimit,
        retryDelay: cfg.retryDelay,
        expireInSeconds: cfg.expireInSeconds,
        deadLetter: dlq,
      });
    } catch (e) {
      swallowExists(e);
    }
  }
}

// ============================================================================
// Dispatcher
// ============================================================================

export interface PgBossDispatcherDeps {
  boss: PgBoss;
  queueName: string;
  /** Partition this dispatcher is bound to — stamped into every enqueued job. */
  partitionKey: string;
  config?: StepQueueConfig;
}

/** A job id that cannot exist — see {@link fillQueueCache}. */
const NIL_JOB_ID = '00000000-0000-0000-0000-000000000000';

/**
 * pg-boss keeps a per-instance cache of queue rows and consults it on every
 * `send` — on a miss it reads the queue on its *own* connection, whatever `db`
 * the send was handed. `createQueue` evicts the entry rather than filling it,
 * and the `PgBoss` facade keeps the cache private. So after creating our queue
 * we fill the entry here, outside any transaction, through the one public call
 * that reads the queue via the cache and then changes nothing: cancelling a job
 * id that cannot exist. (A boss that does expose `getQueueCache` — a bare
 * manager — is asked directly.) Without this the first transactional send would
 * read on a second connection — a deadlock on a single-connection database.
 */
async function fillQueueCache(boss: PgBoss, queueName: string): Promise<void> {
  const cache = (boss as unknown as { getQueueCache?: (name: string) => Promise<unknown> }).getQueueCache;
  if (typeof cache === 'function') await cache.call(boss, queueName);
  else await boss.cancel(queueName, NIL_JOB_ID);
}

/** A flow-core `Dispatcher` backed by pg-boss. */
export function createPgBossDispatcher(deps: PgBossDispatcherDeps): Dispatcher {
  const { boss, queueName, partitionKey } = deps;
  const cfg = { ...DEFAULT_STEP_QUEUE_CONFIG, ...deps.config };

  /**
   * Queue DDL and cache warm, once. Memoised as a promise so concurrent first
   * sends share it; cleared on failure so the next attempt retries. The engine
   * calls this through `prepare` before its first transaction; a direct
   * `enqueueStep` outside any transaction is free to trigger it lazily.
   */
  let prepared: Promise<void> | null = null;
  function ensurePrepared(): Promise<void> {
    prepared ??= (async () => {
      await ensureStepQueue(boss, queueName, cfg);
      await fillQueueCache(boss, queueName);
    })().catch((error: unknown) => {
      prepared = null;
      throw error;
    });
    return prepared;
  }

  /**
   * Adapt flow's `SqlExecutor` to the shape pg-boss wants for a caller-supplied
   * connection. The two are the same idea under different names.
   */
  function asPgBossDb(handle: unknown): { executeSql(text: string, values?: unknown[]): Promise<{ rows: unknown[] }> } | undefined {
    const exec = handle as { query?: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }> } | undefined;
    if (!exec || typeof exec.query !== 'function') return undefined;
    return { executeSql: (text, values) => exec.query!(text, values) };
  }

  async function send(
    payload: DispatchStepPayload,
    options?: EnqueueOptions,
    handle?: unknown,
  ): Promise<Result<void, FlowErrorShape>> {
    try {
      const wire: WireStepPayload = { kind: 'execute', ...payload, partitionKey };
      const parsed = WIRE_STEP_PAYLOAD_SCHEMA.safeParse(wire);
      if (!parsed.success) {
        return { ok: false, error: { key: 'queue_error', message: `Invalid step payload: ${parsed.error.message}` } };
      }
      // Queue creation is DDL — never inside the caller's transaction, where a
      // rollback would undo it and a lock could outlive the send. With a
      // transaction handle this is expected to be a no-op already: the engine
      // ran `prepare` first. (On a single-connection database, reaching it here
      // with an open transaction would deadlock — which is why `prepare` exists.)
      await ensurePrepared();
      const startAfter = options?.startAfterSeconds;
      const db = handle === undefined ? undefined : asPgBossDb(handle);
      if (handle !== undefined && !db) {
        return { ok: false, error: { key: 'queue_error', message: 'Transaction handle is not a SqlExecutor — is the store store-pg?' } };
      }
      const jobId = await boss.send(queueName, parsed.data, {
        retryLimit: cfg.retryLimit,
        retryDelay: cfg.retryDelay,
        expireInSeconds: cfg.expireInSeconds,
        ...(startAfter != null && startAfter > 0 ? { startAfter } : {}),
        ...(db ? { db } : {}),
      });
      if (!jobId) {
        return { ok: false, error: { key: 'queue_error', message: `Failed to enqueue job to ${queueName}` } };
      }
      return { ok: true, value: undefined };
    } catch (error) {
      return { ok: false, error: { key: 'queue_error', message: error instanceof Error ? error.message : 'Unknown enqueue error' } };
    }
  }

  return {
    /** Queue DDL + cache fill, outside any transaction. See `Dispatcher.prepare`. */
    async prepare() {
      try {
        await ensurePrepared();
        return { ok: true, value: undefined };
      } catch (error) {
        return { ok: false, error: { key: 'queue_error', message: error instanceof Error ? error.message : 'Failed to prepare step queue' } };
      }
    },
    enqueueStep: (payload, options) => send(payload, options),
    /**
     * Enqueue on the store's open transaction, so the job and the state change
     * that produced it commit together. `handle` is the transaction-bound
     * `SqlExecutor` that `createWorkflowStore.runInTransaction` hands out.
     */
    enqueueStepIn: (handle, payload, options) => send(payload, options, handle),
  };
}

// ============================================================================
// Worker
// ============================================================================

export interface PgBossStepWorkerOptions {
  /** Steps fetched per poll. Default 1. */
  batchSize?: number;
  /** Base poll interval in seconds (pg-boss minimum 0.5). Default: pg-boss's own. */
  pollingIntervalSeconds?: number;
  /**
   * Poll interval used while pg-boss's LISTEN/NOTIFY is active for the queue, where
   * polling is only a backstop. Ignored when notify is off or unavailable.
   */
  notifyPollingIntervalSeconds?: number;
  /**
   * Keep fetching with no delay while every fetch returns a full `batchSize` batch;
   * the first short fetch ends the burst. Ignored when `batchSize` is 1.
   *
   * **Worth enabling for throughput.** Without it a worker spends most of its time
   * waiting out the poll interval: it drains a batch in milliseconds, then sleeps.
   * See the performance table in the README.
   */
  burstWhenBatchFull?: boolean;
  /** Burst while the queue's cached ready count exceeds this. Reacts on pg-boss's stats cadence. */
  burstWhenReadyExceeds?: number;
  /**
   * How many steps from one fetched batch run at a time. Default 1 (serial).
   *
   * Raising this only helps once the worker is not poll-bound — pair it with
   * `burstWhenBatchFull`. Each in-flight step needs a store connection, so keep
   * `concurrency × workers` within your Postgres pool and `max_connections`.
   */
  concurrency?: number;
}

export interface PgBossStepWorkerDeps {
  boss: PgBoss;
  queueName: string;
  config?: StepQueueConfig;
  logger?: Logger;
  workerOptions?: PgBossStepWorkerOptions;
}

/** Handles a single step job. Reconstruct the partition-scoped engine and call
 * `engine.executeStep(workflowId, stepId)`. Throw to trigger a pg-boss retry. */
export type StepJobProcessor = (payload: WireStepPayload) => Promise<void>;

/** A pg-boss worker that drives `executeStep` for step jobs. */
export function createPgBossStepWorker(deps: PgBossStepWorkerDeps) {
  const { boss, queueName } = deps;
  let workerId: string | null = null;

  async function start(process: StepJobProcessor): Promise<void> {
    await ensureStepQueue(boss, queueName, deps.config);
    const opts = deps.workerOptions ?? {};
    const lanes = Math.max(1, opts.concurrency ?? 1);
    const workOpts: WorkOptions = {
      batchSize: opts.batchSize ?? 1,
      // Settle each job on its own outcome. Without this pg-boss fails the *whole*
      // batch when the handler throws, so one bad step would drag its neighbours
      // into a retry — wasteful, and it muddies which job actually dead-lettered.
      perJobResults: true,
      ...(opts.pollingIntervalSeconds != null && { pollingIntervalSeconds: opts.pollingIntervalSeconds }),
      ...(opts.notifyPollingIntervalSeconds != null && { notifyPollingIntervalSeconds: opts.notifyPollingIntervalSeconds }),
      ...(opts.burstWhenBatchFull != null && { burstWhenBatchFull: opts.burstWhenBatchFull }),
      ...(opts.burstWhenReadyExceeds != null && { burstWhenReadyExceeds: opts.burstWhenReadyExceeds }),
    };

    workerId = await boss.work<WireStepPayload>(queueName, workOpts, async (jobs: Job<WireStepPayload>[]) => {
      const results: JobResult[] = [];
      const pending = [...jobs];

      // `lanes` consumers drain the batch. At lanes = 1 this is the original
      // serial loop; above that, each in-flight step holds a store connection.
      await Promise.all(
        Array.from({ length: Math.min(lanes, pending.length) }, async () => {
          for (;;) {
            const job = pending.shift();
            if (!job) break;
            const parsed = WIRE_STEP_PAYLOAD_SCHEMA.safeParse(job.data);
            if (!parsed.success) {
              // Unprocessable: it will not parse on a retry either, so route it
              // straight to the dead-letter queue instead of burning attempts.
              results.push({
                id: job.id,
                status: 'deadletter',
                output: { message: `Invalid step payload for job ${job.id}: ${parsed.error.message}` },
              });
              continue;
            }
            try {
              await process(parsed.data);
              results.push({ id: job.id, status: 'completed' });
            } catch (error) {
              // Fails only this job — pg-boss applies the queue's retry policy to
              // it alone, and the rest of the batch keeps its own outcome.
              results.push({
                id: job.id,
                status: 'failed',
                output: { message: error instanceof Error ? error.message : String(error) },
              });
            }
          }
        }),
      );
      return results;
    });
  }

  async function stop(): Promise<void> {
    if (workerId) {
      await boss.offWork(workerId);
      workerId = null;
    }
  }

  return { start, stop };
}

// ============================================================================
// Dead-letter worker
// ============================================================================

export interface PgBossDlqWorkerDeps {
  boss: PgBoss;
  queueName: string;
  logger?: Logger;
  pollingIntervalSeconds?: number;
}

/** Invoked for a step job that exhausted all retries. Typically calls
 * `engine.handleStepExhausted(workflowId, stepId, reason)`. */
export type DlqProcessor = (payload: WireStepPayload) => Promise<void>;

/** A pg-boss worker on the step queue's dead-letter queue. */
export function createPgBossDlqWorker(deps: PgBossDlqWorkerDeps) {
  const { boss, queueName, logger } = deps;
  const dlq = dlqName(queueName);
  let workerId: string | null = null;

  async function start(onDlq: DlqProcessor): Promise<void> {
    workerId = await boss.work<WireStepPayload>(
      dlq,
      { pollingIntervalSeconds: deps.pollingIntervalSeconds ?? 30 },
      async (jobs: Job<WireStepPayload>[]) => {
        for (const job of jobs) {
          const parsed = WIRE_STEP_PAYLOAD_SCHEMA.safeParse(job.data);
          if (!parsed.success) {
            logger?.error('Invalid payload in DLQ', undefined, { jobId: job.id });
            continue;
          }
          logger?.error('Step job dead-lettered', undefined, {
            jobId: job.id,
            workflowId: parsed.data.workflowId,
            stepId: parsed.data.stepId,
            stepKey: parsed.data.stepKey,
          });
          try {
            await onDlq(parsed.data);
          } catch (error) {
            logger?.error('DLQ handler failed', error instanceof Error ? error : undefined, { jobId: job.id });
          }
        }
      },
    );
  }

  async function stop(): Promise<void> {
    if (workerId) {
      await boss.offWork(workerId);
      workerId = null;
    }
  }

  return { start, stop };
}
