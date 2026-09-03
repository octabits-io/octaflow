/**
 * The pg-boss dispatcher against a fake boss — no Postgres. What is pinned here
 * is the *ordering* that keeps a single-connection database alive: queue DDL
 * and the queue-cache fill happen in `prepare`, outside any transaction, and a
 * transactional send afterwards touches nothing but the handle it was given.
 * The real-Postgres behaviour (atomic commit of job + state) is in `queue.test.ts`.
 */
import { describe, it, expect } from 'vitest';
import type { PgBoss } from 'pg-boss';
import { createPgBossDispatcher, ensureStepQueue } from './queue';

const NIL = '00000000-0000-0000-0000-000000000000';

function fakeBoss(options: { existing?: string[]; failCreate?: boolean; exposeCache?: boolean } = {}) {
  const calls: string[] = [];
  const queues = new Set(options.existing ?? []);
  let createFailures = options.failCreate ? 1 : 0;
  const boss: Record<string, unknown> = {
    async getQueue(name: string) {
      calls.push(`getQueue:${name}`);
      return queues.has(name) ? { name } : null;
    },
    async createQueue(name: string) {
      if (createFailures > 0) {
        createFailures -= 1;
        throw new Error('DDL refused');
      }
      calls.push(`createQueue:${name}`);
      queues.add(name);
    },
    async cancel(name: string, id: string) {
      calls.push(`cancel:${name}:${id === NIL ? 'nil' : id}`);
      return { jobs: [] };
    },
    async send(name: string, _data: unknown, opts: { db?: { executeSql: (t: string) => Promise<unknown> } }) {
      calls.push(`send:${name}:${opts.db ? 'handle' : 'own'}`);
      if (opts.db) await opts.db.executeSql('INSERT job');
      return 'job-1';
    },
  };
  if (options.exposeCache) {
    boss.getQueueCache = async (name: string) => {
      calls.push(`getQueueCache:${name}`);
      return { name, table: 'job' };
    };
  }
  return { boss: boss as unknown as PgBoss, calls };
}

const payload = { workflowId: 1, stepId: 2, stepKey: 'only', stepType: 'x' };

describe('ensureStepQueue', () => {
  it('creates only the queues that are missing — createQueue evicts pg-boss\'s cache, so an existing queue is left alone', async () => {
    const { boss, calls } = fakeBoss({ existing: ['steps-dlq'] });
    await ensureStepQueue(boss, 'steps');
    expect(calls).toEqual(['getQueue:steps-dlq', 'getQueue:steps', 'createQueue:steps']);
  });
});

describe('createPgBossDispatcher — prepare', () => {
  it('on a fresh database: creates the queues, fills the cache through the public API, once', async () => {
    const { boss, calls } = fakeBoss();
    const dispatcher = createPgBossDispatcher({ boss, queueName: 'steps', partitionKey: 'p' });

    const prepared = await dispatcher.prepare!();
    expect(prepared.ok).toBe(true);
    await dispatcher.prepare!();
    expect(calls).toEqual([
      'getQueue:steps-dlq', 'createQueue:steps-dlq',
      'getQueue:steps', 'createQueue:steps',
      // The facade hides its queue cache; cancelling an id that cannot exist
      // reads the queue through it and updates nothing.
      'cancel:steps:nil',
    ]);
  });

  it('a transactional send after prepare touches only its handle', async () => {
    const { boss, calls } = fakeBoss();
    const dispatcher = createPgBossDispatcher({ boss, queueName: 'steps', partitionKey: 'p' });
    await dispatcher.prepare!();
    calls.length = 0;

    const handleQueries: string[] = [];
    const handle = { query: async (sql: string) => { handleQueries.push(sql); return { rows: [] }; } };
    const sent = await dispatcher.enqueueStepIn!(handle, payload);

    expect(sent.ok).toBe(true);
    expect(calls).toEqual(['send:steps:handle']);
    expect(handleQueries).toEqual(['INSERT job']);
  });

  it('uses getQueueCache when a boss exposes it, instead of the cancel trick', async () => {
    const { boss, calls } = fakeBoss({ existing: ['steps-dlq', 'steps'], exposeCache: true });
    const dispatcher = createPgBossDispatcher({ boss, queueName: 'steps', partitionKey: 'p' });

    await dispatcher.prepare!();
    expect(calls).toEqual(['getQueue:steps-dlq', 'getQueue:steps', 'getQueueCache:steps']);
  });

  it('reports a prepare failure and retries on the next call', async () => {
    const { boss, calls } = fakeBoss({ failCreate: true });
    const dispatcher = createPgBossDispatcher({ boss, queueName: 'steps', partitionKey: 'p' });

    const first = await dispatcher.prepare!();
    expect(first.ok).toBe(false);
    expect(!first.ok && first.error.message).toContain('DDL refused');

    const second = await dispatcher.prepare!();
    expect(second.ok).toBe(true);
    expect(calls.filter((c) => c.startsWith('createQueue'))).toEqual(['createQueue:steps-dlq', 'createQueue:steps']);
    expect(calls.filter((c) => c.startsWith('cancel'))).toHaveLength(1);
  });

  it('still prepares lazily for a plain enqueue outside any transaction', async () => {
    const { boss, calls } = fakeBoss({ existing: ['steps-dlq', 'steps'] });
    const dispatcher = createPgBossDispatcher({ boss, queueName: 'steps', partitionKey: 'p' });

    const sent = await dispatcher.enqueueStep(payload);

    expect(sent.ok).toBe(true);
    expect(calls).toEqual(['getQueue:steps-dlq', 'getQueue:steps', 'cancel:steps:nil', 'send:steps:own']);
  });

  it('shares one in-flight prepare between concurrent first sends', async () => {
    const { boss, calls } = fakeBoss();
    const dispatcher = createPgBossDispatcher({ boss, queueName: 'steps', partitionKey: 'p' });

    await Promise.all([dispatcher.enqueueStep(payload), dispatcher.enqueueStep(payload), dispatcher.prepare!()]);

    expect(calls.filter((c) => c.startsWith('createQueue'))).toHaveLength(2);
    expect(calls.filter((c) => c.startsWith('cancel'))).toHaveLength(1);
    expect(calls.filter((c) => c.startsWith('send'))).toHaveLength(2);
  });
});
