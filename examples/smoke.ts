/**
 * End-to-end smoke test for Phase 1.
 *
 * Spins up an in-process Producer + Worker, enqueues a few jobs, waits
 * for them to land in 'completed' or 'failed', and prints the result.
 *
 * Run:
 *   docker compose up -d           # start Redis + Postgres
 *   cp .env.example .env
 *   npm install
 *   npm run build                  # compile shared/server/worker
 *   npm run smoke
 *
 * If the script exits with code 0 and you see three completed jobs,
 * Phase 1 is working.
 */

import 'dotenv/config';
import {
  createPgPool,
  createRedisClient,
  getJob,
} from '@nexusqueue/shared';
import { Producer } from '@nexusqueue/server';
import { Worker } from '@nexusqueue/worker';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://nexus:nexus@localhost:5432/nexusqueue';
const QUEUE = 'smoke';

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function main(): Promise<void> {
  const redis = createRedisClient(REDIS_URL);
  const pg = createPgPool(DATABASE_URL);

  const producer = new Producer({ redis, pg });
  const worker = new Worker({ redis, pg, queue: QUEUE, workerId: 'smoke-worker' });

  worker.register('greet', async (payload: unknown, ctx) => {
    const p = payload as { name?: string };
    console.log(`  > greet handler: hello ${p.name} (job=${ctx.jobId})`);
    return { greeted: p.name };
  });

  worker.register('boom', async () => {
    throw new Error('intentional failure');
  });

  worker.start();

  console.log('Enqueueing 3 jobs...');
  const id1 = await producer.enqueue('greet', { name: 'Ada' }, { queue: QUEUE });
  const id2 = await producer.enqueue('greet', { name: 'Linus' }, { queue: QUEUE });
  const id3 = await producer.enqueue('boom', {}, { queue: QUEUE });
  console.log('  enqueued:', { id1, id2, id3 });

  // Poll Postgres until all three are terminal.
  const ids = [id1, id2, id3];
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const rows = await Promise.all(ids.map((id) => getJob(pg, id)));
    const allTerminal = rows.every(
      (r) => r && (r.status === 'completed' || r.status === 'failed'),
    );
    if (allTerminal) break;
    await sleep(200);
  }

  console.log('\nFinal job rows from Postgres:');
  for (const id of ids) {
    const job = await getJob(pg, id);
    console.log(`  ${id} -> ${job?.status} ${job?.errorMessage ?? ''}`);
  }

  // Capture final state BEFORE closing connections.
  const finals = await Promise.all(ids.map((id) => getJob(pg, id)));
  const completed = finals.filter((j) => j?.status === 'completed').length;
  const failed = finals.filter((j) => j?.status === 'failed').length;

  await worker.stop();
  await redis.quit();
  await pg.end();

  console.log(`\nSummary: ${completed} completed, ${failed} failed (expected 2 / 1).`);
  if (completed !== 2 || failed !== 1) {
    console.error('Smoke test FAILED — unexpected counts.');
    process.exit(1);
  }
  console.log('Smoke test PASSED.');
}

main().catch((err) => {
  console.error('smoke failed:', err);
  process.exit(1);
});
