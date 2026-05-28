import { createPgPool, createRedisClient } from '@nexusqueue/shared';
import { loadWorkerConfig } from './config.js';
import { Worker } from './worker.js';

/**
 * Worker entry point.
 *
 * In production each worker is its own container/process. Locally you can
 * run `npm run dev:worker` in one terminal and `npm run dev:server` in
 * another, or use the smoke test which spins both up in-process.
 *
 * Phase 1 ships ONE example handler ("echo") so you can see end-to-end
 * flow. Real apps register their own handlers before calling start().
 */
async function main(): Promise<void> {
  const cfg = loadWorkerConfig();
  const redis = createRedisClient(cfg.redisUrl);
  const pg = createPgPool(cfg.databaseUrl);

  const worker = new Worker({
    redis,
    pg,
    queue: cfg.queue,
    workerId: cfg.workerId,
  });

  worker.register('echo', async (payload, ctx) => {
    // eslint-disable-next-line no-console
    console.log(
      `[worker:${ctx.workerId}] echo job=${ctx.jobId} attempt=${ctx.attempt}`,
      payload,
    );
    return { echoed: payload };
  });

  worker.start();
  // eslint-disable-next-line no-console
  console.log(
    `[worker:${cfg.workerId}] listening on queue="${cfg.queue}" handlers=[echo]`,
  );

  const shutdown = async (signal: string): Promise<void> => {
    // eslint-disable-next-line no-console
    console.log(`[worker:${cfg.workerId}] received ${signal}, draining...`);
    await worker.stop();
    await Promise.all([redis.quit(), pg.end()]);
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[worker] failed to start:', err);
  process.exit(1);
});

export { Worker } from './worker.js';
export { HandlerRegistry, type JobHandler, type HandlerContext } from './handlers.js';
