import { createPgPool, createRedisClient } from '@nexusqueue/shared';
import { loadWorkerConfig } from './config.js';
import { Worker } from './worker.js';
import { Scheduler } from './scheduler.js';

/**
 * Worker entry point.
 *
 * In production each worker is its own container/process. Locally you can
 * run `npm run dev:worker` in one terminal and `npm run dev:server` in
 * another, or use the smoke test which spins both up in-process.
 *
 * Phase 3 starts a Scheduler alongside the Worker (same process,
 * separate polling loop) to promote delayed jobs and fire cron jobs.
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

  // Start the scheduler (promotes delayed jobs, fires cron).
  const scheduler = new Scheduler({ redis, pg });
  scheduler.start();

  worker.start();
  // eslint-disable-next-line no-console
  console.log(
    `[worker:${cfg.workerId}] listening on queue="${cfg.queue}" handlers=[echo]`,
  );

  const shutdown = async (signal: string): Promise<void> => {
    // eslint-disable-next-line no-console
    console.log(`[worker:${cfg.workerId}] received ${signal}, draining...`);
    scheduler.stop();
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
export { Scheduler } from './scheduler.js';
export { CronManager, type CronJobDef } from './cron.js';
export { RateLimiter } from './rate-limiter.js';
