import http from 'node:http';
import { createPgPool, createRedisClient, createLogger } from '@nexusqueue/shared';
import { loadWorkerConfig } from './config.js';
import { Worker } from './worker.js';
import { Scheduler } from './scheduler.js';
import { Janitor } from './janitor.js';

/**
 * Worker entry point.
 *
 * In production each worker is its own container/process. Locally you can
 * run `npm run dev:worker` in one terminal and `npm run dev:server` in
 * another, or use the smoke test which spins both up in-process.
 *
 * Phase 3 starts a Scheduler alongside the Worker (same process,
 * separate polling loop) to promote delayed jobs and fire cron jobs.
 *
 * Phase 4 adds the Janitor for dead-worker detection and job recovery.
 */
async function main(): Promise<void> {
  const cfg = loadWorkerConfig();
  const logger = createLogger('worker');
  const redis = createRedisClient(cfg.redisUrl);
  const pg = createPgPool(cfg.databaseUrl);

  // Wait for Redis connection before issuing commands.
  // ioredis with enableOfflineQueue=false rejects commands before TCP handshake.
  await new Promise<void>((resolve, reject) => {
    if (redis.status === 'ready') { resolve(); return; }
    redis.once('ready', resolve);
    redis.once('error', reject);
  });
  logger.info('redis connected');

  const worker = new Worker({
    redis,
    pg,
    queue: cfg.queue,
    workerId: cfg.workerId,
    concurrency: cfg.concurrency,
  });

  worker.register('echo', async (payload, ctx) => {
    logger.debug(
      { jobId: ctx.jobId, attempt: ctx.attempt, payload },
      'echo job executing',
    );
    return { echoed: payload };
  });

  // Start the scheduler (promotes delayed jobs, fires cron).
  const scheduler = new Scheduler({ redis, pg });
  scheduler.start();

  // Conditionally start janitor.
  let janitor: Janitor | null = null;
  if (cfg.janitorEnabled) {
    janitor = new Janitor({ redis, pg, intervalMs: cfg.janitorIntervalMs });
    janitor.start();
  }

  await worker.start();
  logger.info(
    { workerId: cfg.workerId, queue: cfg.queue, concurrency: cfg.concurrency, handlers: ['echo'] },
    'worker started',
  );

  // Minimal HTTP health server so the worker can run as a Render Web Service
  // (free tier doesn't support Background Workers).
  const healthPort = Number(process.env.HEALTH_PORT ?? '0');
  if (healthPort > 0) {
    const healthServer = http.createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', workerId: cfg.workerId }));
    });
    healthServer.listen(healthPort, () => {
      logger.info({ port: healthPort }, 'worker health endpoint listening');
    });
  }

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'received shutdown signal, draining...');
    scheduler.stop();
    if (janitor) janitor.stop();
    await worker.stop();
    await Promise.all([redis.quit(), pg.end()]);
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  const logger = createLogger('worker');
  logger.fatal({ err }, 'failed to start');
  process.exit(1);
});

export { Worker } from './worker.js';
export { HandlerRegistry, type JobHandler, type HandlerContext } from './handlers.js';
export { Scheduler } from './scheduler.js';
export { CronManager, type CronJobDef } from './cron.js';
export { RateLimiter } from './rate-limiter.js';
export { Janitor } from './janitor.js';
