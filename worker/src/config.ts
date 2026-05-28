export interface WorkerConfig {
  redisUrl: string;
  databaseUrl: string;
  queue: string;
  concurrency: number;
  workerId: string;
  janitorEnabled: boolean;
  janitorIntervalMs: number;
}

export function loadWorkerConfig(env: NodeJS.ProcessEnv = process.env): WorkerConfig {
  const redisUrl = env.REDIS_URL;
  const databaseUrl = env.DATABASE_URL;
  if (!redisUrl) throw new Error('REDIS_URL is required');
  if (!databaseUrl) throw new Error('DATABASE_URL is required');

  return {
    redisUrl,
    databaseUrl,
    queue: env.WORKER_QUEUE ?? 'default',
    concurrency: Number(env.WORKER_CONCURRENCY ?? 5),
    workerId: env.WORKER_ID ?? `worker-${process.pid}`,
    janitorEnabled: env.JANITOR_ENABLED !== 'false',
    janitorIntervalMs: Number(env.JANITOR_INTERVAL_MS ?? 30000),
  };
}
