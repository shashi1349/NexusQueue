/**
 * All process-wide configuration is read once at startup, validated,
 * and frozen. Any code that needs config imports from here.
 *
 * Why not pass `env` everywhere? Because every service needs the same
 * three or four values, and threading them through call sites is noise.
 */
export interface ServerConfig {
  redisUrl: string;
  databaseUrl: string;
  port: number;
  host: string;
}

export function loadServerConfig(env: NodeJS.ProcessEnv = process.env): ServerConfig {
  const redisUrl = env.REDIS_URL;
  const databaseUrl = env.DATABASE_URL;
  if (!redisUrl) throw new Error('REDIS_URL is required');
  if (!databaseUrl) throw new Error('DATABASE_URL is required');

  return {
    redisUrl,
    databaseUrl,
    port: Number(env.SERVER_PORT ?? 3000),
    host: env.SERVER_HOST ?? '0.0.0.0',
  };
}
