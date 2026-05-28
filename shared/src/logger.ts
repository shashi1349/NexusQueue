import pino from 'pino';

export type Logger = pino.Logger;

export function createLogger(service: string): Logger {
  const level = process.env.LOG_LEVEL ?? 'info';

  if (process.env.NODE_ENV !== 'production') {
    try {
      return pino({
        level,
        base: { service, pid: process.pid },
        transport: { target: 'pino-pretty' },
      });
    } catch {
      // pino-pretty not available, fall through to JSON logger
    }
  }

  return pino({
    level,
    base: { service, pid: process.pid },
  });
}
