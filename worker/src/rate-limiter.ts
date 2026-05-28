import type { Redis } from 'ioredis';
import { redisKeys, type RateLimitConfig } from '@nexusqueue/shared';

/**
 * Token bucket rate limiter.
 *
 * Provides both a Lua script for production use (atomic) and a
 * fallback implementation using HGETALL + HSET for environments
 * where Lua eval is not available (e.g., ioredis-mock in tests).
 */
export class RateLimiter {
  /**
   * Lua script implementing the token bucket algorithm.
   * KEYS[1] = rate limit hash key
   * ARGV[1] = maxTokens, ARGV[2] = refillRate (tokens/sec), ARGV[3] = now (ms)
   *
   * Returns [1, 0] if allowed, [0, retryAfterMs] if denied.
   */
  static readonly LUA_SCRIPT = `
local key = KEYS[1]
local maxTokens = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call('HGETALL', key)
local tokens = maxTokens
local lastRefill = now

if #data > 0 then
  for i = 1, #data, 2 do
    if data[i] == 'tokens' then
      tokens = tonumber(data[i+1])
    elseif data[i] == 'lastRefill' then
      lastRefill = tonumber(data[i+1])
    end
  end
end

local elapsed = (now - lastRefill) / 1000
local newTokens = math.min(maxTokens, tokens + elapsed * refillRate)

if newTokens >= 1 then
  newTokens = newTokens - 1
  redis.call('HSET', key, 'tokens', tostring(newTokens), 'lastRefill', tostring(now))
  return {1, 0}
else
  local deficit = 1 - newTokens
  local retryAfterMs = math.ceil((deficit / refillRate) * 1000)
  redis.call('HSET', key, 'tokens', tostring(newTokens), 'lastRefill', tostring(now))
  return {0, retryAfterMs}
end
`;

  /**
   * Check rate limit using the Lua script (production path).
   */
  async checkLimit(
    redis: Redis,
    queueName: string,
    config: RateLimitConfig,
  ): Promise<{ allowed: boolean; retryAfterMs: number }> {
    const key = redisKeys.rateLimit(queueName);
    const now = Date.now();
    const result = await redis.eval(
      RateLimiter.LUA_SCRIPT,
      1,
      key,
      String(config.maxTokens),
      String(config.refillRate),
      String(now),
    ) as [number, number];
    return {
      allowed: result[0] === 1,
      retryAfterMs: result[1] ?? 0,
    };
  }

  /**
   * Non-Lua fallback using HGETALL + HSET.
   * Used in test environments where Lua eval is not supported (ioredis-mock).
   *
   * Known limitation: This fallback is non-atomic under concurrency. Two workers
   * can both read the same token count and both allow, exceeding the configured
   * rate. In production the Lua checkLimit() path should be used for atomicity.
   * The fallback exists solely because ioredis-mock does not support EVAL/EVALSHA.
   */
  async checkLimitFallback(
    redis: Redis,
    queueName: string,
    config: RateLimitConfig,
  ): Promise<{ allowed: boolean; retryAfterMs: number }> {
    const key = redisKeys.rateLimit(queueName);
    const now = Date.now();
    const data = await redis.hgetall(key);

    let tokens = config.maxTokens;
    let lastRefill = now;

    if (data && data.tokens !== undefined && data.lastRefill !== undefined) {
      tokens = Number(data.tokens);
      lastRefill = Number(data.lastRefill);
    }

    const elapsed = (now - lastRefill) / 1000;
    const newTokens = Math.min(config.maxTokens, tokens + elapsed * config.refillRate);

    if (newTokens >= 1) {
      const afterConsume = newTokens - 1;
      await redis.hset(key, { tokens: String(afterConsume), lastRefill: String(now) });
      return { allowed: true, retryAfterMs: 0 };
    } else {
      const deficit = 1 - newTokens;
      const retryAfterMs = Math.ceil((deficit / config.refillRate) * 1000);
      await redis.hset(key, { tokens: String(newTokens), lastRefill: String(now) });
      return { allowed: false, retryAfterMs };
    }
  }
}
