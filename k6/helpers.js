/**
 * Shared utilities for k6 load tests.
 * NOTE: k6 uses its own JS runtime - these are NOT Node.js modules.
 */

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';

const QUEUE_NAMES = ['default', 'emails', 'notifications', 'reports', 'analytics'];

const JOB_NAMES = ['processOrder', 'sendEmail', 'generateReport', 'syncData', 'resizeImage'];

/**
 * Generate a random JSON payload with various field types.
 */
export function randomPayload() {
  return {
    userId: Math.floor(Math.random() * 10000),
    action: JOB_NAMES[Math.floor(Math.random() * JOB_NAMES.length)],
    timestamp: Date.now(),
    data: {
      value: Math.random() * 1000,
      flag: Math.random() > 0.5,
      tags: ['load-test', `batch-${Math.floor(Math.random() * 100)}`],
    },
  };
}

/**
 * Pick a random queue name from the pool.
 */
export function randomQueueName() {
  return QUEUE_NAMES[Math.floor(Math.random() * QUEUE_NAMES.length)];
}

/**
 * Return a weighted random priority: 60% normal, 30% high, 10% low.
 */
export function randomPriority() {
  const roll = Math.random();
  if (roll < 0.6) return 'normal';
  if (roll < 0.9) return 'high';
  return 'low';
}
