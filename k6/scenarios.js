import http from 'k6/http';
import { check, sleep } from 'k6';
import { BASE_URL, randomPayload, randomQueueName, randomPriority } from './helpers.js';

export const options = {
  scenarios: {
    burst: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 1000,
      maxDuration: '10s',
      exec: 'burst',
    },
    sustained: {
      executor: 'constant-arrival-rate',
      rate: 500,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 100,
      maxVUs: 200,
      exec: 'sustained',
      startTime: '15s', // start after burst finishes
    },
    mixed_priorities: {
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s',
      stages: [
        { duration: '1m', target: 200 },
        { duration: '3m', target: 200 },
        { duration: '1m', target: 0 },
      ],
      preAllocatedVUs: 100,
      maxVUs: 300,
      exec: 'mixedPriorities',
      startTime: '5m30s', // start after sustained finishes
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<100'],
    http_req_failed: ['rate<0.01'],
  },
};

const headers = { 'Content-Type': 'application/json' };

if (__ENV.API_KEY) {
  headers['X-API-Key'] = __ENV.API_KEY;
}

/**
 * Burst scenario: fire 1000 jobs as fast as possible with 50 VUs.
 */
export function burst() {
  const body = JSON.stringify({
    jobName: `burst-${Date.now()}`,
    payload: randomPayload(),
    queue: randomQueueName(),
    priority: 'normal',
  });

  const res = http.post(`${BASE_URL}/jobs`, body, { headers });

  check(res, {
    'burst: status is 201': (r) => r.status === 201,
  });
}

/**
 * Sustained scenario: constant rate of 500 req/s for 5 minutes.
 */
export function sustained() {
  const body = JSON.stringify({
    jobName: `sustained-${Date.now()}`,
    payload: randomPayload(),
    queue: randomQueueName(),
    priority: 'normal',
  });

  const res = http.post(`${BASE_URL}/jobs`, body, { headers });

  check(res, {
    'sustained: status is 201': (r) => r.status === 201,
  });
}

/**
 * Mixed priorities scenario: ramping rate with varied priority distribution.
 * Uses weighted priorities: 60% normal, 30% high, 10% low.
 */
export function mixedPriorities() {
  const priority = randomPriority();
  const body = JSON.stringify({
    jobName: `mixed-${priority}-${Date.now()}`,
    payload: randomPayload(),
    queue: randomQueueName(),
    priority,
  });

  const res = http.post(`${BASE_URL}/jobs`, body, { headers });

  check(res, {
    'mixed: status is 201': (r) => r.status === 201,
  });
}
