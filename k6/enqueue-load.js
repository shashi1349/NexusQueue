import http from 'k6/http';
import { check, sleep } from 'k6';
import { BASE_URL, randomPayload, randomQueueName, randomPriority } from './helpers.js';

export const options = {
  stages: [
    { duration: '30s', target: 100 },  // ramp up to 100 VUs over 30s
    { duration: '2m', target: 100 },   // sustain 100 VUs for 2 minutes
    { duration: '10s', target: 0 },    // ramp down over 10s
  ],
  thresholds: {
    http_req_duration: ['p(95)<100'],  // 95th percentile response time < 100ms
    http_req_failed: ['rate<0.01'],    // error rate < 1%
  },
};

const headers = { 'Content-Type': 'application/json' };

// If API_KEY is set, include it in requests
if (__ENV.API_KEY) {
  headers['X-API-Key'] = __ENV.API_KEY;
}

export default function () {
  const body = JSON.stringify({
    jobName: `load-test-${Date.now()}`,
    payload: randomPayload(),
    queue: randomQueueName(),
    priority: randomPriority(),
  });

  const res = http.post(`${BASE_URL}/jobs`, body, { headers });

  check(res, {
    'status is 201': (r) => r.status === 201,
  });

  sleep(0.1);
}
