import type { QueueStats, Job, WorkerInfo } from './types.js';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:3000';

export async function getQueues(): Promise<{ queues: QueueStats[] }> {
  const res = await fetch(`${API_URL}/queues`);
  if (!res.ok) throw new Error(`Failed to fetch queues: ${res.status}`);
  return res.json();
}

export async function getQueueJobs(
  name: string,
  opts?: { status?: string; limit?: number; offset?: number },
): Promise<{ jobs: Job[]; total: number }> {
  const params = new URLSearchParams();
  if (opts?.status) params.set('status', opts.status);
  if (opts?.limit != null) params.set('limit', String(opts.limit));
  if (opts?.offset != null) params.set('offset', String(opts.offset));
  const qs = params.toString();
  const res = await fetch(`${API_URL}/queues/${encodeURIComponent(name)}/jobs${qs ? `?${qs}` : ''}`);
  if (!res.ok) throw new Error(`Failed to fetch jobs: ${res.status}`);
  return res.json();
}

export async function getJob(id: string): Promise<Job> {
  const res = await fetch(`${API_URL}/jobs/${encodeURIComponent(id)}`);
  if (!res.ok) throw new Error(`Failed to fetch job: ${res.status}`);
  return res.json();
}

export async function retryJob(id: string): Promise<{ success: boolean }> {
  const res = await fetch(`${API_URL}/jobs/${encodeURIComponent(id)}/retry`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  });
  if (!res.ok) throw new Error(`Failed to retry job: ${res.status}`);
  return res.json();
}

export async function getWorkers(): Promise<{ workers: WorkerInfo[] }> {
  const res = await fetch(`${API_URL}/workers`);
  if (!res.ok) throw new Error(`Failed to fetch workers: ${res.status}`);
  return res.json();
}

export async function getDlqJobs(queueName: string): Promise<{ jobs: Job[] }> {
  const res = await fetch(`${API_URL}/queues/${encodeURIComponent(queueName)}/dlq`);
  if (!res.ok) throw new Error(`Failed to fetch DLQ jobs: ${res.status}`);
  return res.json();
}

export async function requeueDlq(
  queueName: string,
  jobIds?: string[],
): Promise<{ requeued: number }> {
  const body = jobIds ? { jobIds } : { all: true };
  const res = await fetch(`${API_URL}/queues/${encodeURIComponent(queueName)}/dlq/requeue`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Failed to requeue DLQ jobs: ${res.status}`);
  return res.json();
}
