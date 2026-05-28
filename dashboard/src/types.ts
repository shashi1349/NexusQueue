export interface QueueStats {
  name: string;
  pending: number;
  active: number;
  completed: number;
  failed: number;
  dlq: number;
}

export interface Job {
  id: string;
  queueName: string;
  jobName: string;
  payload: unknown;
  status: string;
  attempts: number;
  maxAttempts: number;
  errorMessage: string | null;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
}

export interface JobEvent {
  type: string;
  jobId: string;
  jobName: string;
  queueName: string;
  timestamp: number;
  duration?: number;
}

export interface WorkerInfo {
  id: string;
  status: string;
  queue: string;
  startedAt: string | null;
  currentJobs: number;
  lastHeartbeat: number | null;
}
