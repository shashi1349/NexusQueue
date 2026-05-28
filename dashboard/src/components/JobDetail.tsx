import { useState, useEffect } from 'react';
import { getJob, retryJob } from '../api.js';
import type { Job } from '../types.js';

interface JobDetailProps {
  jobId: string;
  onClose?: () => void;
}

export default function JobDetail({ jobId, onClose }: JobDetailProps) {
  const [job, setJob] = useState<Job | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [retrying, setRetrying] = useState(false);

  useEffect(() => {
    setError(null);
    getJob(jobId)
      .then(setJob)
      .catch((err) => setError(err.message));
  }, [jobId]);

  const handleRetry = async () => {
    setRetrying(true);
    try {
      await retryJob(jobId);
      const updated = await getJob(jobId);
      setJob(updated);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Retry failed');
    } finally {
      setRetrying(false);
    }
  };

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-4">
        <p className="text-red-600">Error: {error}</p>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="bg-white rounded-lg shadow p-4">
        <p className="text-gray-400">Loading...</p>
      </div>
    );
  }

  const canRetry = job.status === 'failed' || job.status === 'dlq';

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-gray-800">Job Detail</h3>
        {onClose && (
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            Close
          </button>
        )}
      </div>
      <dl className="grid grid-cols-2 gap-2 text-sm">
        <dt className="text-gray-500">ID</dt>
        <dd className="font-mono">{job.id}</dd>
        <dt className="text-gray-500">Name</dt>
        <dd>{job.jobName}</dd>
        <dt className="text-gray-500">Queue</dt>
        <dd>{job.queueName}</dd>
        <dt className="text-gray-500">Status</dt>
        <dd>{job.status}</dd>
        <dt className="text-gray-500">Attempts</dt>
        <dd>
          {job.attempts} / {job.maxAttempts}
        </dd>
        <dt className="text-gray-500">Created</dt>
        <dd>{new Date(job.createdAt).toLocaleString()}</dd>
        <dt className="text-gray-500">Started</dt>
        <dd>{job.startedAt ? new Date(job.startedAt).toLocaleString() : '-'}</dd>
        <dt className="text-gray-500">Completed</dt>
        <dd>{job.completedAt ? new Date(job.completedAt).toLocaleString() : '-'}</dd>
        <dt className="text-gray-500">Error</dt>
        <dd className="text-red-600">{job.errorMessage || '-'}</dd>
        <dt className="text-gray-500">Payload</dt>
        <dd className="col-span-2 font-mono text-xs bg-gray-50 p-2 rounded overflow-auto max-h-32">
          {JSON.stringify(job.payload, null, 2)}
        </dd>
      </dl>
      {canRetry && (
        <button
          onClick={handleRetry}
          disabled={retrying}
          className="mt-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 transition"
        >
          {retrying ? 'Retrying...' : 'Retry Job'}
        </button>
      )}
    </div>
  );
}
