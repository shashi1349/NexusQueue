import { useState, useEffect } from 'react';
import { getDlqJobs, requeueDlq, retryJob } from '../api.js';
import type { Job } from '../types.js';

interface DlqInspectorProps {
  queueName: string;
}

export default function DlqInspector({ queueName }: DlqInspectorProps) {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchJobs = async () => {
    try {
      const data = await getDlqJobs(queueName);
      setJobs(data.jobs);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load DLQ');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
  }, [queueName]);

  const handleRequeueAll = async () => {
    try {
      await requeueDlq(queueName);
      await fetchJobs();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Requeue failed');
    }
  };

  const handleRetry = async (jobId: string) => {
    try {
      await retryJob(jobId);
      await fetchJobs();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Retry failed');
    }
  };

  if (loading) {
    return <p className="text-gray-400">Loading DLQ...</p>;
  }

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-gray-800">Dead Letter Queue</h3>
        {jobs.length > 0 && (
          <button
            onClick={handleRequeueAll}
            className="px-3 py-1 bg-purple-600 text-white text-sm rounded hover:bg-purple-700 transition"
          >
            Requeue All
          </button>
        )}
      </div>
      {error && <p className="text-red-600 text-sm mb-2">{error}</p>}
      {jobs.length === 0 ? (
        <p className="text-gray-400 text-sm">No jobs in DLQ</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-600">
                <th className="pb-2 pr-4">ID</th>
                <th className="pb-2 pr-4">Name</th>
                <th className="pb-2 pr-4">Error</th>
                <th className="pb-2 pr-4">Created</th>
                <th className="pb-2">Action</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.id} className="border-b last:border-0">
                  <td className="py-2 pr-4 font-mono">{job.id.slice(0, 8)}</td>
                  <td className="py-2 pr-4">{job.jobName}</td>
                  <td className="py-2 pr-4 text-red-600 max-w-xs truncate">
                    {job.errorMessage || '-'}
                  </td>
                  <td className="py-2 pr-4 text-gray-500">
                    {new Date(job.createdAt).toLocaleString()}
                  </td>
                  <td className="py-2">
                    <button
                      onClick={() => handleRetry(job.id)}
                      className="px-2 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 transition"
                    >
                      Retry
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
