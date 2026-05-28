import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { getQueueJobs } from '../api.js';
import type { Job } from '../types.js';
import JobDetail from '../components/JobDetail.tsx';
import DlqInspector from '../components/DlqInspector.tsx';

const STATUSES = ['all', 'pending', 'active', 'completed', 'failed', 'dlq'];
const PAGE_SIZE = 20;

export default function QueueDetail() {
  const { name } = useParams<{ name: string }>();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [total, setTotal] = useState(0);
  const [status, setStatus] = useState('all');
  const [offset, setOffset] = useState(0);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);

  useEffect(() => {
    if (!name) return;
    const statusParam = status === 'all' ? undefined : status;
    getQueueJobs(name, { status: statusParam, limit: PAGE_SIZE, offset })
      .then((data) => {
        setJobs(data.jobs);
        setTotal(data.total);
      })
      .catch(() => {});
  }, [name, status, offset]);

  if (!name) return null;

  const totalPages = Math.ceil(total / PAGE_SIZE);
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Queue: {name}</h1>

      <div className="flex gap-2 flex-wrap">
        {STATUSES.map((s) => (
          <button
            key={s}
            onClick={() => {
              setStatus(s);
              setOffset(0);
            }}
            className={`px-3 py-1 text-sm rounded transition ${
              status === s
                ? 'bg-blue-600 text-white'
                : 'bg-white text-gray-700 border hover:bg-gray-50'
            }`}
          >
            {s.charAt(0).toUpperCase() + s.slice(1)}
          </button>
        ))}
      </div>

      <div className="flex gap-6">
        <div className="flex-1">
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-600 bg-gray-50">
                  <th className="p-3">ID</th>
                  <th className="p-3">Name</th>
                  <th className="p-3">Status</th>
                  <th className="p-3">Attempts</th>
                  <th className="p-3">Created</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr
                    key={job.id}
                    onClick={() => setSelectedJobId(job.id)}
                    className={`border-b last:border-0 cursor-pointer hover:bg-blue-50 transition ${
                      selectedJobId === job.id ? 'bg-blue-50' : ''
                    }`}
                  >
                    <td className="p-3 font-mono">{job.id.slice(0, 8)}</td>
                    <td className="p-3">{job.jobName}</td>
                    <td className="p-3">{job.status}</td>
                    <td className="p-3">
                      {job.attempts}/{job.maxAttempts}
                    </td>
                    <td className="p-3 text-gray-500">
                      {new Date(job.createdAt).toLocaleString()}
                    </td>
                  </tr>
                ))}
                {jobs.length === 0 && (
                  <tr>
                    <td colSpan={5} className="p-3 text-center text-gray-400">
                      No jobs found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-4">
              <button
                onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                disabled={offset === 0}
                className="px-3 py-1 text-sm border rounded disabled:opacity-50 hover:bg-gray-50 transition"
              >
                Previous
              </button>
              <span className="text-sm text-gray-600">
                Page {currentPage} of {totalPages}
              </span>
              <button
                onClick={() => setOffset(offset + PAGE_SIZE)}
                disabled={currentPage >= totalPages}
                className="px-3 py-1 text-sm border rounded disabled:opacity-50 hover:bg-gray-50 transition"
              >
                Next
              </button>
            </div>
          )}
        </div>

        {selectedJobId && (
          <div className="w-96">
            <JobDetail jobId={selectedJobId} onClose={() => setSelectedJobId(null)} />
          </div>
        )}
      </div>

      <DlqInspector queueName={name} />
    </div>
  );
}
