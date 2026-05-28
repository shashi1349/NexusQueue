import { useState, useEffect } from 'react';
import { wsManager } from '../ws.js';
import type { JobEvent } from '../types.js';

const STATUS_COLORS: Record<string, string> = {
  'job.created': 'bg-gray-100 text-gray-800',
  'job.started': 'bg-blue-100 text-blue-800',
  'job.completed': 'bg-green-100 text-green-800',
  'job.failed': 'bg-red-100 text-red-800',
  'job.dlq': 'bg-purple-100 text-purple-800',
  'job.retried': 'bg-yellow-100 text-yellow-800',
};

export default function JobTable() {
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [filterQueue, setFilterQueue] = useState<string>('');
  const [filterStatus, setFilterStatus] = useState<string>('');

  useEffect(() => {
    const unsubscribe = wsManager.subscribe((event) => {
      setEvents((prev) => [event, ...prev].slice(0, 100));
    });
    return unsubscribe;
  }, []);

  const queues = [...new Set(events.map((e) => e.queueName))];
  const statuses = [...new Set(events.map((e) => e.type))];

  const filtered = events.filter((e) => {
    if (filterQueue && e.queueName !== filterQueue) return false;
    if (filterStatus && e.type !== filterStatus) return false;
    return true;
  });

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-gray-800">Live Job Stream</h2>
        <div className="flex gap-2">
          <select
            value={filterQueue}
            onChange={(e) => setFilterQueue(e.target.value)}
            className="text-sm border rounded px-2 py-1"
          >
            <option value="">All Queues</option>
            {queues.map((q) => (
              <option key={q} value={q}>
                {q}
              </option>
            ))}
          </select>
          <select
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value)}
            className="text-sm border rounded px-2 py-1"
          >
            <option value="">All Statuses</option>
            {statuses.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-gray-600">
              <th className="pb-2 pr-4">Time</th>
              <th className="pb-2 pr-4">Job ID</th>
              <th className="pb-2 pr-4">Name</th>
              <th className="pb-2 pr-4">Queue</th>
              <th className="pb-2 pr-4">Status</th>
              <th className="pb-2">Duration</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((event, i) => (
              <tr key={`${event.jobId}-${event.timestamp}-${i}`} className="border-b last:border-0">
                <td className="py-2 pr-4 text-gray-500">
                  {new Date(event.timestamp).toLocaleTimeString()}
                </td>
                <td className="py-2 pr-4 font-mono">{event.jobId.slice(0, 8)}</td>
                <td className="py-2 pr-4">{event.jobName}</td>
                <td className="py-2 pr-4">{event.queueName}</td>
                <td className="py-2 pr-4">
                  <span
                    className={`px-2 py-0.5 rounded text-xs font-medium ${STATUS_COLORS[event.type] || 'bg-gray-100 text-gray-800'}`}
                  >
                    {event.type}
                  </span>
                </td>
                <td className="py-2 text-gray-500">
                  {event.duration != null ? `${event.duration}ms` : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <p className="text-center text-gray-400 py-8">No events yet. Waiting for jobs...</p>
        )}
      </div>
    </div>
  );
}
