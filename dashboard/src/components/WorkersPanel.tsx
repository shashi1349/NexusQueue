import { useState, useEffect } from 'react';
import { getWorkers } from '../api.js';
import type { WorkerInfo } from '../types.js';

const STATUS_COLORS: Record<string, string> = {
  active: 'bg-green-100 text-green-800',
  idle: 'bg-gray-100 text-gray-800',
  draining: 'bg-yellow-100 text-yellow-800',
};

function relativeTime(timestamp: number | null): string {
  if (timestamp == null) return '-';
  const diff = Math.floor((Date.now() - timestamp) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

export default function WorkersPanel() {
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);

  useEffect(() => {
    const fetchWorkers = () => {
      getWorkers()
        .then((data) => setWorkers(data.workers))
        .catch(() => {});
    };
    fetchWorkers();
    const interval = setInterval(fetchWorkers, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <h2 className="text-lg font-semibold text-gray-800 mb-4">Workers</h2>
      {workers.length === 0 ? (
        <p className="text-gray-400 text-sm">No active workers</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-600">
                <th className="pb-2 pr-4">ID</th>
                <th className="pb-2 pr-4">Status</th>
                <th className="pb-2 pr-4">Queue</th>
                <th className="pb-2 pr-4">Jobs</th>
                <th className="pb-2">Last Heartbeat</th>
              </tr>
            </thead>
            <tbody>
              {workers.map((worker) => (
                <tr key={worker.id} className="border-b last:border-0">
                  <td className="py-2 pr-4 font-mono">{worker.id.slice(0, 8)}</td>
                  <td className="py-2 pr-4">
                    <span
                      className={`px-2 py-0.5 rounded text-xs font-medium ${STATUS_COLORS[worker.status] || 'bg-gray-100 text-gray-800'}`}
                    >
                      {worker.status}
                    </span>
                  </td>
                  <td className="py-2 pr-4">{worker.queue}</td>
                  <td className="py-2 pr-4">{worker.currentJobs}</td>
                  <td className="py-2 text-gray-500">{relativeTime(worker.lastHeartbeat)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
