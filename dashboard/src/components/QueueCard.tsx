import { useNavigate } from 'react-router-dom';
import type { QueueStats } from '../types.js';

interface QueueCardProps {
  queue: QueueStats;
}

export default function QueueCard({ queue }: QueueCardProps) {
  const navigate = useNavigate();

  return (
    <div
      onClick={() => navigate(`/queues/${encodeURIComponent(queue.name)}`)}
      className="bg-white rounded-lg shadow p-4 cursor-pointer hover:shadow-md transition"
    >
      <h3 className="text-lg font-semibold text-gray-800 mb-3">{queue.name}</h3>
      <div className="flex flex-wrap gap-2">
        <span className="px-2 py-1 text-xs font-medium rounded bg-yellow-100 text-yellow-800">
          Pending: {queue.pending}
        </span>
        <span className="px-2 py-1 text-xs font-medium rounded bg-blue-100 text-blue-800">
          Active: {queue.active}
        </span>
        <span className="px-2 py-1 text-xs font-medium rounded bg-green-100 text-green-800">
          Completed: {queue.completed}
        </span>
        <span className="px-2 py-1 text-xs font-medium rounded bg-red-100 text-red-800">
          Failed: {queue.failed}
        </span>
        <span className="px-2 py-1 text-xs font-medium rounded bg-purple-100 text-purple-800">
          DLQ: {queue.dlq}
        </span>
      </div>
    </div>
  );
}
