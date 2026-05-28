import { useState, useEffect } from 'react';
import { getQueues } from '../api.js';
import type { QueueStats } from '../types.js';
import QueueCard from '../components/QueueCard.tsx';
import WorkersPanel from '../components/WorkersPanel.tsx';
import ThroughputChart from '../components/ThroughputChart.tsx';
import JobTable from '../components/JobTable.tsx';

export default function Dashboard() {
  const [queues, setQueues] = useState<QueueStats[]>([]);

  useEffect(() => {
    const fetchQueues = () => {
      getQueues()
        .then((data) => setQueues(data.queues))
        .catch(() => {});
    };
    fetchQueues();
    const interval = setInterval(fetchQueues, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {queues.map((queue) => (
          <QueueCard key={queue.name} queue={queue} />
        ))}
        {queues.length === 0 && (
          <p className="text-gray-400 col-span-full">No queues found</p>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ThroughputChart />
        <WorkersPanel />
      </div>

      <JobTable />
    </div>
  );
}
