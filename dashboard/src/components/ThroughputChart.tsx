import { useState, useEffect, useRef } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { wsManager } from '../ws.js';

interface DataPoint {
  time: string;
  throughput: number;
}

export default function ThroughputChart() {
  const [data, setData] = useState<DataPoint[]>([]);
  const counterRef = useRef(0);

  useEffect(() => {
    const unsubscribe = wsManager.subscribe((event) => {
      if (event.type === 'job.completed') {
        counterRef.current += 1;
      }
    });

    const interval = setInterval(() => {
      const now = new Date();
      const timeStr = now.toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      });
      const point: DataPoint = { time: timeStr, throughput: counterRef.current };
      counterRef.current = 0;
      setData((prev) => [...prev, point].slice(-300));
    }, 1000);

    return () => {
      unsubscribe();
      clearInterval(interval);
    };
  }, []);

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <h2 className="text-lg font-semibold text-gray-800 mb-4">Throughput (jobs/sec)</h2>
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
          <YAxis allowDecimals={false} />
          <Tooltip />
          <Line type="monotone" dataKey="throughput" stroke="#3b82f6" dot={false} strokeWidth={2} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
