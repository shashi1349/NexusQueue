import { Routes, Route } from 'react-router-dom';
import Layout from './components/Layout.tsx';
import Dashboard from './pages/Dashboard.tsx';
import QueueDetail from './pages/QueueDetail.tsx';

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/queues/:name" element={<QueueDetail />} />
      </Routes>
    </Layout>
  );
}
