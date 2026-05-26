import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';
import * as k8s from '@kubernetes/client-node';
import cors from 'cors';

const app = express();
app.use(cors());
app.use(express.json());

const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' }
});

// ─── Kubernetes setup ─────────────────────────────────────────────
const kc = new k8s.KubeConfig();
kc.loadFromDefault();
const k8sApps = kc.makeApiClient(k8s.AppsV1Api);

// ─── Kafka setup ──────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'kubevigil-dashboard',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'dashboard-group' });

// ─── In memory drift store ────────────────────────────────────────
const driftEvents = [];

// ─── Kafka consumer ───────────────────────────────────────────────
async function startKafkaConsumer() {
  await consumer.connect();
  console.log('[dashboard] Kafka connected');

  await consumer.subscribe({
    topics: ['kubevigil.drift.detected', 'kubevigil.alert.dispatched'],
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const data = JSON.parse(message.value.toString());

      if (topic === 'kubevigil.drift.detected') {
        const event = {
          id:           data.driftEventId,
          resourceName: data.resourceName,
          namespace:    data.namespace,
          severity:     data.severity,
          diffs:        data.diffs,
          detectedAt:   data.detectedAt,
          status:       'DRIFTED',
          report:       null,
        };
        driftEvents.unshift(event);
        if (driftEvents.length > 50) driftEvents.pop();

        console.log(`[dashboard] Drift event received — ${data.resourceName}`);
        io.emit('drift:detected', event);
      }

      if (topic === 'kubevigil.alert.dispatched') {
        const existing = driftEvents.find(e => e.id === data.driftEventId);
        if (existing) {
          existing.report = data.report;
          io.emit('drift:report', { id: data.driftEventId, report: data.report });
        }
      }
    },
  });
}

// ─── REST API ─────────────────────────────────────────────────────

// Get all drift events
app.get('/api/drifts', (req, res) => {
  res.json(driftEvents);
});

// Restore a drifted resource
app.post('/api/restore', async (req, res) => {
  const { resourceName, namespace, replicas } = req.body;

  try {
    await k8sApps.patchNamespacedDeployment(
      resourceName,
      namespace,
      { spec: { replicas: parseInt(replicas) } },
      undefined, undefined, undefined, undefined,
      undefined,
      { headers: { 'Content-Type': 'application/merge-patch+json' } }
    );

    // Update drift event status
    const event = driftEvents.find(e => e.resourceName === resourceName);
    if (event) {
      event.status = 'RESTORED';
      io.emit('drift:restored', { resourceName, replicas });
    }

    console.log(`[dashboard] Restored ${resourceName} to ${replicas} replicas`);
    res.json({ success: true, message: `${resourceName} restored to ${replicas} replicas` });
  } catch (err) {
    console.error('[dashboard] Restore failed:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// ─── Socket.IO ────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('[dashboard] Client connected');
  socket.emit('drift:history', driftEvents);

  socket.on('disconnect', () => {
    console.log('[dashboard] Client disconnected');
  });
});

// ─── Start ────────────────────────────────────────────────────────
const PORT = process.env.PORT || 4000;

httpServer.listen(PORT, async () => {
  console.log(`[dashboard] Server running on port ${PORT}`);
  await startKafkaConsumer();
});