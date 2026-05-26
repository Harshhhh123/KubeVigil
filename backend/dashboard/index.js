import { config } from 'dotenv';
config();
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';
import pg from 'pg';
import * as k8s from '@kubernetes/client-node';
import cors from 'cors';

const { Pool } = pg;
const app = express();
app.use(cors());
app.use(express.json());

const httpServer = createServer(app);
const io = new Server(httpServer, { cors: { origin: '*' } });

// ─── PostgreSQL ───────────────────────────────────────────────────
const pgPool = new Pool({
  host:     process.env.PG_HOST,
  port:     5432,
  database: process.env.PG_DATABASE || 'kubevigil',
  user:     process.env.PG_USER     || 'postgres',
  password: process.env.PG_PASSWORD || 'kubevigil123',
  ssl: { rejectUnauthorized: false },
});

// ─── Kafka ────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'kubevigil-dashboard',
  brokers: [process.env.KAFKA_BROKER || 'kafka.kubevigil.svc.cluster.local:9092'],
});
const consumer = kafka.consumer({ groupId: 'dashboard-group' });

// ─── Kubernetes ───────────────────────────────────────────────────
const kc = new k8s.KubeConfig();
kc.loadFromCluster();
const k8sApps = kc.makeApiClient(k8s.AppsV1Api);

// ─── Report store — keyed by resourceName ─────────────────────────
const reportStore = new Map();

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
        console.log(`[dashboard] Drift detected: ${data.resourceName}`);
        io.emit('drift:detected', {
          id:           data.driftEventId,
          resourceName: data.resourceName,
          namespace:    data.namespace,
          severity:     data.severity,
          diffs:        data.diffs,
          detectedAt:   data.detectedAt,
          status:       'DRIFTED',
          report:       reportStore.get(data.resourceName) || null,
        });
      }

      if (topic === 'kubevigil.alert.dispatched') {
        console.log(`[dashboard] Report received for: ${data.resourceName}`);
        // Store by resourceName
        reportStore.set(data.resourceName, data.report);
        io.emit('drift:report', {
          id:           data.driftEventId,
          resourceName: data.resourceName,
          report:       data.report,
        });
      }
    },
  });
}

// ─── Poll PostgreSQL every 5 seconds ─────────────────────────────
let lastChecked = new Date(Date.now() - 60000).toISOString();

async function pollDriftEvents() {
  try {
    const result = await pgPool.query(
      `SELECT DISTINCT ON (resource_name) * 
       FROM drift_events 
       WHERE detected_at > $1
       ORDER BY resource_name, detected_at DESC`,
      [lastChecked]
    );
    if (result.rows.length > 0) {
      lastChecked = new Date().toISOString();
      result.rows.forEach(row => {
        io.emit('drift:detected', {
          id:           row.id,
          resourceName: row.resource_name,
          namespace:    row.namespace,
          severity:     row.severity,
          diffs:        row.diff,
          detectedAt:   row.detected_at,
          status:       row.resolved_at ? 'RESTORED' : 'DRIFTED',
          report:       reportStore.get(row.resource_name) || null,
        });
      });
    }
  } catch (err) {
    console.error('[dashboard] Poll error:', err.message);
  }
}

// ─── REST API ─────────────────────────────────────────────────────
app.get('/api/drifts', async (req, res) => {
  try {
    const result = await pgPool.query(
      `SELECT DISTINCT ON (resource_name) * 
       FROM drift_events 
       ORDER BY resource_name, detected_at DESC`
    );
    res.json(result.rows.map(row => ({
      id:           row.id,
      resourceName: row.resource_name,
      namespace:    row.namespace,
      severity:     row.severity,
      diffs:        row.diff,
      detectedAt:   row.detected_at,
      status:       row.resolved_at ? 'RESTORED' : 'DRIFTED',
      report:       reportStore.get(row.resource_name) || null,
    })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/restore', async (req, res) => {
  const { resourceName, namespace, replicas } = req.body;
  try {
    await k8sApps.patchNamespacedDeployment(
      resourceName, namespace,
      { spec: { replicas: parseInt(replicas) } },
      undefined, undefined, undefined, undefined, undefined,
      { headers: { 'Content-Type': 'application/merge-patch+json' } }
    );

    await pgPool.query(
      `UPDATE drift_events SET resolved_at = NOW() WHERE resource_name = $1 AND resolved_at IS NULL`,
      [resourceName]
    );

    io.emit('drift:restored', { resourceName });
    console.log(`[dashboard] Restored ${resourceName} to ${replicas} replicas`);
    res.json({ success: true });
  } catch (err) {
    console.error('[dashboard] Restore failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

// ─── Socket.IO ────────────────────────────────────────────────────
io.on('connection', async (socket) => {
  console.log('[dashboard] Client connected');
  try {
    const result = await pgPool.query(
      `SELECT DISTINCT ON (resource_name) * 
       FROM drift_events 
       ORDER BY resource_name, detected_at DESC`
    );
    socket.emit('drift:history', result.rows.map(row => ({
      id:           row.id,
      resourceName: row.resource_name,
      namespace:    row.namespace,
      severity:     row.severity,
      diffs:        row.diff,
      detectedAt:   row.detected_at,
      status:       row.resolved_at ? 'RESTORED' : 'DRIFTED',
      report:       reportStore.get(row.resource_name) || null,
    })));
  } catch (err) {
    console.error('[dashboard] History error:', err.message);
  }
});

// ─── Start ────────────────────────────────────────────────────────
const PORT = process.env.PORT || 4000;
httpServer.listen(PORT, async () => {
  console.log(`[dashboard] Server running on port ${PORT}`);
  await startKafkaConsumer();
  setInterval(pollDriftEvents, 5000);
  console.log('[dashboard] Polling PostgreSQL every 5 seconds...');
});