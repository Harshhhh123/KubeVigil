import 'dotenv/config';
import { Kafka } from 'kafkajs';
import { Octokit } from '@octokit/rest';

import pg from 'pg';
import { createClient } from 'redis';
import yaml from 'js-yaml';
import pkg from 'deep-diff';
const { diff } = pkg;

const { Pool } = pg;

// ─── Clients setup ────────────────────────────────────────────────

const kafka = new Kafka({
  clientId: 'kubevigil-drift-engine',
  brokers: [process.env.KAFKA_BROKER || 'kafka.kubevigil.svc.cluster.local:9092'],
});

const consumer = kafka.consumer({ groupId: 'drift-engine-group' });
const producer = kafka.producer();

const octokit = new Octokit({
  auth: process.env.GITHUB_TOKEN,
});

const pgPool = new Pool({
  host:     process.env.PG_HOST     || 'postgresql.kubevigil.svc.cluster.local',
  port:     5432,
  database: process.env.PG_DATABASE || 'kubevigil',
  user:     process.env.PG_USER     || 'postgres',
  password: process.env.PG_PASSWORD || 'kubevigil123',
  ssl: { rejectUnauthorized: false },
});

const redisClient = createClient({
  url: process.env.REDIS_URL || 'redis://redis-master.kubevigil.svc.cluster.local:6379',
});

const GITHUB_OWNER = process.env.GITHUB_OWNER || 'Harshhhh123';
const GITHUB_REPO  = process.env.GITHUB_REPO  || 'KubeVigil';

// ─── Severity classifier ──────────────────────────────────────────
function classifySeverity(diffs) {
  for (const d of diffs) {
    const path = d.path?.join('.') || '';
    if (path.includes('replicas'))   return 'CRITICAL';
    if (path.includes('resources'))  return 'HIGH';
    if (path.includes('containers')) return 'HIGH';
  }
  return 'LOW';
}

// ─── Fetch declared manifest from GitHub ─────────────────────────
async function fetchGitManifest(resourceKind, resourceName, namespace) {
  try {
    const path = `k8s/${namespace}/${resourceKind.toLowerCase()}/${resourceName}.yaml`;

    const response = await octokit.repos.getContent({
      owner: GITHUB_OWNER,
      repo:  GITHUB_REPO,
      path,
    });

    const content = Buffer.from(response.data.content, 'base64').toString('utf8');
    return yaml.load(content);
  } catch (err) {
    if (err.status === 404) {
      console.log(`[drift-engine] No Git manifest for ${resourceKind}/${resourceName} — skipping`);
      return null;
    }
    throw err;
  }
}

// ─── Core drift detection ─────────────────────────────────────────
async function detectDrift(snapshot) {
  const { resourceKind, resourceName, namespace, fullObject } = snapshot;

  const cacheKey = `${namespace}:${resourceKind}:${resourceName}`;

  const gitManifest = await fetchGitManifest(resourceKind, resourceName, namespace);
  if (!gitManifest) return;

  const liveSpec = fullObject.spec;
  const gitSpec  = gitManifest.spec;
  const diffs    = diff(gitSpec, liveSpec);

  if (!diffs || diffs.length === 0) {
    await redisClient.set(cacheKey, JSON.stringify(fullObject), { EX: 3600 });
    console.log(`[drift-engine] No drift on ${resourceKind}/${resourceName}`);
    return;
  }

  const severity = classifySeverity(diffs);
  console.log(`[drift-engine] DRIFT DETECTED — ${resourceKind}/${resourceName} — ${severity}`);

  const result = await pgPool.query(
    `INSERT INTO drift_events
       (resource_name, resource_kind, namespace, diff, severity)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING id`,
    [resourceName, resourceKind, namespace, JSON.stringify(diffs), severity]
  );

  const driftEventId = result.rows[0].id;
  console.log(`[drift-engine] Written to PostgreSQL — id: ${driftEventId}`);

  await producer.send({
    topic: 'kubevigil.drift.detected',
    messages: [{
      key:   `${namespace}/${resourceKind}/${resourceName}`,
      value: JSON.stringify({
        driftEventId,
        resourceKind,
        resourceName,
        namespace,
        diffs,
        severity,
        detectedAt:    new Date().toISOString(),
        liveState:     fullObject,
        declaredState: gitManifest,
      }),
    }],
  });

  console.log(`[drift-engine] Published to Kafka for AI agent`);
}

// ─── Main ─────────────────────────────────────────────────────────
async function main() {
  await redisClient.connect();
  console.log('[drift-engine] Redis connected');

  await consumer.connect();
  await producer.connect();
  console.log('[drift-engine] Kafka connected');

  await consumer.subscribe({
    topic:         'kubevigil.resource.snapshot',
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const snapshot = JSON.parse(message.value.toString());
      console.log(`[drift-engine] Received snapshot — ${snapshot.resourceKind}/${snapshot.resourceName}`);
      await detectDrift(snapshot);
    },
  });
}

main().catch(err => {
  console.error('[drift-engine] Fatal error:', err);
  process.exit(1);
});