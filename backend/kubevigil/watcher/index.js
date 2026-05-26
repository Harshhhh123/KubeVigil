import * as k8s from '@kubernetes/client-node';
import { Kafka } from 'kafkajs';
// ─── Kubernetes setup ────────────────────────────────────────────
// KubeConfig reads your kubeconfig file (~/.kube/config)
// This is what kubectl uses too — same config, same cluster
const kc = new k8s.KubeConfig();
kc.loadFromDefault();

const watch = new k8s.Watch(kc);

// ─── Kafka setup ─────────────────────────────────────────────────
// When running inside the cluster, we reach Kafka via its service name
// kafka.kubevigil.svc.cluster.local is the internal DNS name
// Kubernetes automatically creates DNS entries for every service
const kafka = new Kafka({
  clientId: 'kubevigil-watcher',
  brokers: [process.env.KAFKA_BROKER || 'kafka.kubevigil.svc.cluster.local:9092'],
});

const producer = kafka.producer();

// ─── Resources to watch ──────────────────────────────────────────
// These are all the Kubernetes resource types we care about
// Any change to any of these triggers a snapshot
const WATCH_RESOURCES = [
  { group: '/apis/apps/v1',               resource: 'deployments' },
  { group: '/api/v1',                     resource: 'configmaps'  },
  { group: '/api/v1',                     resource: 'services'    },
  { group: '/apis/networking.k8s.io/v1',  resource: 'ingresses'   },
];

// ─── Main watch function ──────────────────────────────────────────
async function watchResource({ group, resource }) {
  const path = `${group}/namespaces/default/watch/${resource}`;  // watch default namespace only for now
  // wait for the issue to
  console.log(`[watcher] Starting the watch on the ${resource}...`);

  watch.watch(
    `${group}/namespaces/default/${resource}`,
    {},
    async (eventType, obj) => {
      // eventType is ADDED, MODIFIED, or DELETED
      // obj is the full Kubernetes object — metadata, spec, status, everything
      if (eventType !== 'MODIFIED' && eventType !== 'ADDED') return;

      const snapshot = {
        eventType,
        resourceKind:  obj.kind,
        resourceName:  obj.metadata.name,
        namespace:     obj.metadata.namespace,
        resourceVersion: obj.metadata.resourceVersion,
        timestamp:     new Date().toISOString(),
        fullObject:    obj,   // complete resource state — spec, status, labels, everything
      };

      console.log(`[watcher] ${eventType} detected on ${obj.kind}/${obj.metadata.name}`);

      // Publish to Kafka
      await producer.send({
        topic: 'kubevigil.resource.snapshot',
        messages: [
          {
            key:   `${obj.metadata.namespace}/${obj.kind}/${obj.metadata.name}`,
            value: JSON.stringify(snapshot),
          },
        ],
      });

      console.log(`[watcher] Published snapshot for ${obj.kind}/${obj.metadata.name} to Kafka`);
    },
    (err) => {
      // Watch stream closed — restart it after 5 seconds
      if (err) console.error(`[watcher] Watch error on ${resource}:`, err.message);
      console.log(`[watcher] Restarting watch on ${resource} in 5s...`);
      setTimeout(() => watchResource({ group, resource }), 5000);
    }
  );
}

// ─── Entry point ─────────────────────────────────────────────────
async function main() {
  console.log('[watcher] Connecting to Kafka...');
  await producer.connect();
  console.log('[watcher] Kafka connected. Starting watches...');

  // Start watching all resource types simultaneously
  await Promise.all(WATCH_RESOURCES.map(watchResource));

  console.log('[watcher] All watches active. Listening for changes...');
}

main().catch((err) => {
  console.error('[watcher] Fatal error:', err);
  process.exit(1);
});