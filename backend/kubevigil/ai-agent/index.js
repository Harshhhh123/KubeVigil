import 'dotenv/config';
import { Kafka } from 'kafkajs';
import { ChatGroq } from '@langchain/groq';
import { tool } from '@langchain/core/tools';
import { createReactAgent } from '@langchain/langgraph/prebuilt';
import { z } from 'zod';
import { CloudWatchLogsClient, FilterLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
import { Octokit } from '@octokit/rest';
import pg from 'pg';
const { Pool } = pg;

// ─── Clients ─────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'kubevigil-ai-agent',
  brokers: [process.env.KAFKA_BROKER || 'kafka.kubevigil.svc.cluster.local:9092'],
});
const consumer = kafka.consumer({ groupId: 'ai-agent-group' });

const llm = new ChatGroq({
  apiKey: process.env.GROQ_API_KEY,
  model: 'llama3-70b-8192',
  temperature: 0,
});

const cloudwatch = new CloudWatchLogsClient({ region: 'ap-south-1' });

const octokit = new Octokit({ auth: process.env.GITHUB_TOKEN });

const pgPool = new Pool({
  host:     process.env.PG_HOST,
  port:     5432,
  database: process.env.PG_DATABASE || 'kubevigil',
  user:     process.env.PG_USER     || 'postgres',
  password: process.env.PG_PASSWORD || 'kubevigil123',
  ssl: { rejectUnauthorized: false },
});

const GITHUB_OWNER = process.env.GITHUB_OWNER || 'Harshhhh123';
const GITHUB_REPO  = process.env.GITHUB_REPO  || 'KubeVigil';
const LOG_GROUP    = '/aws/eks/kubevigil/cluster';

// ─── Tool 1: CloudWatch Audit Logs ───────────────────────────────
const getAuditLogs = tool(
  async ({ resourceName, minutesBack }) => {
    try {
      const startTime = Date.now() - minutesBack * 60 * 1000;
      const command = new FilterLogEventsCommand({
        logGroupName: LOG_GROUP,
        startTime,
        filterPattern: `"${resourceName}"`,
        limit: 10,
      });
      const response = await cloudwatch.send(command);
      if (!response.events || response.events.length === 0) {
        return `No audit log entries found for ${resourceName} in the last ${minutesBack} minutes.`;
      }
      const logs = response.events.map(e => {
        try {
          const parsed = JSON.parse(e.message);
          return `Time: ${parsed.requestReceivedTimestamp || new Date(e.timestamp).toISOString()} | User: ${parsed.user?.username || 'unknown'} | Action: ${parsed.verb || 'unknown'} | Resource: ${parsed.objectRef?.resource || 'unknown'}/${parsed.objectRef?.name || 'unknown'}`;
        } catch {
          return e.message;
        }
      });
      return logs.join('\n');
    } catch (err) {
      return `CloudWatch query failed: ${err.message}`;
    }
  },
  {
    name: 'get_audit_logs',
    description: 'Get kubectl audit logs from CloudWatch to find who changed a Kubernetes resource and when',
    schema: z.object({
      resourceName: z.string().describe('Name of the Kubernetes resource'),
      minutesBack:  z.number().describe('How many minutes back to search'),
    }),
  }
);

// ─── Tool 2: Dependency Graph ─────────────────────────────────────
const getDependencyGraph = tool(
  async ({ serviceName }) => {
    try {
      const { execSync } = await import('child_process');
      const result = execSync(`kubectl get deployments -n default -o json`).toString();
      const deployments = JSON.parse(result);
      const dependents = deployments.items
        .filter(d => d.metadata.name !== serviceName)
        .map(d => d.metadata.name);
      if (dependents.length === 0) return `No other services found in default namespace.`;
      return `Services that may be affected by ${serviceName} drift:\n${dependents.map(d => `- ${d}`).join('\n')}`;
    } catch (err) {
      return `Could not fetch dependency graph: ${err.message}`;
    }
  },
  {
    name: 'get_dependency_graph',
    description: 'Get list of services running alongside the drifted service to assess blast radius',
    schema: z.object({
      serviceName: z.string().describe('Name of the drifted service'),
    }),
  }
);

// ─── Tool 3: Past Drift History ───────────────────────────────────
const searchPastDrifts = tool(
  async ({ resourceName }) => {
    try {
      const result = await pgPool.query(
        `SELECT severity, detected_at, diff 
         FROM drift_events 
         WHERE resource_name = $1 
         ORDER BY detected_at DESC 
         LIMIT 5`,
        [resourceName]
      );
      if (result.rows.length === 0) return `No past drift history found for ${resourceName}.`;
      const history = result.rows.map(r =>
        `- ${r.severity} drift at ${r.detected_at}`
      ).join('\n');
      return `Past drift history for ${resourceName}:\n${history}`;
    } catch (err) {
      return `Could not query drift history: ${err.message}`;
    }
  },
  {
    name: 'search_past_drifts',
    description: 'Search PostgreSQL for past drift events on this resource to find patterns',
    schema: z.object({
      resourceName: z.string().describe('Name of the Kubernetes resource'),
    }),
  }
);

// ─── Tool 4: Open GitHub PR ───────────────────────────────────────
const openGithubPR = tool(
  async ({ resourceName, namespace, declaredReplicas }) => {
    try {
      const { data: repo } = await octokit.repos.get({ owner: GITHUB_OWNER, repo: GITHUB_REPO });
      const baseSha = repo.default_branch;

      const manifestPath = `k8s/${namespace}/deployment/${resourceName}.yaml`;
      const { data: fileData } = await octokit.repos.getContent({
        owner: GITHUB_OWNER, repo: GITHUB_REPO, path: manifestPath,
      });

      const content = Buffer.from(fileData.content, 'base64').toString('utf8');
      const updated = content.replace(/replicas:\s*\d+/, `replicas: ${declaredReplicas}`);
      const newContent = Buffer.from(updated).toString('base64');

      const branchName = `fix/restore-${resourceName}-${Date.now()}`;
      const { data: ref } = await octokit.git.getRef({
        owner: GITHUB_OWNER, repo: GITHUB_REPO, ref: `heads/${repo.default_branch}`,
      });

      await octokit.git.createRef({
        owner: GITHUB_OWNER, repo: GITHUB_REPO,
        ref: `refs/heads/${branchName}`,
        sha: ref.object.sha,
      });

      await octokit.repos.createOrUpdateFileContents({
        owner: GITHUB_OWNER, repo: GITHUB_REPO,
        path: manifestPath, message: `fix: restore ${resourceName} to declared state`,
        content: newContent, sha: fileData.sha, branch: branchName,
      });

      const { data: pr } = await octokit.pulls.create({
        owner: GITHUB_OWNER, repo: GITHUB_REPO,
        title: `fix: restore ${resourceName} replicas to ${declaredReplicas}`,
        body: `KubeVigil detected drift on ${resourceName}. This PR restores it to the declared state in Git.`,
        head: branchName, base: repo.default_branch,
      });

      return `PR opened: ${pr.html_url}`;
    } catch (err) {
      return `Could not open PR: ${err.message}`;
    }
  },
  {
    name: 'open_github_pr',
    description: 'Open a GitHub PR to restore the drifted resource back to its declared state in Git',
    schema: z.object({
      resourceName:     z.string().describe('Name of the drifted resource'),
      namespace:        z.string().describe('Kubernetes namespace'),
      declaredReplicas: z.number().describe('The correct replica count from Git'),
    }),
  }
);

// ─── Agent runner ─────────────────────────────────────────────────
async function runAgent(driftEvent) {
  const { resourceName, namespace, diffs, severity, declaredState } = driftEvent;
  const declaredReplicas = declaredState?.spec?.replicas || 3;

  const tools = [getAuditLogs, getDependencyGraph, searchPastDrifts, openGithubPR];
  const agent = createReactAgent({ llm, tools });

  const prompt = `
You are KubeVigil, an AI SRE agent. A drift was detected in a Kubernetes cluster.

DRIFT DETAILS:
- Resource: ${resourceName} (${namespace} namespace)
- Severity: ${severity}
- What changed: ${JSON.stringify(diffs, null, 2)}
- Declared replicas in Git: ${declaredReplicas}

Your job:
1. Call get_audit_logs to find who made this change and when (search last 120 minutes)
2. Call get_dependency_graph to find what services are affected
3. Call search_past_drifts to check if this happened before
4. Call open_github_pr to create a remediation PR restoring replicas to ${declaredReplicas}
5. Write a final incident report with: WHAT CHANGED, WHEN, WHO, BLAST RADIUS, HISTORY, REMEDIATION

Be concise and factual.
`;

  console.log(`[ai-agent] Running agent for ${resourceName}...`);

  const result = await agent.invoke({
    messages: [{ role: 'user', content: prompt }],
  });

  const report = result.messages[result.messages.length - 1].content;
  console.log('\n[ai-agent] ═══════════════════════════════');
  console.log('[ai-agent] INCIDENT REPORT:');
  console.log(report);
  console.log('[ai-agent] ═══════════════════════════════\n');

  return report;
}

// ─── Main Kafka consumer ──────────────────────────────────────────
async function main() {
  await consumer.connect();
  console.log('[ai-agent] Kafka connected. Waiting for drift events...');

  await consumer.subscribe({
    topic: 'kubevigil.drift.detected',
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const driftEvent = JSON.parse(message.value.toString());
      console.log(`[ai-agent] Drift event received — ${driftEvent.resourceName}`);
      await runAgent(driftEvent);
    },
  });
}

main().catch(err => {
  console.error('[ai-agent] Fatal error:', err);
  process.exit(1);
});