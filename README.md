# KubeVigil

**AI-powered GitOps drift detection and autonomous root cause analysis for Kubernetes.**

KubeVigil continuously compares your live cluster state against your Git repository — the source of truth — and when it finds a difference, it doesn't just tell you something drifted. It tells you *when* it happened, *who* caused it, *what* the blast radius is across dependent services, and opens a remediation PR automatically.

> Built for platform engineers and SRE teams who are tired of ArgoCD's yellow "OutOfSync" badge that nobody looks at.

---

## The Problem

Every Kubernetes cluster drifts from its Git-declared state. A developer scales a deployment manually during an incident and forgets to revert. Someone edits a ConfigMap directly in prod to debug an issue. An automation script modifies resource limits without updating the manifests. These changes are invisible — no alerts fire, no dashboards change — until something breaks.

**ArgoCD tells you *that* drift happened. KubeVigil tells you *everything about why*.**

| | ArgoCD | KubeVigil |
|---|---|---|
| Detects drift | ✅ | ✅ |
| Who caused it | ❌ | ✅ |
| When it happened | ❌ | ✅ |
| Root cause analysis | ❌ | ✅ AI-powered |
| Blast radius | ❌ | ✅ Dependency graph |
| Auto remediation PR | ❌ | ✅ |
| Structured Slack report | Basic | ✅ Full incident report |

KubeVigil is not a replacement for ArgoCD — it's an intelligence layer on top of it. ArgoCD detects the sync signal. KubeVigil does the investigation.

---

## What KubeVigil Does

- **Continuous state snapshotting** — watches the Kubernetes API server in real time and snapshots every resource (Deployments, ConfigMaps, Secrets, Services, Ingresses, RBAC) into a time-series store
- **Git-vs-live diff engine** — compares live cluster state against declared manifests in your connected Git repository, field by field
- **AI root cause analysis** — a LangChain agent correlates the drift event with kubectl audit logs, CloudTrail events, and Git commit history to reconstruct exactly what happened and why
- **Blast radius analysis** — maps service dependency graphs to show which upstream and downstream services are affected by the drifted resource
- **Automated remediation PRs** — generates the exact manifest change to restore sync, opens a GitHub PR, and assigns it to the service owner
- **Slack alerts** — posts a structured drift report with timeline, root cause, and blast radius to your incident channel
- **MERN dashboard** — real-time drift map showing all resources, their sync status, drift history, and AI-generated explanations

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Data Sources                          │
│  K8s API Server · kubectl audit logs · CloudTrail · Git │
└────────────────────────┬────────────────────────────────┘
                         │
                    Kafka topics
                    (pod inside EKS)
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
   State snapshots   Audit events   Git state poller
         │               │               │
         └───────────────┴───────────────┘
                         │
               Drift detection engine
               (Node.js + PostgreSQL)
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        No drift             Drift detected
                                   │
                        AI Root Cause Agent
                        (LangChain + Groq)
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼
             GitHub PR        Slack alert    Dashboard event
             (remediation)    (report)       (real-time)
```

---

## How It Works — Full Flow

```
1. Developer manually scales a deployment in prod via kubectl
          ↓
2. K8s API server registers the change
          ↓
3. KubeVigil watcher picks it up via Watch stream
   → publishes to Kafka topic: kubevigil.resource.snapshot
          ↓
4. Drift engine consumes the event
   → fetches declared manifest from GitHub via octokit
   → deep-diff finds: replicas 3 → 1
   → stores drift event in PostgreSQL with timestamp
   → publishes to kubevigil.drift.detected
          ↓
5. AI agent picks up the drift event
   → calls get_kubectl_audit_logs() → finds who ran the command
   → calls get_cloudtrail_events() → confirms AWS API action
   → calls get_dependency_graph() → finds 3 affected services
   → calls search_incident_history() → finds similar past incident
   → calls open_github_pr() → remediation PR opened automatically
   → calls post_slack_alert() → team notified with full report
          ↓
6. Dashboard receives Socket.IO event
   → drift appears on live map
   → AI-generated report displayed
   → React Flow graph shows blast radius visually
```

---

## Tech Stack

### Infrastructure & Kubernetes

| Tool | Purpose |
|------|---------|
| **AWS EKS** | Managed Kubernetes cluster — what KubeVigil monitors |
| **Terraform** | Provisions EKS, VPC, IAM roles, RDS — full IaC |
| **ArgoCD** | GitOps operator — KubeVigil reads its sync status as an input signal |
| **Helm** | Packages KubeVigil itself for single-command installation on any cluster |
| **AWS CloudTrail** | Audit log source for API-level actions on AWS resources |
| **kubectl audit logs** | K8s-level audit trail — who ran what command and when |

### Backend — Watcher Service

| Tool | Purpose |
|------|---------|
| **Node.js** | Runtime for the watcher microservice |
| **@kubernetes/client-node** | Official K8s JS client — streams resource changes via Watch API |
| **KafkaJS** | Publishes resource change events to Kafka topics |

### Event Backbone

| Tool | Purpose |
|------|---------|
| **Kafka (Bitnami Helm chart, runs as pod inside EKS)** | Message bus — all microservices communicate through Kafka topics, never directly |

### Backend — Drift Detection Engine

| Tool | Purpose |
|------|---------|
| **Node.js** | Runtime for the drift engine microservice |
| **KafkaJS** | Consumes from resource snapshot topic |
| **octokit** | GitHub API client — fetches declared YAML manifests from repo |
| **deep-diff** | Field-level JSON diffing between live state and Git-declared state |
| **PostgreSQL (AWS RDS t3.micro)** | Time-series store for snapshots and drift history |
| **Redis (runs as pod inside EKS)** | Caches last-known-good state for fast diffing without hitting DB |

### AI Root Cause Agent

| Tool | Purpose |
|------|---------|
| **LangChain (JS)** | Agent orchestration — decides which tools to call and in what order |
| **Groq** | Free LLM for root cause reasoning, blast radius explanation, PR description |
| **LlamaIndex** | RAG over runbooks and past incident reports |
| **Pinecone** | Vector store for embedded runbooks and incident history |

**Custom LangChain tools the agent calls autonomously:**

```javascript
tools = [
  get_kubectl_audit_logs(resource, timerange),   // Who ran what kubectl command
  get_cloudtrail_events(resource, timerange),    // AWS API-level actions
  get_git_commit_history(file_path, timerange),  // What changed in Git and when
  get_dependency_graph(service_name),            // Which services depend on this
  search_incident_history(query),                // RAG search over past incidents
  open_github_pr(branch, manifest, description), // Create remediation PR
  post_slack_alert(channel, report),             // Send structured alert
]
```

### API & Frontend — MERN Dashboard

| Tool | Purpose |
|------|---------|
| **Express.js** | REST + WebSocket API server |
| **Socket.IO** | Pushes drift events to dashboard in real time |
| **MongoDB Atlas** | Stores dashboard state, user preferences, alert configs, drift comments |
| **React** | SPA dashboard — real-time drift map, resource explorer, incident timeline |
| **React Flow** | Interactive service dependency graph showing blast radius visually |
| **Recharts** | Drift frequency charts, resource health over time |
| **TailwindCSS** | Styling |

### CI/CD

| Tool | Purpose |
|------|---------|
| **GitHub Actions** | CI pipeline — lint, test, build Docker image, push to ECR |
| **AWS ECR** | Container registry for KubeVigil Docker images |
| **ArgoCD** | CD — deploys KubeVigil itself to EKS via GitOps |
| **Docker** | Each microservice (watcher, drift-engine, ai-agent, api) runs as its own container |

### Observability & Security

| Tool | Purpose |
|------|---------|
| **Prometheus** | Metrics — drift detection lag, Kafka consumer lag, agent response time |
| **Grafana** | Dashboards for KubeVigil's own operational health |
| **OpenTelemetry** | Distributed tracing across all four microservices |
| **AWS CloudWatch** | Centralised log aggregation |
| **AWS Secrets Manager** | Stores all API keys and credentials — never in env files or code |

---

## Repository Structure

```
kubevigil/
├── apps/
│   ├── watcher/          # K8s API Watch client — streams resource changes to Kafka
│   ├── drift-engine/     # Consumes Kafka, diffs against Git, writes drift events
│   ├── ai-agent/         # LangChain agent — root cause analysis + remediation
│   └── api/              # Express REST + WebSocket API for dashboard
├── dashboard/            # React MERN frontend
├── infra/
│   ├── terraform/        # EKS, RDS, IAM, VPC
│   └── helm/             # KubeVigil Helm chart for self-deployment
├── k8s/                  # ArgoCD ApplicationSet manifests
├── .github/
│   └── workflows/        # CI/CD pipelines
└── docs/
    ├── architecture.md
    ├── local-setup.md
    └── runbooks/         # Indexed by LlamaIndex for RAG
```

---

## Key Kafka Topics

```
kubevigil.resource.snapshot     # Full resource snapshots — live cluster state
kubevigil.drift.detected        # Drift events with field-level diff
kubevigil.audit.ingested        # Processed kubectl + CloudTrail audit events
kubevigil.remediation.created   # Remediation PR opened events
kubevigil.alert.dispatched      # Slack alert dispatched events
```

---

## Drift Detection Logic

```
for each resource in live_cluster:
  git_state   = fetch_from_git(resource.kind, resource.name, resource.namespace)
  live_state  = fetch_from_k8s_api(resource)
  diff        = deep_diff(git_state, live_state)

  if diff is not empty:
    drift_event = {
      resource:    resource.name,
      kind:        resource.kind,
      namespace:   resource.namespace,
      diff:        diff,
      detected_at: now(),
      severity:    classify_severity(diff)
    }
    publish to kubevigil.drift.detected
```

---

## AI-Generated Drift Report Example

```
DRIFT DETECTED — production/payments-api Deployment
Detected at: 2024-03-15 03:47 IST

WHAT CHANGED
  replicas: 3 → 1
  resources.limits.memory: 512Mi → 128Mi

WHEN IT HAPPENED
  2024-03-15 02:31 IST — 76 minutes before detection

ROOT CAUSE
  During incident INC-7821 (payment gateway timeout), on-call engineer
  manually scaled the deployment down to 1 replica and reduced memory
  limits via kubectl to isolate the issue. The incident was resolved at
  02:58 IST but the manual changes were not reverted.

  Audit log: kubectl scale deployment payments-api --replicas=1
  Executed by: rahul@company.com via kubectl (IP: 10.0.4.22)

BLAST RADIUS
  3 services depend on payments-api:
  → checkout-service (HIGH — direct dependency, currently degraded)
  → order-service (MEDIUM — async dependency)
  → analytics-pipeline (LOW — read-only, 15min delay acceptable)

REMEDIATION
  PR #847 opened → restores replicas: 3, memory: 512Mi
  Assigned to: rahul@company.com (service owner)
  Estimated restore time: ~2 min after merge
```

---
