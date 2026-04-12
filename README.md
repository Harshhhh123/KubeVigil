# KubeVigil

**AI-powered GitOps drift detection and autonomous root cause analysis for Kubernetes.**

KubeVigil continuously compares your live cluster state against your Git repository — the source of truth — and when it finds a difference, it doesn't just tell you something drifted. It tells you *when* it happened, *who* caused it, *what* the blast radius is across dependent services, and opens a remediation PR automatically.

> Built for platform engineers and SRE teams who are tired of ArgoCD's yellow "OutOfSync" badge that nobody looks at.

---

## The problem

Every Kubernetes cluster drifts from its Git-declared state. A developer scales a deployment manually during an incident and forgets to revert. Someone edits a ConfigMap directly in prod to debug an issue. An automation script modifies resource limits without updating the manifests. These changes are invisible — no alerts fire, no dashboards change — until something breaks.

Existing tools tell you *that* drift happened. KubeVigil tells you *everything about why*.

---

## What KubeVigil does

- **Continuous state snapshotting** — polls the Kubernetes API server every 60 seconds and snapshots every resource (Deployments, ConfigMaps, Secrets, Services, Ingresses, RBAC) into a time-series store
- **Git-vs-live diff engine** — compares live cluster state against declared manifests in your connected Git repository, field by field
- **AI root cause analysis** — an LangChain agent correlates the drift event with kubectl audit logs, CloudTrail events, and Git commit history to reconstruct exactly what happened and why
- **Blast radius analysis** — maps service dependency graphs to show which upstream and downstream services are affected by the drifted resource
- **Automated remediation PRs** — generates the exact manifest change to restore sync, opens a GitHub PR, and assigns it to the service owner
- **Slack + PagerDuty alerts** — posts a structured drift report with timeline, root cause, and blast radius to your incident channel
- **MERN dashboard** — real-time drift map showing all resources, their sync status, drift history, and AI-generated explanations

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────┐
│                    Data sources                          │
│  K8s API Server · kubectl audit logs · CloudTrail · Git │
└────────────────────────┬────────────────────────────────┘
                         │
                    Kafka topics
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
                        (LangChain + GPT-4o)
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼
             GitHub PR        Slack alert    Dashboard event
             (remediation)    (report)       (real-time)
```

---

## Tech stack

### Infrastructure & Kubernetes
| Tool | Purpose |
|------|---------|
| **AWS EKS** | Managed Kubernetes cluster — what KubeVigil monitors |
| **Terraform** | Provision EKS, VPC, IAM roles, RDS, MSK (Kafka) |
| **AWS MSK** | Managed Kafka — event backbone for all state change events |
| **AWS CloudTrail** | Audit log source for API-level actions on AWS resources |
| **kubectl audit logs** | K8s-level audit trail — who ran what kubectl command and when |
| **ArgoCD** | GitOps operator — KubeVigil reads ArgoCD sync status as one of its signals |
| **Helm** | Package KubeVigil itself as a Helm chart for easy installation |

### Backend — drift detection engine
| Tool | Purpose |
|------|---------|
| **Node.js** | Core backend runtime for the drift detection service |
| **@kubernetes/client-node** | Official K8s JS client — used to Watch the API server for resource changes |
| **KafkaJS** | Produce and consume drift events on MSK topics |
| **PostgreSQL (AWS RDS)** | Time-series store for snapshots + drift history |
| **Redis (ElastiCache)** | Cache last-known-good state for each resource for fast diffing |
| **deep-diff** | Field-level JSON diffing between live state and Git-declared state |
| **octokit** | GitHub API client — fetch manifests from repo, open remediation PRs |

### AI root cause agent
| Tool | Purpose |
|------|---------|
| **LangChain (JS)** | Agent orchestration framework |
| **GPT-4o** | LLM for root cause reasoning, blast radius explanation, PR description generation |
| **LlamaIndex** | RAG over runbooks, past incident reports, and service dependency docs |
| **Pinecone** | Vector store for embedded runbooks and incident history |
| **custom tools** | LangChain tools: `get_audit_logs`, `get_git_history`, `get_dependency_graph`, `open_github_pr`, `post_slack_alert` |

### CI/CD & DevOps
| Tool | Purpose |
|------|---------|
| **GitHub Actions** | CI pipeline — lint, test, build Docker image, push to ECR |
| **ArgoCD** | CD — deploys KubeVigil itself to EKS via GitOps |
| **AWS ECR** | Container registry for KubeVigil Docker images |
| **Docker** | Containerise each microservice (watcher, drift-engine, ai-agent, api) |
| **Prometheus + Grafana** | Monitor KubeVigil's own health — lag, drift detection latency, agent response time |
| **AWS Secrets Manager** | Store all API keys, DB credentials, LLM keys — never in env files |

### Frontend — MERN dashboard
| Tool | Purpose |
|------|---------|
| **React** | SPA dashboard — real-time drift map, resource explorer, incident timeline |
| **Socket.IO** | Push drift events to dashboard in real time without polling |
| **Recharts** | Drift frequency charts, resource health over time |
| **React Flow** | Interactive service dependency graph showing blast radius visually |
| **Express.js** | REST + WebSocket API server |
| **MongoDB Atlas** | Store dashboard state, user preferences, alert configs, drift comments |
| **TailwindCSS** | Styling |

### Observability
| Tool | Purpose |
|------|---------|
| **OpenTelemetry** | Distributed tracing across all KubeVigil microservices |
| **AWS CloudWatch** | Centralised log aggregation |
| **Prometheus** | Metrics — drift detection lag, Kafka consumer lag, agent latency |
| **Grafana** | Dashboards for KubeVigil's own operational health |

---

## Repository structure

```
kubevigil/
├── apps/
│   ├── watcher/          # K8s API Watch client — streams resource changes to Kafka
│   ├── drift-engine/     # Consumes Kafka, diffs against Git, writes drift events
│   ├── ai-agent/         # LangChain agent — root cause analysis + remediation
│   └── api/              # Express REST + WebSocket API for dashboard
├── dashboard/            # React MERN frontend
├── infra/
│   ├── terraform/        # EKS, RDS, MSK, ElastiCache, IAM, VPC
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

## Key Kafka topics

```
kubevigil.resource.snapshot     # Full resource snapshots every 60s
kubevigil.drift.detected        # Drift events with field-level diff
kubevigil.audit.ingested        # Processed kubectl + CloudTrail audit events
kubevigil.remediation.created   # Remediation PR events
kubevigil.alert.dispatched      # Slack / PagerDuty alert events
```

---

## Drift detection logic

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

## AI agent tools

The LangChain agent is given these tools and autonomously decides which to call:

```javascript
tools = [
  get_kubectl_audit_logs(resource, timerange),   // Who ran what kubectl command
  get_cloudtrail_events(resource, timerange),    // AWS API level actions
  get_git_commit_history(file_path, timerange),  // What changed in Git and when
  get_dependency_graph(service_name),            // Which services depend on this
  search_incident_history(query),                // RAG over past incidents
  open_github_pr(branch, manifest, description), // Create remediation PR
  post_slack_alert(channel, report),             // Send structured alert
]
```

---

## AI-generated drift report example

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

## Getting started

### Prerequisites
- AWS account with EKS access
- `kubectl`, `terraform`, `helm` installed
- GitHub token with repo + PR permissions
- OpenAI API key

### Local development

```bash
git clone https://github.com/yourusername/kubevigil
cd kubevigil

# Spin up local K8s cluster
kind create cluster --name kubevigil-dev

# Start all services
docker compose up

# Dashboard
cd dashboard && npm install && npm run dev
```

### Deploy to AWS EKS

```bash
cd infra/terraform
terraform init
terraform apply

# Install KubeVigil on your cluster via Helm
helm repo add kubevigil https://yourusername.github.io/kubevigil
helm install kubevigil kubevigil/kubevigil \
  --set openai.apiKey=$OPENAI_API_KEY \
  --set github.token=$GITHUB_TOKEN \
  --set slack.webhookUrl=$SLACK_WEBHOOK
```

---

## Roadmap

- [ ] K8s Operator + CRD — `DriftPolicy` custom resource for per-namespace drift rules
- [ ] Multi-cluster support — monitor 10+ clusters from a single KubeVigil instance
- [ ] Predictive drift — ML model to predict which resources are likely to drift based on historical patterns
- [ ] Auto-remediation mode — merge PR automatically for low-severity, high-confidence drifts
- [ ] RBAC drift detection — specifically track permission escalations as a security signal
- [ ] Compliance reports — SOC 2 / ISO 27001 drift audit exports

---

## Why KubeVigil exists

ArgoCD tells you a resource is out of sync. It does not tell you why, when, who, or what broke because of it. KubeVigil fills that gap with an AI layer that turns a yellow badge into an actionable incident report in under 60 seconds.

---

## License

MIT
