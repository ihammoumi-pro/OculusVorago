# Oculus Platform — Kubernetes Deployment Guide

This directory contains all Kubernetes manifests and Helm value overrides needed
to deploy the complete Oculus intelligence platform into a Kubernetes cluster.

---

## Directory Structure

```
infra/
├── k8s/                         # Kubernetes manifests (custom apps)
│   ├── 00-namespace.yaml        # "oculus" namespace
│   ├── 01-secrets.yaml          # Secret templates (populate before apply)
│   ├── 02-configmap.yaml        # Non-sensitive platform config
│   ├── aperio/
│   │   └── deployment.yaml      # OculusAperio (Next.js frontend) + Service
│   ├── ontologia/
│   │   ├── api-deployment.yaml  # OculusOntologia API + Service + HPA
│   │   └── consumer-deployment.yaml  # Kafka consumer worker + HPA
│   ├── vorago/
│   │   ├── streaming-deployment.yaml  # Long-running streaming ETL
│   │   ├── batch-job.yaml            # One-shot batch Job (e.g., CSV)
│   │   └── cronjob.yaml             # Scheduled recurring ingestion
│   └── ingress.yaml             # NGINX Ingress (external traffic routing)
└── helm-values/                 # Helm overrides for stateful dependencies
    ├── postgresql-values.yaml   # PostgreSQL + PostGIS (Bitnami chart)
    ├── neo4j-values.yaml        # Neo4j (official chart)
    └── kafka-values.yaml        # Kafka KRaft (Bitnami chart)
```

---

## Prerequisites

- Kubernetes cluster (1.27+) with `kubectl` configured
- [Helm 3](https://helm.sh/docs/intro/install/) installed locally
- Container images built and pushed to your registry
- A default `StorageClass` available in the cluster for PVCs

---

## Step 1 — Install the NGINX Ingress Controller

```bash
helm upgrade --install ingress-nginx ingress-nginx \
  --repo https://kubernetes.github.io/ingress-nginx \
  --namespace ingress-nginx \
  --create-namespace \
  --set controller.replicaCount=2
```

---

## Step 2 — Create the Namespace

```bash
kubectl apply -f infra/k8s/00-namespace.yaml
```

---

## Step 3 — Populate Secrets

**Never store real credentials in version control.** The `01-secrets.yaml` file
contains `CHANGE_ME` placeholders. Replace them before applying, or use one of
the approaches below:

### Option A — `kubectl create secret` (simple)

```bash
kubectl create secret generic oculus-db-secret \
  --namespace oculus \
  --from-literal=POSTGRES_USER='oculus_user' \
  --from-literal=POSTGRES_PASSWORD='<your-strong-password>' \
  --from-literal=POSTGRES_DB='oculusdb' \
  --from-literal=DATABASE_URL='postgresql://oculus_user:<password>@postgres-svc.oculus.svc.cluster.local:5432/oculusdb'

kubectl create secret generic oculus-neo4j-secret \
  --namespace oculus \
  --from-literal=NEO4J_URI='bolt://neo4j-svc.oculus.svc.cluster.local:7687' \
  --from-literal=NEO4J_USER='neo4j' \
  --from-literal=NEO4J_PASSWORD='<your-strong-password>'

kubectl create secret generic oculus-kafka-secret \
  --namespace oculus \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS='kafka-svc.oculus.svc.cluster.local:9092'
```

### Option B — External Secrets Operator (recommended for production)

Use [External Secrets Operator](https://external-secrets.io/) to sync secrets
from AWS Secrets Manager, HashiCorp Vault, Azure Key Vault, or GCP Secret Manager
into Kubernetes Secrets automatically.

---

## Step 4 — Install Stateful Dependencies via Helm

Add the required Helm repositories:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add neo4j   https://helm.neo4j.com/neo4j
helm repo update
```

### PostgreSQL with PostGIS

```bash
helm upgrade --install postgres bitnami/postgresql \
  --namespace oculus \
  --version ">=15.5.0" \
  -f infra/helm-values/postgresql-values.yaml
```

### Neo4j

```bash
helm upgrade --install neo4j neo4j/neo4j \
  --namespace oculus \
  --version ">=5.0.0" \
  -f infra/helm-values/neo4j-values.yaml
```

### Kafka (KRaft — no Zookeeper)

```bash
helm upgrade --install kafka bitnami/kafka \
  --namespace oculus \
  --version ">=26.0.0" \
  -f infra/helm-values/kafka-values.yaml
```

Wait for all stateful pods to become `Ready` before proceeding:

```bash
kubectl get pods -n oculus -w
```

---

## Step 5 — Apply ConfigMap

```bash
kubectl apply -f infra/k8s/02-configmap.yaml
```

---

## Step 6 — Deploy Custom Applications

```bash
# OculusAperio (Frontend)
kubectl apply -f infra/k8s/aperio/deployment.yaml

# OculusOntologia (Backend API + Consumer)
kubectl apply -f infra/k8s/ontologia/api-deployment.yaml
kubectl apply -f infra/k8s/ontologia/consumer-deployment.yaml

# Ingress routing
kubectl apply -f infra/k8s/ingress.yaml
```

---

## Step 7 — Run Vorago ETL Pipelines

### One-shot batch Job (e.g., process a CSV file)

Edit `infra/k8s/vorago/batch-job.yaml` to set the correct `--source`,
`--config`, and PVC name, then:

```bash
kubectl apply -f infra/k8s/vorago/batch-job.yaml
kubectl logs -n oculus -l app=vorago-batch -f
```

### Scheduled CronJob (nightly ingestion)

```bash
kubectl apply -f infra/k8s/vorago/cronjob.yaml
```

### Continuous streaming Deployment

```bash
kubectl apply -f infra/k8s/vorago/streaming-deployment.yaml
```

---

## Step 8 — Verify the Deployment

```bash
# Check all pods are Running / Completed
kubectl get pods -n oculus

# Check HPA status
kubectl get hpa -n oculus

# Check Ingress and external IP
kubectl get ingress -n oculus
```

---

## Auto-Scaling (HPA)

HorizontalPodAutoscalers are bundled with the OculusOntologia manifests:

| Target                | Min Replicas | Max Replicas | CPU Trigger |
|-----------------------|:------------:|:------------:|:-----------:|
| ontologia-api         | 2            | 10           | 70 %        |
| ontologia-consumer    | 2            | 8            | 70 %        |

> **Note:** The Kubernetes Metrics Server must be installed for HPA to work:
> ```bash
> kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
> ```

---

## Security Notes

- All Kubernetes `Secret` objects are created **outside** of version control
  (see Step 3 above). Never commit real credentials to this repo.
- All custom containers run as non-root (`runAsUser: 1001`).
- `allowPrivilegeEscalation: false` is set on all containers.
- All application logs go to `stdout`/`stderr` (Twelve-Factor App compliance).
- No application state is stored on local pod filesystems; all persistent data
  lives in PostgreSQL (PVC), Neo4j (PVC), or Kafka (PVC).

---

## Image Build & Push

Build and push OculusVorago from the repo root:

```bash
docker build -t ghcr.io/ihammoumi-pro/oculus-vorago:latest .
docker push  ghcr.io/ihammoumi-pro/oculus-vorago:latest
```

For production deployments, pin a specific version tag instead of `latest`.
