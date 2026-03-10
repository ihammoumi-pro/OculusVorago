# OculusVorago — Kubernetes Deployment Guide

This directory contains **only** the application-specific manifests for the
**OculusVorago ETL Engine**.

Shared platform infrastructure (Namespace, Ingress, stateful data-store Helm
charts) is managed by **OculusCodex**. Do not add those resources here.

---

## Directory Structure

```
infra/k8s/vorago/
├── secret.yaml      # ETL-specific Secret  (Kafka credentials)
├── configmap.yaml   # ETL-specific ConfigMap (Kafka topic, log level)
├── deployment.yaml  # Long-running streaming ETL worker
├── jobs.yaml        # One-shot batch Job    (e.g., large CSV processing)
└── cronjobs.yaml    # Scheduled CronJob     (e.g., nightly ingestion)
```

---

## Prerequisites

- Kubernetes `oculus` namespace already exists (provisioned by OculusCodex).
- Kafka is reachable at the address stored in `vorago-secret`.
- `kubectl` is configured with access to the target cluster.

---

## Step 1 — Populate the Secret

**Never store real credentials in version control.**
Create the Secret before applying the manifests:

```bash
kubectl create secret generic vorago-secret \
  --namespace oculus \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS='kafka-svc.oculus.svc.cluster.local:9092'
```

Or use the [External Secrets Operator](https://external-secrets.io/) to sync
credentials from AWS Secrets Manager, HashiCorp Vault, or Azure Key Vault.

---

## Step 2 — Apply the ConfigMap

```bash
kubectl apply -f infra/k8s/vorago/configmap.yaml
```

---

## Step 3 — Deploy the Vorago Workload

### Long-running Streaming Worker

Used when Vorago continuously ingests from a live API source.

```bash
kubectl apply -f infra/k8s/vorago/deployment.yaml
```

### One-shot Batch Job (e.g., process a CSV file)

Edit `jobs.yaml` to set the correct `--source`, `--config`, and PVC name,
then:

```bash
kubectl apply -f infra/k8s/vorago/jobs.yaml
kubectl logs -n oculus -l app.kubernetes.io/component=etl-batch -f
```

### Scheduled CronJob (nightly ingestion)

```bash
kubectl apply -f infra/k8s/vorago/cronjobs.yaml
```

Trigger a manual run of the CronJob immediately (for testing):

```bash
kubectl create job -n oculus vorago-manual-run \
  --from=cronjob/vorago-scheduled-ingest
```

---

## Verify

```bash
# All Vorago pods running
kubectl get pods -n oculus -l app.kubernetes.io/name=vorago

# Check Job status
kubectl get jobs -n oculus

# Stream logs
kubectl logs -n oculus -l app.kubernetes.io/name=vorago -f
```

---

## Image Build & Push

Build and push OculusVorago from the repo root:

```bash
docker build -t ghcr.io/ihammoumi-pro/oculus-vorago:latest .
docker push  ghcr.io/ihammoumi-pro/oculus-vorago:latest
```

For production deployments, pin a specific version tag instead of `latest`.

---

## Security Notes

- The `secret.yaml` template uses `CHANGE_ME` placeholders — never apply it
  with real values directly from source control; use `kubectl create secret`
  or an external secrets operator instead.
- All containers run as non-root (`runAsUser: 1001`,
  `allowPrivilegeEscalation: false`).
- All application logs go to `stdout`/`stderr` (Twelve-Factor App compliance).
