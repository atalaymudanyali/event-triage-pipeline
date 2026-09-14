#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="triage-pipeline"
IMAGE_NAME="triage-pipeline:latest"
NAMESPACE="triage-pipeline"

echo "=== V4: Kubernetes Setup ==="

# 1. Build the Docker image
echo "[1/6] Building Docker image..."
docker build -t "$IMAGE_NAME" .

# 2. Create kind cluster (skip if exists)
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "[2/6] Cluster '$CLUSTER_NAME' already exists, skipping creation."
else
  echo "[2/6] Creating kind cluster..."
  kind create cluster --name "$CLUSTER_NAME" --config k8s/kind-config.yaml
fi

# 3. Load image into kind
echo "[3/6] Loading image into kind..."
kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"

# 4. Apply namespace and manifests
echo "[4/6] Applying manifests..."
kubectl apply -f k8s/namespace.yaml

# Create secret from .env file
if [ ! -f .env ]; then
  echo "ERROR: .env file not found. Copy .env.example and add your GEMINI_API_KEY."
  exit 1
fi

GEMINI_KEY=$(grep -E "^GEMINI_API_KEY=" .env | cut -d'=' -f2-)
PG_PASS=$(grep -E "^POSTGRES_PASSWORD=" .env | cut -d'=' -f2- || echo "triage")

if [ -z "$PG_PASS" ]; then
  PG_PASS="triage"
fi

kubectl create secret generic triage-secrets \
  --namespace "$NAMESPACE" \
  --from-literal=GEMINI_API_KEY="$GEMINI_KEY" \
  --from-literal=POSTGRES_PASSWORD="$PG_PASS" \
  --dry-run=client -o yaml | kubectl apply -f -

# Create Grafana dashboard ConfigMap from file
kubectl create configmap grafana-dashboard-json \
  --namespace "$NAMESPACE" \
  --from-file=triage-pipeline.json=monitoring/grafana/dashboards/triage-pipeline.json \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redpanda.yaml
kubectl apply -f k8s/redpanda-console.yaml
kubectl apply -f k8s/prometheus.yaml
kubectl apply -f k8s/grafana.yaml
kubectl apply -f k8s/triage-api.yaml
kubectl apply -f k8s/triage-consumer.yaml

# 5. Wait for pods
echo "[5/6] Waiting for pods to be ready..."
kubectl wait --namespace "$NAMESPACE" \
  --for=condition=ready pod \
  --selector=app=postgres \
  --timeout=120s

kubectl wait --namespace "$NAMESPACE" \
  --for=condition=ready pod \
  --selector=app=redpanda \
  --timeout=120s

kubectl wait --namespace "$NAMESPACE" \
  --for=condition=ready pod \
  --selector=app=triage-api \
  --timeout=120s

# 6. Show status
echo "[6/6] Cluster ready!"
echo ""
kubectl get pods -n "$NAMESPACE"
echo ""
echo "Services:"
echo "  API + Frontend:    http://localhost:8000"
echo "  Grafana:           http://localhost:3000"
echo "  Prometheus:        http://localhost:9090"
echo "  Redpanda Console:  http://localhost:8090"
