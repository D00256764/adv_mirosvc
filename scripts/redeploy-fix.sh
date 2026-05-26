#!/usr/bin/env bash
# Fix CrashLoop: remove Istio sidecars from Kafka/ZK/Redis, restart apps with new images.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Apply manifests"
kubectl apply -k "${ROOT}/manifests"
kubectl apply -k "${ROOT}/manifests/istio"

echo "==> Remove Istio sidecars from infrastructure (delete pods)"
kubectl delete pod -n microsvc kafka-0 zookeeper-0 \
  redis-order-0 redis-inventory-0 redis-payment-0 \
  --ignore-not-found

echo "==> Restart app deployments (pick up inject:true + new images)"
kubectl rollout restart deployment -n microsvc \
  order-service order-service-v2 inventory-service payment-service

echo "==> Wait for Kafka (no sidecar)"
kubectl wait -n microsvc --for=condition=ready pod/kafka-0 --timeout=300s

echo "==> Wait for app pods"
kubectl wait -n microsvc --for=condition=ready pod \
  -l 'app in (order-service,inventory-service,payment-service)' --timeout=300s

kubectl get pods -n microsvc
