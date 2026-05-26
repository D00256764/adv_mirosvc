#!/usr/bin/env bash
set -euo pipefail

# Simple checks for Istio features in the 'microsvc' namespace.
# Requires: kubectl, istioctl, curl, jq

NS=microsvc
INGRESS_NS=istio-system
INGRESS_SVC=istio-ingressgateway
PORT=8080

function port_forward_ingress() {
  kubectl -n ${INGRESS_NS} port-forward svc/${INGRESS_SVC} ${PORT}:80 >/dev/null 2>&1 &
  PF_PID=$!
  # wait for forward to be ready
  sleep 2
}

function cleanup() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill ${PF_PID} >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Port-forwarding ingressgateway to localhost:${PORT}"
port_forward_ingress

BASE_URL="http://localhost:${PORT}"

echo "== mTLS check =="
istioctl authn tls-check -n ${NS} || { echo "istioctl tls-check failed"; exit 1; }

echo "== Basic ingress routing check =="
for path in /order/health /inventory/health /payment/health; do
  status=$(curl -s -o /dev/null -w "%{http_code}" ${BASE_URL}${path} || true)
  echo "  ${path} -> ${status}"
done

echo "== Canary routing sample (order-service) =="
v1=0
v2=0
for i in {1..50}; do
  # try to discover which version served the request via header or body
  out=$(curl -s -I ${BASE_URL}/order/ || true)
  if echo "$out" | grep -qi "version: v2"; then
    ((v2++))
  else
    ((v1++))
  fi
done
echo "  Observed v1: ${v1}, v2: ${v2} (expected ~80/20)"

echo "== Rate-limit quick test (order-service) =="
codes=$(for i in {1..150}; do curl -s -o /dev/null -w "%{http_code} " ${BASE_URL}/order/ || echo "000 "; done)
echo "  status codes sample: ${codes}
"

echo "== Fault injection observation (inventory-service) =="
slow=0
errors=0
for i in {1..40}; do
  t=$(curl -s -o /dev/null -w "%{time_total} %{http_code}" ${BASE_URL}/inventory/ || echo "10 000")
  code=$(echo $t | awk '{print $2}')
  time_sec=$(echo $t | awk '{print $1}')
  if (( $(echo "$time_sec > 3" | bc -l) )); then
    ((slow++))
  fi
  if [[ "$code" == "503" ]]; then
    ((errors++))
  fi
done
echo "  slow responses (>3s): ${slow}, 503 errors: ${errors}"

echo "Checks complete. Review outputs above for expected behavior."
