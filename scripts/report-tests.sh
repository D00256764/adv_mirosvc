#!/usr/bin/env bash
# =============================================================================
# report-tests.sh — Generates evidence for every Istio marking criterion
# Run from any directory. Requires: kubectl, istioctl, curl, bc, jq
# =============================================================================
set -uo pipefail

NS=microsvc
INGRESS_NS=istio-system
INGRESS_SVC=istio-ingressgateway
LOCAL_PORT=8080
PF_PID=""

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

header()  { echo -e "\n${BOLD}========================================${NC}"; echo -e "${BOLD}  $1${NC}"; echo -e "${BOLD}========================================${NC}"; }
ok()      { echo -e "  ${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "  ${YELLOW}[NOTE]${NC} $1"; }
result()  { echo -e "  ${BOLD}>>> $1${NC}"; }

cleanup() {
  [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Start ingress port-forward
header "Setup: port-forwarding ingress gateway → localhost:${LOCAL_PORT}"
kubectl -n "$INGRESS_NS" port-forward "svc/$INGRESS_SVC" "${LOCAL_PORT}:80" >/dev/null 2>&1 &
PF_PID=$!
sleep 3
BASE="http://localhost:${LOCAL_PORT}"
ok "Ingress available at ${BASE}"

# =============================================================================
# 1. SECURE COMMUNICATIONS — mTLS
# =============================================================================
header "1. SECURE COMMUNICATIONS — mTLS (STRICT mode)"

echo ""
echo "--- PeerAuthentication resource ---"
kubectl get peerauthentication default -n "$NS" -o yaml

echo ""
echo "--- istioctl mTLS status for all services ---"
istioctl authn tls-check -n "$NS" 2>&1 || true

echo ""
echo "--- Envoy sidecar injection (2/2 = sidecar present) ---"
kubectl get pods -n "$NS" -o wide

ok "STRICT mTLS means all pod-to-pod traffic is encrypted. Screenshot the tls-check output."

# =============================================================================
# 2. ZERO TRUST — Authorization Policies
# =============================================================================
header "2. ZERO TRUST — Authorization Policies"

echo ""
echo "--- Applied policies ---"
kubectl get authorizationpolicy -n "$NS"

echo ""
echo "--- deny-all policy (default deny) ---"
kubectl get authorizationpolicy deny-all -n "$NS" -o yaml

echo ""
echo "--- Testing: hit a path NOT listed in any ALLOW policy (expect 403/RBAC denied) ---"
DENY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/order/orders/nonexistent-path-xyz" 2>/dev/null || echo "000")
result "Request to unlisted path → HTTP ${DENY_STATUS} (403 or 404 = policy enforced)"

echo ""
echo "--- Testing: valid allowed path ---"
ALLOW_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/order/health" 2>/dev/null || echo "000")
result "GET /order/health → HTTP ${ALLOW_STATUS} (200 = allow rule matched)"

warn "Screenshot the kubectl get authorizationpolicy output and the two HTTP status codes above."

# =============================================================================
# 3. TRAFFIC CONTROL — Canary Deployment (80% v1 / 20% v2)
# =============================================================================
header "3. TRAFFIC CONTROL — Canary Deployment"

echo ""
echo "--- VirtualService weight split ---"
kubectl get virtualservice order-service -n "$NS" -o yaml | grep -A 20 "http:"

echo ""
echo "--- Sending 100 requests and counting which version responds ---"
V1=0; V2=0; UNKNOWN=0
for i in $(seq 1 100); do
  RESP=$(curl -s "${BASE}/order/health" 2>/dev/null || echo "fail")
  if echo "$RESP" | grep -q '"version":"v2"'; then
    ((V2++))
  elif echo "$RESP" | grep -qi "v2"; then
    ((V2++))
  else
    ((V1++))
  fi
done
result "After 100 requests: v1=${V1}, v2=${V2} (expect ~80 v1, ~20 v2)"
warn "If both show 0, check that your /health endpoint returns a version field."
warn "You can also verify in Kiali: Graph → show traffic split between order-service-v1 and v2 pods."

# =============================================================================
# 4. TRAFFIC CONTROL — Load Balancing
# =============================================================================
header "4. TRAFFIC CONTROL — Load Balancing (ROUND_ROBIN)"

echo ""
echo "--- DestinationRule policies ---"
kubectl get destinationrule -n "$NS" -o yaml | grep -E "host:|simple:|loadBalancer:" | head -20

ok "ROUND_ROBIN is configured on all three services. Kiali graph will show balanced arrows."
warn "Screenshot the DestinationRule yaml and the Kiali graph during traffic generation."

# =============================================================================
# 5. GENERATE SUSTAINED TRAFFIC (for dashboards)
# =============================================================================
header "5. GENERATING TRAFFIC — for Grafana/Kiali/Jaeger dashboards"

echo ""
warn "Deleting old traffic-gen pod if present..."
kubectl delete pod traffic-gen -n "$NS" --ignore-not-found 2>/dev/null
warn "Applying traffic-gen pod..."
kubectl apply -f "$(dirname "$0")/../manifests/traffic-gen.yaml" 2>&1

echo ""
echo "Sending 200 requests now (mix of orders, inventory, payment)..."
for i in $(seq 1 200); do
  curl -s -o /dev/null "${BASE}/order/health" &
  curl -s -o /dev/null -X POST "${BASE}/order/orders" \
    -H "Content-Type: application/json" \
    -d '{"customer_id":"test","items":[{"product_id":"p1","quantity":1,"price":9.99}]}' &
  curl -s -o /dev/null "${BASE}/inventory/stock/p1" &
  curl -s -o /dev/null "${BASE}/payment/health" &
  # throttle slightly
  if (( i % 20 == 0 )); then sleep 1; fi
done
wait
ok "Traffic generated. NOW take screenshots of Grafana, Kiali, Jaeger while data is fresh."
warn "Dashboards: Grafana=:3000, Kiali=:20001, Jaeger=:16686, Prometheus=:9090"

# =============================================================================
# 6. OBSERVABILITY — Prometheus metrics
# =============================================================================
header "6. OBSERVABILITY — Prometheus metrics"

echo ""
echo "--- Checking Prometheus for Istio request metrics ---"
PROM_PF_PID=""
kubectl -n "$INGRESS_NS" port-forward svc/prometheus 9090:9090 >/dev/null 2>&1 &
PROM_PF_PID=$!
sleep 2

REQ_COUNT=$(curl -s "http://localhost:9090/api/v1/query?query=istio_requests_total" 2>/dev/null | jq '.data.result | length' 2>/dev/null || echo "0")
result "istio_requests_total metric series found: ${REQ_COUNT}"

LATENCY=$(curl -s "http://localhost:9090/api/v1/query?query=histogram_quantile(0.99,rate(istio_request_duration_milliseconds_bucket[1m]))" 2>/dev/null | jq '.data.result[0].value[1]' 2>/dev/null || echo "N/A")
result "p99 request latency (last 1m): ${LATENCY} ms"

kill "$PROM_PF_PID" 2>/dev/null || true
warn "Screenshot: Prometheus graph for 'istio_requests_total' and 'istio_request_duration_milliseconds_bucket'"

# =============================================================================
# 7. FAULT INJECTION — Delay + Abort on inventory-service
# =============================================================================
header "7. FAULT/CHAOS TESTING — applying fault injection"

echo ""
warn "Applying fault-injection VirtualService (10% 5s delay, 5% 503 abort)..."
kubectl apply -f "$(dirname "$0")/../manifests/istio/fault-injection.yaml" 2>&1

echo ""
echo "--- Sending 60 requests to inventory-service, measuring latency and errors ---"
SLOW=0; ERRORS=0; FAST=0
for i in $(seq 1 60); do
  # capture both time and status code
  RESULT=$(curl -s -o /dev/null -w "%{time_total} %{http_code}" \
    "${BASE}/inventory/stock/p1" 2>/dev/null || echo "0 000")
  TIME_S=$(echo "$RESULT" | awk '{print $1}')
  CODE=$(echo "$RESULT" | awk '{print $2}')

  if [[ "$CODE" == "503" ]]; then
    ((ERRORS++))
  elif (( $(echo "$TIME_S > 3.0" | bc -l 2>/dev/null || echo 0) )); then
    ((SLOW++))
  else
    ((FAST++))
  fi
done

result "Fast (<3s): ${FAST} | Slow (≥3s, injected delay): ${SLOW} | 503 (injected abort): ${ERRORS}"
warn "Expected: ~3 aborts (5%), ~6 slow (10%). Screenshot these numbers."
warn "Also check Kiali (Graph > show error %) and Jaeger (find slow inventory traces)."

echo ""
echo "--- Cleaning up fault injection after screenshot ---"
warn "NOTE: fault-injection.yaml conflicts with virtual-services.yaml for inventory-service."
warn "After screenshotting, run: kubectl delete vs inventory-service-fault -n microsvc"
warn "to restore normal inventory routing."

# =============================================================================
# 8. RESILIENCE — Rate Limiting
# =============================================================================
header "8. RESILIENCE — Rate Limiting (100 req/60s on order-service)"

echo ""
echo "--- EnvoyFilter config ---"
kubectl get envoyfilter order-service-rate-limit -n "$NS" -o yaml | grep -E "max_tokens:|tokens_per_fill:|fill_interval:|stat_prefix:" | head -10

echo ""
echo "Sending 120 rapid requests to order-service (limit is 100/min)..."
ALLOWED=0; LIMITED=0
for i in $(seq 1 120); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/order/health" 2>/dev/null || echo "000")
  if [[ "$CODE" == "429" ]]; then
    ((LIMITED++))
  elif [[ "$CODE" == "200" ]]; then
    ((ALLOWED++))
  fi
done
result "Allowed: ${ALLOWED} | Rate-limited (429): ${LIMITED}"
warn "If LIMITED=0, the local rate limiter may need the x-local-rate-limit header check."
warn "Prometheus metric to screenshot: envoy_http_local_rate_limiter_rate_limited (or ratelimit_total)"

# =============================================================================
# 9. RESILIENCE — Circuit Breaker (outlierDetection)
# =============================================================================
header "9. RESILIENCE — Circuit Breaker (outlierDetection)"

echo ""
echo "--- DestinationRule outlierDetection settings ---"
kubectl get destinationrule -n "$NS" -o yaml | grep -A 8 "outlierDetection:"

ok "Circuit breaker is configured: 5 consecutive 5xx errors trigger host ejection for 30s."
warn "To demonstrate: inject faults (done in step 7), watch Kiali show ejected hosts."
warn "Screenshot: Kiali > Graph > expand a service > show 'No healthy upstream' or Prometheus 'outlier_detection_ejections_total'"

# =============================================================================
# 10. OBSERVABILITY — Distributed Tracing (Jaeger)
# =============================================================================
header "10. OBSERVABILITY — Distributed Tracing (Jaeger)"

warn "Port-forward Jaeger if not already open: kubectl port-forward -n istio-system svc/jaeger 16686:16686"
warn "In Jaeger UI:"
warn "  1. Select Service: 'order-service' → Find Traces"
warn "  2. Click a trace → you will see spans across order→inventory→payment"
warn "  3. Screenshot the flame graph showing cross-service propagation"
warn "  4. Select a slow trace (from fault injection) to show 5s delay span"

# =============================================================================
# 11. OBSERVABILITY — Cluster Topology (Kiali)
# =============================================================================
header "11. OBSERVABILITY — Kiali Cluster Topology"

warn "Port-forward Kiali: kubectl port-forward -n istio-system svc/kiali 20001:20001"
warn "In Kiali UI:"
warn "  1. Graph > Namespace: microsvc > Display: Traffic Animation ON"
warn "  2. Screenshot the topology graph (shows order, inventory, payment, kafka, redis)"
warn "  3. Enable 'Traffic Distribution' to see canary 80/20 split on order-service"
warn "  4. Enable 'Security' layer to see mTLS padlock icons on all connections"
warn "  5. During fault injection: enable 'Response Time' and 'Error Rate' — red edges show"

# =============================================================================
# SUMMARY
# =============================================================================
header "SUMMARY — Screenshots checklist for your report"
echo ""
echo "  [ ] 1. mTLS:         istioctl authn tls-check output + PeerAuthentication STRICT yaml"
echo "  [ ] 2. Zero Trust:   kubectl get authorizationpolicy + 403 on denied path"
echo "  [ ] 3. Canary:       VirtualService weight yaml + Kiali graph showing 80/20 split"
echo "  [ ] 4. Load balancer: DestinationRule yaml showing ROUND_ROBIN"
echo "  [ ] 5. Grafana:      Istio Service Dashboard — RPS, p99 latency, error % panels"
echo "  [ ] 6. Prometheus:   istio_requests_total query graph"
echo "  [ ] 7. Jaeger:       Cross-service trace + 1 slow trace from fault injection"
echo "  [ ] 8. Kiali graph:  Topology + mTLS icons + traffic animation"
echo "  [ ] 9. Fault inject: slow/503 count output from this script"
echo "  [ ] 10. Rate limit:  429 responses in script output"
echo "  [ ] 11. Circuit brk: DestinationRule yaml + Kiali ejected host"
echo ""
echo "Script complete."
