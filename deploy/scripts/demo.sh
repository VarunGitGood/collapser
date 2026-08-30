#!/usr/bin/env bash
# Drives a burst of identical concurrent requests through the deployed collapser
# and reports how many actually reached the backend.
#
# The backend's own counter is the source of truth here, so the collapse ratio is
# measured at the origin rather than taken from the proxy's self-reported metrics.
set -euo pipefail

CONCURRENCY="${CONCURRENCY:-500}"
NS="${NAMESPACE:-default}"

trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

echo "port-forwarding proxy and backend..."
kubectl -n "$NS" port-forward svc/collapser-proxy 50052:50052 2112:2112 >/dev/null 2>&1 &
kubectl -n "$NS" port-forward svc/hello-backend   8080:8080             >/dev/null 2>&1 &

for _ in $(seq 1 40); do
  curl -sf localhost:2112/health >/dev/null 2>&1 && curl -sf localhost:8080/calls >/dev/null 2>&1 && break
  sleep 0.5
done

curl -sf -X POST localhost:8080/reset >/dev/null 2>&1 || curl -sf localhost:8080/reset >/dev/null

echo "sending $CONCURRENCY concurrent identical requests..."
START=$(date +%s%N)
CONCURRENCY="$CONCURRENCY" go run ./cmd/client
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))

BACKEND_CALLS=$(curl -s localhost:8080/calls | tr -d '[:space:]')

echo
echo "================ RESULT ================"
echo "  client requests : $CONCURRENCY"
echo "  backend calls   : $BACKEND_CALLS   (counted at the backend)"
if [ "${BACKEND_CALLS:-0}" -gt 0 ]; then
  echo "  collapse ratio  : $(awk "BEGIN{printf \"%.1f:1\", $CONCURRENCY/$BACKEND_CALLS}")"
fi
echo "  wall time       : ${ELAPSED_MS}ms"
echo "========================================"
echo
echo "proxy metrics:"
curl -s localhost:2112/metrics | grep -E '^collapser_(requests|collapsed_requests|backend_calls|cache_hits)_total'
