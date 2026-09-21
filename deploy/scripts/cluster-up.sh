#!/usr/bin/env bash
# Creates a local kind cluster. Everything lives inside Docker; `make
# cluster-down` removes all of it.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-collapser}"

command -v kind    >/dev/null || { echo "kind not installed: https://kind.sigs.k8s.io"; exit 1; }
command -v kubectl >/dev/null || { echo "kubectl not installed"; exit 1; }

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "cluster '$CLUSTER' already exists"
else
  echo "creating kind cluster '$CLUSTER'..."
  kind create cluster --name "$CLUSTER" --wait 120s
fi

kubectl cluster-info --context "kind-$CLUSTER" >/dev/null

echo
echo "cluster ready. next: make deploy && make demo"
