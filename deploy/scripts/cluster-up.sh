#!/usr/bin/env bash
# Creates a local kind cluster and installs Istio into it.
# Everything lives inside Docker; `make cluster-down` removes all of it.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-collapser}"
ISTIO_VERSION="${ISTIO_VERSION:-1.28.1}"

command -v kind    >/dev/null || { echo "kind not installed: https://kind.sigs.k8s.io"; exit 1; }
command -v kubectl >/dev/null || { echo "kubectl not installed"; exit 1; }

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "cluster '$CLUSTER' already exists"
else
  echo "creating kind cluster '$CLUSTER'..."
  kind create cluster --name "$CLUSTER" --wait 120s
fi

kubectl cluster-info --context "kind-$CLUSTER" >/dev/null

if ! command -v istioctl >/dev/null; then
  echo "istioctl not on PATH; install Istio $ISTIO_VERSION and re-run" >&2
  exit 1
fi

if ! kubectl get ns istio-system >/dev/null 2>&1; then
  echo "installing Istio $ISTIO_VERSION (demo profile)..."
  istioctl install --set profile=demo -y
fi

# Sidecar injection is what puts an Envoy next to each pod; without it the
# Istio manifests apply but do nothing.
kubectl label namespace default istio-injection=enabled --overwrite

echo
echo "cluster ready. next: make deploy && make demo"
