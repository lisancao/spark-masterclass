#!/usr/bin/env bash
# Install Apache YuniKorn 1.10.0 into the kind cluster "ch48" with Helm.
#   ch48/yunikorn.sh
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
helm repo add yunikorn https://apache.github.io/yunikorn-release >/dev/null 2>&1 || true
helm repo update yunikorn >/dev/null
helm --kube-context kind-ch48 upgrade --install yunikorn yunikorn/yunikorn \
  --version 1.10.0 --namespace yunikorn --create-namespace \
  -f "$CH48/k8s/yunikorn-values.yaml" --wait --timeout 5m
kubectl --context kind-ch48 -n yunikorn get pods
