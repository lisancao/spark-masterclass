#!/usr/bin/env bash
# Install Apache YuniKorn 1.10.0 (Helm chart 1.10.0) into the kind cluster "ch63" with Helm.
#   ch63/yunikorn.sh
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
helm repo add yunikorn https://apache.github.io/yunikorn-release >/dev/null 2>&1 || true
helm repo update yunikorn >/dev/null
helm --kube-context kind-ch63 upgrade --install yunikorn yunikorn/yunikorn \
  --version 1.10.0 --namespace yunikorn --create-namespace \
  -f "$CH48/k8s/yunikorn-values.yaml" --wait --timeout 5m
kubectl --context kind-ch63 -n yunikorn get pods
# The scheduler's REST API and web UI on localhost:19048, for watch_pods.py (stop it with: kill $(cat <file>)).
PF=${CH48_WORK:-$HOME/.cache/spark-masterclass/ch63}/yunikorn-port-forward.pid
mkdir -p "$(dirname "$PF")"; [[ -f $PF ]] && kill "$(cat "$PF")" 2>/dev/null || true
kubectl --context kind-ch63 -n yunikorn port-forward svc/yunikorn-service 19048:9080 >/dev/null 2>&1 &
echo $! > "$PF"
