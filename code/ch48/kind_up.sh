#!/usr/bin/env bash
# Create (or delete) Chapter 48's kind cluster "ch48" and load the Spark 4.3.0 image into it.
#   ch48/kind_up.sh [create|delete]
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
TAXI=${CH48_TAXI:-$HOME/spark-masterclass/data/nyc-taxi}
IMAGE=${SPARK_IMAGE:-apache/spark:4.3.0-python3}
if [[ ${1:-create} == delete ]]; then kind delete cluster --name ch48; exit 0; fi
sed -e "s#__TAXI__#$TAXI#" -e "s#__JOBS__#$CH48/jobs#" "$CH48/k8s/kind-ch48.yaml" > /tmp/kind-ch48.$$.yaml
kind create cluster --config /tmp/kind-ch48.$$.yaml --wait 120s
rm -f /tmp/kind-ch48.$$.yaml
kind load docker-image "$IMAGE" --name ch48
kubectl --context kind-ch48 apply -f "$CH48/k8s/rbac.yaml"
kubectl --context kind-ch48 get nodes
