#!/usr/bin/env bash
# Chapter 70: a local kind cluster for native Spark on Kubernetes (no operator).
#   ch70/k8s/cluster.sh up | down        (from code/)
# Each node mounts the taxi data and code/ch70 from this machine, so pods can use hostPath volumes.
set -euo pipefail
cd "$(dirname "$0")"
NAME=ops80 IMAGE=spark-masterclass/spark:4.3.0-book
DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}/nyc-taxi
CODE=$(cd .. && pwd)
if [[ ${1:-up} == down ]]; then kind delete cluster --name $NAME; exit 0; fi
node() { printf -- '- role: %s\n  extraMounts:\n  - {hostPath: %s, containerPath: /data/nyc-taxi, readOnly: true}\n  - {hostPath: %s, containerPath: /opt/ch70, readOnly: true}\n' "$1" "$DATA" "$CODE"; }
{ echo "kind: Cluster"; echo "apiVersion: kind.x-k8s.io/v1alpha4"; echo "nodes:"
  node control-plane; node worker; node worker; } > /tmp/kind-$NAME.yaml
kind get clusters | grep -qx $NAME || kind create cluster --name $NAME --config /tmp/kind-$NAME.yaml --image kindest/node:v1.37.0
rm -f /tmp/kind-$NAME.yaml
kind load docker-image --name $NAME $IMAGE >/dev/null
kubectl --context kind-$NAME apply -f rbac.yaml >/dev/null
kubectl --context kind-$NAME get nodes
