#!/usr/bin/env bash
# Chapter 45: create or delete the local kind cluster "part08" with the Spark Kubernetes Operator.
#   ch45/cluster.sh up | down        (from code/)
set -euo pipefail
cd "$(dirname "$0")"
NAME=part08 CTX=kind-part08
OPERATOR_CHART=1.9.0        # chart 1.9.0 = operator 1.1.0, built against Spark 4.3.0
if [[ ${1:-up} == down ]]; then kind delete cluster --name $NAME; exit 0; fi
kind get clusters | grep -qx $NAME || kind create cluster --name $NAME --config kind.yaml --image kindest/node:v1.37.0
docker build -q -t spark-masterclass/spark:4.3.0-book . >/dev/null
for img in apache/spark-kubernetes-operator:1.1.0 apache/spark:4.3.0-python3 \
           spark-masterclass/spark:4.3.0-book; do
  kind load docker-image --name $NAME "$img" >/dev/null
done
helm repo add spark-kubernetes-operator https://apache.github.io/spark-kubernetes-operator >/dev/null 2>&1 || true
helm upgrade --install spark spark-kubernetes-operator/spark-kubernetes-operator \
  --kube-context $CTX --version $OPERATOR_CHART --wait >/dev/null
kubectl --context $CTX get pods -o wide | cut -c1-90
