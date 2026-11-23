#!/usr/bin/env bash
# Install the Apache Spark Kubernetes Operator 1.1.0 (Helm chart 1.9.0) into the kind cluster "ch63".
#   ch63/spark_operator.sh
# Chapter 62 explains the operator. This chapter only submits its SparkApplication resources from Airflow.
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
helm repo add spark-kubernetes-operator https://apache.github.io/spark-kubernetes-operator >/dev/null 2>&1 || true
helm repo update spark-kubernetes-operator >/dev/null
helm --kube-context kind-ch63 upgrade --install spark-kubernetes-operator \
  spark-kubernetes-operator/spark-kubernetes-operator --version 1.9.0 \
  --namespace spark-operator --create-namespace -f "$CH48/k8s/spark-operator-values.yaml" --wait --timeout 5m >/dev/null
kubectl --context kind-ch63 -n spark-operator get pods
kubectl --context kind-ch63 get crd -o name | grep spark.apache.org
