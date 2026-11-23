#!/usr/bin/env bash
# Run an airflow command against Chapter 77's local Airflow (see airflow_env.sh).
#   ch77/af.sh dags test ch77_taxi_submit
# Connections are environment variables, so the metadata database holds no secrets and no state that matters.
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
WORK=${CH48_WORK:-$HOME/.cache/spark-masterclass/ch77}
TARBALL=${SPARK_TARBALL:-$HOME/spark-masterclass/data/tarball/spark-4.3.0-bin-hadoop3}
export AIRFLOW_HOME=$WORK/airflow-home
export AIRFLOW__CORE__DAGS_FOLDER=$CH48/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT=${CH48_AIRFLOW_PORT:-8148}
export AIRFLOW__LOGGING__COLORED_CONSOLE_LOG=False
export PATH=$WORK/airflow-venv/bin:$TARBALL/bin:$PATH      # airflow first, then spark-submit / spark-sql
export SPARK_HOME=$TARBALL SPARK_LOCAL_IP=127.0.0.1
export CH48_TAXI=${CH48_TAXI:-$HOME/spark-masterclass/data/nyc-taxi} CH48_OUT=${CH48_OUT:-$WORK/out/zone_revenue}
# Connections, in Airflow's JSON form.
export AIRFLOW_CONN_SPARK_LOCAL='{"conn_type": "spark", "host": "local[2]"}'
export AIRFLOW_CONN_SPARK_SQL_LOCAL='{"conn_type": "spark_sql", "host": "local[2]"}'
export AIRFLOW_CONN_SPARK_CONNECT_CH48='{"conn_type": "spark_connect", "host": "localhost", "port": 15064, "login": "airflow"}'
# The kind cluster "ch77", for the Kubernetes DAG: one kubeconfig file for spark-submit and for Airflow.
if kind get clusters 2>/dev/null | grep -qx ch77; then
  export KUBECONFIG=$WORK/kubeconfig-ch77
  kind get kubeconfig --name ch77 > "$KUBECONFIG"
  API=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
  export AIRFLOW_CONN_SPARK_K8S='{"conn_type": "spark", "host": "k8s://'$API'",
    "extra": {"deploy-mode": "cluster", "namespace": "spark"}}'
  export AIRFLOW_CONN_SPARK_K8S_JOBS='{"conn_type": "spark", "host": "k8s://'$API'",
    "extra": {"deploy-mode": "cluster", "namespace": "spark-jobs"}}'
  export AIRFLOW_CONN_KUBERNETES_DEFAULT='{"conn_type": "kubernetes",
    "extra": {"kube_config_path": "'$KUBECONFIG'", "namespace": "spark-apps"}}'
  export AIRFLOW__KUBERNETES_EXECUTOR__IN_CLUSTER=False   # SparkSubmitOperator polls the driver pod with this client
fi
mkdir -p "$AIRFLOW_HOME"
exec airflow "$@"
