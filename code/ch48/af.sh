#!/usr/bin/env bash
# Run an airflow command against Chapter 48's local Airflow (see airflow_env.sh).
#   ch48/af.sh dags test ch48_taxi_submit
# Connections are environment variables, so the metadata database holds no secrets and no state that matters.
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
WORK=${CH48_WORK:-$HOME/.cache/spark-masterclass/ch48}
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
export AIRFLOW_CONN_SPARK_CONNECT_CH48='{"conn_type": "spark_connect", "host": "localhost", "port": 15064}'
mkdir -p "$AIRFLOW_HOME"
exec airflow "$@"
