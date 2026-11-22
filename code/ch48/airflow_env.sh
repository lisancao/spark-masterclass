#!/usr/bin/env bash
# Build a local Airflow 3.4.0 for Chapter 48, with the official constraints file.
#   CH48_WORK=<dir> ch48/airflow_env.sh
# Airflow, the Apache Spark provider and the thin Spark Connect client go in a venv.
# spark-submit and spark-sql come from the Spark 4.3.0 tarball (Chapter 2), on PATH.
set -euo pipefail
cd "$(dirname "$0")"
WORK=${CH48_WORK:-$HOME/.cache/spark-masterclass/ch48}
AIRFLOW_VERSION=3.4.0
PY=3.12
mkdir -p "$WORK"
uv venv --python "$PY" "$WORK/airflow-venv" >/dev/null
curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PY.txt" \
  -o "$WORK/constraints-$PY.txt"
VIRTUAL_ENV="$WORK/airflow-venv" uv pip install -q \
  "apache-airflow==$AIRFLOW_VERSION" \
  "apache-airflow-providers-apache-spark[cncf-kubernetes]" \
  --constraint "$WORK/constraints-$PY.txt"
"$WORK/airflow-venv/bin/python" - <<'PY'
from importlib.metadata import version
for p in ["apache-airflow", "apache-airflow-providers-apache-spark",
          "apache-airflow-providers-cncf-kubernetes", "pyspark-client"]:
    print(f"{p:42} {version(p)}")
PY
