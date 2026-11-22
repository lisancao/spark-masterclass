#!/usr/bin/env bash
# Chapter 45, Step 11: structured logging, set at launch with spark-submit.
#   ch45/logs.sh      (from code/)
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH=$PWD/.venv/bin:$PATH SPARK_LOCAL_IP=127.0.0.1   # spark-submit finds SPARK_HOME through python3
for flag in false true; do
  echo "--- spark.log.structuredLogging.enabled=$flag"
  spark-submit --master 'local[1]' --conf spark.ui.port="${SPARK_UI_PORT:-4040}" \
    --conf spark.log.structuredLogging.enabled=$flag ch45/logs_app.py 2>&1 \
    | grep -E 'NativeCodeLoader|python side' | cut -c1-300 || true
done
