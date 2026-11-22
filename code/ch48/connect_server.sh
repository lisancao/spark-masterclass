#!/usr/bin/env bash
# Start or stop the Spark 4.3 Connect server that Chapter 48's Connect DAG uses.
# gRPC on 15064, the UI and its REST API on 4064 (this chapter's ports).
#   ch48/connect_server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs/ch48 SPARK_PID_DIR=$PWD/../data/pids/ch48 SPARK_LOCAL_IP=127.0.0.1
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" \
  --conf spark.connect.grpc.binding.port=15064 --conf spark.ui.port=4064 >/dev/null
until curl -sf localhost:4064/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:15064, UI http://localhost:4064"
