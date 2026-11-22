#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server for Chapter 37: gRPC on 10240, the UI and its REST API on 4124.
# Its own PID and log folders, so it does not stop another Connect server on the machine.
#   ch37/server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs-part04 SPARK_PID_DIR=$PWD/../data/pids-part04 SPARK_LOCAL_IP=127.0.0.1
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" --conf spark.ui.port=4124 \
    --conf spark.connect.grpc.binding.port=10240 --conf spark.sql.session.timeZone=UTC >/dev/null
until curl -sf localhost:4124/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:10240, UI http://localhost:4124"
