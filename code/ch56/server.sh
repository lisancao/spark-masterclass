#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server for Part X on gRPC port 15061 (the UI is off).
#   ch56/server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs-part10 SPARK_PID_DIR=$PWD/../data/pids-part10 SPARK_LOCAL_IP=127.0.0.1
export SPARK_IDENT_STRING=part10
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" --conf spark.ui.enabled=false \
  --conf spark.connect.grpc.binding.port=15061 >/dev/null
until (exec 3<>/dev/tcp/127.0.0.1/15061) 2>/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:15061"
