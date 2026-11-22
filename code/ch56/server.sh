#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server for Part X: gRPC on 15061, UI and REST API on 4161.
# (Ports differ from Chapter 4's 15002/4041 so both can run on one machine.)
#   ch56/server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs-part10 SPARK_PID_DIR=$PWD/../data/pids-part10 SPARK_LOCAL_IP=127.0.0.1
export SPARK_IDENT_STRING=part10
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" --conf spark.ui.port=4161 \
  --conf spark.connect.grpc.binding.port=15061 >/dev/null
until curl -sf localhost:4161/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:15061, UI http://localhost:4161"
