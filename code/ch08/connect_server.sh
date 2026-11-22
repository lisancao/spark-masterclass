#!/usr/bin/env bash
# Chapter 8: start or stop a Connect server that sets three configs at launch.
# gRPC on $CONNECT_PORT (default 15102), the UI and REST API on $SPARK_UI_PORT (default 4142).
#   ch08/connect_server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
PORT=${CONNECT_PORT:-15102} UI=${SPARK_UI_PORT:-4142}
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs-ch08 SPARK_PID_DIR=$PWD/../data/pids-ch08 SPARK_LOCAL_IP=127.0.0.1
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" \
  --conf spark.connect.grpc.binding.port=$PORT --conf spark.ui.port=$UI \
  --conf spark.sql.shuffle.partitions=21 \
  --conf spark.sql.warehouse.dir=/tmp/ch08-server-warehouse \
  --conf spark.myteam.db.password=hunter2 >/dev/null
until curl -sf localhost:$UI/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:$PORT, UI http://localhost:$UI"
