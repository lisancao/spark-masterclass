#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server from the pip install for Part VII.
# gRPC on ${CONNECT_PORT:-10270}, the UI and its REST API on ${CONNECT_UI_PORT:-4127}.
#   ch35/server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
GRPC=${CONNECT_PORT:-10270} UI=${CONNECT_UI_PORT:-4127}
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
DATA_DIR=${BOOK_DATA:-$PWD/../data}
export SPARK_LOG_DIR=$DATA_DIR/logs/part07 SPARK_PID_DIR=$DATA_DIR/pids/part07 SPARK_LOCAL_IP=127.0.0.1
export SPARK_IDENT_STRING=part07
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" --conf spark.ui.port=$UI \
  --conf spark.connect.grpc.binding.port=$GRPC ${SERVER_CONF:-} >/dev/null
until curl -sf localhost:$UI/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:$GRPC, UI http://localhost:$UI"
