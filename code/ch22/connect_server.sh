#!/usr/bin/env bash
# Start or stop the Chapter 22 Spark Connect server (gRPC 15103, UI 4144) from the pip install.
#   ch10/connect_server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=${BOOK_DATA:-$PWD/../data}/logs-ch22 SPARK_PID_DIR=${BOOK_DATA:-$PWD/../data}/pids-ch22
export SPARK_LOCAL_IP=127.0.0.1 SPARK_IDENT_STRING=ch22
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" --conf spark.ui.port=4144 \
  --conf spark.connect.grpc.binding.port=15103 --conf spark.sql.session.timeZone=UTC >/dev/null
until curl -sf localhost:4144/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:15103, UI http://localhost:4144"
