#!/usr/bin/env bash
# Start or stop the Spark Connect server for the v3 API chapters (gRPC 15117, UI off, so Classic scripts keep port 4157) from the pip install.
#   ch12/connect_server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=${BOOK_DATA:-$HOME/spark-masterclass/data}/logs-v3apis SPARK_PID_DIR=${BOOK_DATA:-$HOME/spark-masterclass/data}/pids-v3apis
export SPARK_LOCAL_IP=127.0.0.1 SPARK_IDENT_STRING=v3apis
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" --conf spark.ui.enabled=false \
  --conf spark.connect.grpc.binding.port=15117 --conf spark.sql.session.timeZone=UTC >/dev/null
until (exec 3<>/dev/tcp/127.0.0.1/15117) 2>/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:15117"
