#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server from the pip install.
# gRPC on $CONNECT_PORT (default 15002), the UI and its REST API on $CONNECT_UI_PORT (default 4041).
#   ch04/server.sh start [--conf key=value ...] | stop
set -euo pipefail
cd "$(dirname "$0")/.."
PORT=${CONNECT_PORT:-15002} UI=${CONNECT_UI_PORT:-4041}
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs SPARK_PID_DIR=$PWD/../data/pids SPARK_LOCAL_IP=127.0.0.1
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
cmd=${1:-start}; shift || true
[[ $cmd == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" --conf spark.ui.port=$UI \
  --conf spark.connect.grpc.binding.port=$PORT "$@" >/dev/null
until curl -sf localhost:$UI/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:$PORT, UI http://localhost:$UI"
