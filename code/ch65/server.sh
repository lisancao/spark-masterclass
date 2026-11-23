#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server from the pip install, for Part IX.
# gRPC on 10290, the UI and its REST API on 4129 (so it does not collide with other runs).
#   ch65/server.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs-part09a SPARK_PID_DIR=$PWD/../data/pids-part09a SPARK_LOCAL_IP=127.0.0.1
export SPARK_IDENT_STRING=part09a
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
PYSPARK_PYTHON=$PWD/.venv/bin/python "$SPARK_HOME/sbin/start-connect-server.sh" \
  --conf spark.connect.grpc.binding.port=10290 --conf spark.ui.port=4129 >/dev/null
until curl -sf localhost:4129/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:10290, UI http://localhost:4129"
