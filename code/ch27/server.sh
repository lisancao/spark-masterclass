#!/usr/bin/env bash
# Start or stop the Spark 4.3.0 Connect server that Chapters 27 to 29 use, from the pip install.
# gRPC on CONNECT_PORT (default 15106), the UI and its REST API on SPARK_UI_PORT (default 4146).
# The server binds 0.0.0.0, so client containers that run with --network host can reach it.
#   ch27/server.sh start | stop
#   ch27/server.sh small-streams   each response stream ends after 1 MiB, so clients must reattach
set -euo pipefail
cd "$(dirname "$0")/.."
PORT=${CONNECT_PORT:-15106} UI=${SPARK_UI_PORT:-4146}
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs-ch27 SPARK_PID_DIR=$PWD/../data/pids-ch27
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
EXTRA=()
[[ ${1:-start} == small-streams ]] && EXTRA=(--conf spark.connect.execute.reattachable.senderMaxStreamSize=1m)
"$SPARK_HOME/sbin/start-connect-server.sh" \
  --driver-memory 8g \
  --conf spark.driver.maxResultSize=4g \
  --conf spark.connect.grpc.binding.port=$PORT \
  --conf spark.ui.port=$UI "${EXTRA[@]}" >/dev/null
until curl -sf localhost:$UI/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:$PORT, UI http://localhost:$UI"
