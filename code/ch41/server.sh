#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server from the pip install, for Part VIII.
# gRPC on $CONNECT_PORT (default 10280), the UI and its REST API on $CONNECT_UI_PORT (default 4128).
# Extra arguments go to start-connect-server.sh, so a chapter can pass its own --conf settings.
#   ch41/server.sh start [--conf k=v ...] | stop
set -euo pipefail
cd "$(dirname "$0")/.."
PORT=${CONNECT_PORT:-10280} UI=${CONNECT_UI_PORT:-4128}
export SPARK_HOME=$(.venv/bin/python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
# Logs and PID files go in the repo's own data/ folder (ignored by git), whatever BOOK_DATA says,
# so that start and stop always find the same PID file.
RUN=$PWD/../data
export SPARK_LOG_DIR=$RUN/logs/part08-$PORT SPARK_PID_DIR=$RUN/pids/part08-$PORT SPARK_LOCAL_IP=127.0.0.1
export SPARK_IDENT_STRING=part08-$PORT
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
for _ in $(seq 60); do curl -sf "localhost:$UI/api/v1/applications" >/dev/null 2>&1 || break; sleep 1; done
cmd=${1:-start}; shift || true
[[ $cmd == stop ]] && exit 0
"$SPARK_HOME/sbin/start-connect-server.sh" \
  --conf spark.connect.grpc.binding.port="$PORT" --conf spark.ui.port="$UI" "$@" >/dev/null
until curl -sf "localhost:$UI/api/v1/applications" >/dev/null && (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do sleep 1; done
echo "Connect server up: sc://localhost:$PORT, UI http://localhost:$UI"
