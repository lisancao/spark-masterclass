#!/usr/bin/env bash
# Run spark-pipelines on a copy of a book pipeline project (Part VI, chapters 30 to 34).
#
#   common/pipeline.sh <project> <work-name> [spark-pipelines arguments...]
#
# <project> is a folder under code/ with a spark-pipeline.yml. The first time, or with FRESH=1,
# it is copied to $BOOK_DATA/work/<work-name> together with code/data/$DATA (default: retail)
# as data/, and @WORK@ in the spec becomes the copy's absolute path. Then spark-pipelines runs
# there, so relative paths such as data/orders resolve.
#   <spark-pipelines arguments> = prepare  only copies the project and exits.
#   ICEBERG=1   add the Iceberg runtime for Spark 4.3 (an unreleased build, see notes/part06.md)
#   RAW=1       print the full log (by default WARN lines and Java stack frames are removed)
#   EVENTLOG=1  write the server's Spark event log to <work>/spark-events
#   SERVER_CONF="k=v ..."  extra static confs for the server, for example
#               spark.sql.catalogImplementation=hive (with LOCAL=1 they go to the CLI as --conf)
#   LOCAL=1     let spark-pipelines start its own in-process server (the reader's default,
#               always on port 15002). Without it, this script starts a Connect server on
#               $CONNECT_PORT (default 15056) in the project folder, runs the CLI with
#               --remote, and stops the server, so two writers on one machine do not share 15002.
set -euo pipefail
CODE="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${BOOK_DATA:-$CODE/../data}"
project="$1"; work="$DATA_DIR/work/$2"; shift 2
if [[ "${FRESH:-0}" == 1 ]]; then rm -rf "$work"; fi
if [[ ! -d "$work" ]]; then
  mkdir -p "$(dirname "$work")"
  cp -r "$CODE/$project" "$work"
  cp -r "$CODE/data/${DATA:-retail}" "$work/data"
  # specs in the book say storage: file://@WORK@/...; point them at this copy
  sed -i "s#@WORK@#$work#g" "$work/spark-pipeline.yml"
fi
if [[ "${1:-}" == prepare ]]; then exit 0; fi   # only copy the project
ICEBERG_JAR="$DATA_DIR/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
cd "$work"
export SPARK_LOCAL_IP=127.0.0.1
PY="$CODE/.venv/bin/python"
SPARK_HOME="$("$PY" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
export SPARK_HOME

server_pid=""
stop_server() { [[ -n "$server_pid" ]] && kill "$server_pid" 2>/dev/null && wait "$server_pid" 2>/dev/null; server_pid=""; }
trap stop_server EXIT

if [[ "${LOCAL:-0}" == 1 ]]; then
  jars=(); [[ "${ICEBERG:-0}" == 1 ]] && jars=(--jars "$ICEBERG_JAR")
  for kv in ${SERVER_CONF:-}; do jars+=(--conf "$kv"); done
  cli=(uv run --quiet --project "$CODE" --with pyyaml spark-pipelines "${jars[@]}")
else
  port="${CONNECT_PORT:-15056}"
  sconf=(--master "local[*]" --conf "spark.connect.grpc.binding.port=$port"
         --conf "spark.ui.port=${SPARK_UI_PORT:-4056}" --conf spark.sql.session.timeZone=UTC)
  [[ "${ICEBERG:-0}" == 1 ]] && sconf+=(--jars "$ICEBERG_JAR")
  for kv in ${SERVER_CONF:-}; do sconf+=(--conf "$kv"); done
  if [[ "${EVENTLOG:-0}" == 1 ]]; then
    mkdir -p "$work/spark-events"
    sconf+=(--conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file://$work/spark-events")
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is in use; stop that server first" >&2; exit 1
  fi
  "$SPARK_HOME/bin/spark-submit" --class org.apache.spark.sql.connect.service.SparkConnectServer \
    "${sconf[@]}" spark-internal >"$work/connect-server.log" 2>&1 &
  server_pid=$!
  for _ in $(seq 120); do
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break
    kill -0 "$server_pid" 2>/dev/null || { cat "$work/connect-server.log" >&2; exit 1; }
    sleep 0.5
  done
  cli=(uv run --quiet --project "$CODE" --with pyyaml spark-pipelines --remote "sc://localhost:$port")
fi

run() { "${cli[@]}" "$@" 2>&1; }
set +e
if [[ "${RAW:-0}" == 1 ]]; then
  run "$@"; rc=$?
else
  run "$@" | grep --line-buffered -v -E '^\s+at |WARN|^Using Spark|^To adjust|^\[Stage|^WARNING: Using incubator|^\s*$' \
    | sed -u -e "s#$HOME#~#g" -e 's/\r.*\r//'
  rc=${PIPESTATUS[0]}
fi
stop_server
exit "$rc"
