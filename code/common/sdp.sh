# Shared shell helpers for the SDP part (chapters 77 to 79). Source it:  . common/sdp.sh
#   sdp_venv            a venv with pyspark[pipelines]==4.3.0 at $BOOK_DATA/work/sdp/venv, activated
#   sdp_project <dir>   a fresh copy of the taxi_etl project of Chapter 46 (with two days of data)
#   sdp_server <dir> [--conf k=v ...]   start a Connect server with its current folder in <dir>
#   sdp_stop            stop that server (by PID)
#   x <command>         print "$ command", run it, print a blank line
#   clean               filter Spark log noise and the home folder out of output
BOOK_DATA="${BOOK_DATA:-$HOME/spark-masterclass/data}"; export BOOK_DATA
SDP_CODE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${CONNECT_PORT:-15104}"; UI="${SPARK_UI_PORT:-4144}"
export SPARK_LOCAL_IP=127.0.0.1

clean() { sed -u -e "s#$HOME#~#g" -e 's/\r.*\r//' | grep --line-buffered -v -E '^\s+at |WARN|^Using Spark|^To adjust|^Setting default log|^WARNING: Using incubator|FutureWarning|require_minimum_pandas|^\s*$'; }
x() { echo "\$ $*"; eval "$@" 2>&1; local rc=$?; echo; return $rc; }

sdp_venv() {
  local v="$BOOK_DATA/work/sdp/venv"
  if [[ ! -x "$v/bin/spark-pipelines" ]]; then
    python3 -m venv "$v" && "$v/bin/pip" install --quiet "pyspark[pipelines]==4.3.0" "pandas<3"
  fi
  # shellcheck disable=SC1091
  source "$v/bin/activate"
  SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"; export SPARK_HOME
}

sdp_project() {
  local d="$1"; rm -rf "$d"; mkdir -p "$d/data" "$d/transformations"
  cp "$SDP_CODE/ch46/project/transformations/"* "$d/transformations/"
  cp "$BOOK_DATA/nyc-taxi/taxi_zone_lookup.csv" "$d/data/"
  python "$SDP_CODE/ch46/make_days.py" "$d/data/trips" 2026-06-01 2026-06-02 >/dev/null
  printf 'name: taxi_etl\nstorage: file://%s/pipeline-storage\nlibraries:\n  - glob:\n      include: transformations/**\n' "$d" > "$d/spark-pipeline.yml"
}

SDP_SERVER_PID=""
sdp_server() {
  local d="$1"; shift
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then echo "port $PORT is in use" >&2; return 1; fi
  ( cd "$d" && exec "$SPARK_HOME/bin/spark-submit" --class org.apache.spark.sql.connect.service.SparkConnectServer \
      --master "local[*]" --conf "spark.connect.grpc.binding.port=$PORT" --conf "spark.ui.port=$UI" \
      "$@" spark-internal >"$d/connect-server.log" 2>&1 ) &
  SDP_SERVER_PID=$!
  for _ in $(seq 240); do
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
    kill -0 "$SDP_SERVER_PID" 2>/dev/null || { tail -20 "$d/connect-server.log" >&2; return 1; }
    sleep 0.5
  done
}
sdp_stop() {
  if [[ -n "$SDP_SERVER_PID" ]]; then
    pkill -P "$SDP_SERVER_PID" 2>/dev/null; kill "$SDP_SERVER_PID" 2>/dev/null; wait "$SDP_SERVER_PID" 2>/dev/null
    SDP_SERVER_PID=""
    for _ in $(seq 60); do (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null || return 0; sleep 0.5; done
  fi
}
