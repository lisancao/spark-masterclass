#!/usr/bin/env bash
# Chapter 80: a standalone cluster on this machine: one master and two workers from the tarball's sbin/.
#   ch80/standalone.sh up | down | status        (from code/)
# CH80_EXTRA_DEFAULTS names a file to append to spark-defaults.conf (Chapter 81: the shared secret).
# CH80_DAEMON_OPTS adds -D options to the master and the workers (Chapter 81 uses it for spark.authenticate).
# The master keeps its recovery state in a folder (FILESYSTEM recovery mode), so Step 4 can kill it.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
case ${1:-up} in
up)
  mkdir -p "$SPARK_CONF_DIR" "$SPARK_LOG_DIR" "$SPARK_PID_DIR" "$SPARK_WORKER_DIR" "$CH80_WORK/recovery"
  cp conf/spark-defaults.conf "$SPARK_CONF_DIR/"
  [[ -n ${CH80_EXTRA_DEFAULTS:-} ]] && cat "$CH80_EXTRA_DEFAULTS" >> "$SPARK_CONF_DIR/spark-defaults.conf"
  chmod 600 "$SPARK_CONF_DIR/spark-defaults.conf"
  cat > "$SPARK_CONF_DIR/spark-env.sh" <<ENV
SPARK_MASTER_OPTS="-Dspark.deploy.recoveryMode=FILESYSTEM -Dspark.deploy.recoveryDirectory=$CH80_WORK/recovery -Dspark.master.rest.port=6151 ${CH80_DAEMON_OPTS:-}"
SPARK_WORKER_OPTS="${CH80_DAEMON_OPTS:-} ${CH80_WORKER_OPTS:-}"
ENV
  "$SPARK_HOME/sbin/start-master.sh" >/dev/null
  until curl -sf "http://127.0.0.1:$SPARK_MASTER_WEBUI_PORT/json/" >/dev/null; do sleep 1; done
  "$SPARK_HOME/sbin/start-worker.sh" "$MASTER" >/dev/null
  until [ "$(curl -s "http://127.0.0.1:$SPARK_MASTER_WEBUI_PORT/json/" | python3 -c 'import json,sys;print(json.load(sys.stdin)["aliveworkers"])')" = "$SPARK_WORKER_INSTANCES" ]; do sleep 1; done
  ;;
down)
  "$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
  "$SPARK_HOME/sbin/stop-master.sh" >/dev/null 2>&1 || true
  ;;
status)
  python3 status.py "$SPARK_MASTER_WEBUI_PORT"
  ;;
esac
