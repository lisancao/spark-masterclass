#!/usr/bin/env bash
# Chapter 80, Step 4: kill the standalone master while an application runs, then start it again.
# The master keeps its state in spark.deploy.recoveryDirectory (FILESYSTEM recovery mode).
#   ch80/ha.sh        (from code/, cluster up)
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
t0=$(date +%s.%N)
ts() { python3 -c "import sys,time; print(f'{time.time()-$t0:6.1f}s')"; }
log() { echo "$(ts)  $*"; }

log "recovery folder before: $(ls "$CH80_WORK/recovery" | tr '\n' ' ')"
CH80_LOG_LEVEL=INFO "$SPARK_HOME/bin/spark-submit" --master "$MASTER" --name ch80-ha \
  --conf spark.ui.port=4151 app.py "$BOOK_DATA/nyc-taxi/yellow" --hold 30 \
  > "$CH80_WORK/ha-app.out" 2> "$CH80_WORK/ha-app.err" &
APP=$!
for _ in $(seq 120); do grep -q "hold  3s" "$CH80_WORK/ha-app.out" 2>/dev/null && break; sleep 0.5; done
log "app running; recovery folder: $(ls "$CH80_WORK/recovery" | tr '\n' ' ')"
MPID=$(cat "$SPARK_PID_DIR"/spark-*-org.apache.spark.deploy.master.Master-1.pid)
kill -9 "$MPID"; log "master pid $MPID killed (SIGKILL)"
sleep 8
"$SPARK_HOME/sbin/start-master.sh" >/dev/null; log "master started again"
until curl -sf "http://127.0.0.1:$SPARK_MASTER_WEBUI_PORT/json/" | grep -q '"status" : "ALIVE"'; do sleep 0.5; done
log "master ALIVE"
python3 status.py "$SPARK_MASTER_WEBUI_PORT" | sed 's/^/        /'
wait $APP; log "app exited with code $?"
echo "--- app output (hold lines)"
grep -E "hold" "$CH80_WORK/ha-app.out" | sed -n '1p;6p;10p;15p;20p;25p;30p'
echo "--- driver log: master lost and found"
grep -E "StandaloneAppClient|StandaloneSchedulerBackend|TransportClient|TransportResponseHandler" "$CH80_WORK/ha-app.err" | grep -v -E "Executor (added|updated)|Granted|Connected to Spark|Connecting to master|created connection|ready for scheduling" \
  | sed -E 's/^[0-9/]+ ([0-9:]+) /\1 /' | cut -c1-150 | head -8
echo "--- new master log"
L=$(ls -t "$SPARK_LOG_DIR"/*Master*.out | head -1)
grep -E "recovery|Recover|RECOVERING|ALIVE|re-registered|Registering worker|Application .* re-registered|Worker has been re-registered|elected" "$L" \
  | sed -E 's/^[0-9/]+ ([0-9:]+) /\1 /' | tail -12
