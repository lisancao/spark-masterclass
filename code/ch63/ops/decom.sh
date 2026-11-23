#!/usr/bin/env bash
# Chapter 63, Step 14: decommission one standalone worker while an application holds blocks on it.
#   ch63/ops/decom.sh        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
source ../../ch80/env.sh
../../ch80/standalone.sh down
CH80_WORKER_OPTS="-Dspark.decommission.enabled=true" ../../ch80/standalone.sh up
OUT=$CH80_WORK/decom.out ERR=$CH80_WORK/decom.err
"$SPARK_HOME/bin/spark-submit" --master "$MASTER" --conf spark.executor.cores=2 \
  --conf spark.decommission.enabled=true --conf spark.storage.decommission.enabled=true \
  --conf spark.storage.decommission.rddBlocks.enabled=true \
  --conf spark.storage.decommission.shuffleBlocks.enabled=true \
  decom_app.py > "$OUT" 2> "$ERR" & APP=$!
for _ in $(seq 120); do grep -q READY "$OUT" 2>/dev/null && break; sleep 0.5; done
W2=$(cat "$SPARK_PID_DIR"/*Worker-2.pid)
kill -PWR "$W2"; echo "sent SIGPWR to worker 2 (pid $W2)"
wait $APP
grep -v READY "$OUT"
echo "--- driver log"
grep -E "Decommission|decommission|Migrat|migrat" "$ERR" | grep -v -E "^\s+at " | sed -E 's/^[0-9/]+ ([0-9:]+) (INFO|WARN) /\1 /' | cut -c1-150 | awk '!seen[substr($0,10,70)]++' | head -14
grep -E "Shuffle files lost for host" "$ERR" | sed -E 's/^[0-9/]+ ([0-9:]+) INFO /\1 /'
APPD=$(command ls -td "$SPARK_WORKER_DIR"/app-* | head -1)
E0=$(grep -l "Decommission executor" "$APPD"/*/stderr | head -1)
echo "--- log of the decommissioned executor ($(basename "$(dirname "$E0")"))"
grep -E "Decommission executor|Attempting to migrate|local shuffles are added|Need to replicate|migrated successfully|Migrated|Finished decommissioning|exit" "$E0" \
  | sed -E 's/^[0-9/]+ ([0-9:]+) INFO /\1 /' | sed -E 's/FileSegmentManagedBuffer.*//' | awk '!seen[substr($0,10,45)]++' | cut -c1-120 | head -12
echo "--- worker 2 log"
grep -E -i "decommission" "$(command ls "$SPARK_LOG_DIR"/*Worker-2-*.out | head -1)" | grep -v "Spark Command" | tail -4 | sed -E 's/^[0-9/]+ ([0-9:]+) /\1 /' | cut -c1-140
