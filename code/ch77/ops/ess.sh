#!/usr/bin/env bash
# Chapter 77, Steps 12 and 13: the external shuffle service and push-based shuffle on Chapter 70's
# standalone cluster. The workers run the shuffle service on port 7351.
#   ch77/ops/ess.sh        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
source ../../ch70/env.sh
../../ch70/standalone.sh down
echo "--- two workers on one host, each with a shuffle service"
SPARK_WORKER_INSTANCES=2 CH80_WORKER_OPTS="-Dspark.shuffle.service.enabled=true -Dspark.shuffle.service.port=7351" \
  timeout 20 ../../ch70/standalone.sh up
grep -h -o "requirement failed: Starting multiple workers.*" "$SPARK_LOG_DIR"/*Worker-1-*.out | tail -1 | fold -s -w 92
../../ch70/standalone.sh down
echo "--- one worker with 4 cores and the shuffle service on port 7351"
export SPARK_WORKER_INSTANCES=1 SPARK_WORKER_CORES=4 SPARK_WORKER_MEMORY=4g
CH80_WORKER_OPTS="-Dspark.shuffle.service.enabled=true -Dspark.shuffle.service.port=7351" ../../ch70/standalone.sh up
echo "worker shuffle service: $(grep -h -o "Starting shuffle service on port [0-9]*.*" "$SPARK_LOG_DIR"/*Worker-1-*.out | tail -1)"
DYN=(--conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true
     --conf spark.shuffle.service.port=7351 --conf spark.dynamicAllocation.executorIdleTimeout=5s
     --conf spark.dynamicAllocation.minExecutors=0 --conf spark.executor.cores=2)
echo "--- dynamic allocation with the external shuffle service"
"$SPARK_HOME/bin/spark-submit" --master "$MASTER" "${DYN[@]}" ess_app.py "$BOOK_DATA/nyc-taxi/yellow" 2>/dev/null
echo "--- push-based shuffle requested on standalone (Step 13)"
"$SPARK_HOME/bin/spark-submit" --master "$MASTER" "${DYN[@]}" --conf spark.shuffle.push.enabled=true \
  ../../ch71/probe.py ch77-push 2>&1 | grep -E "Push-based|ok," | sed -E 's/^[0-9/]+ [0-9:]+ //' | uniq -c | sed -E 's/^ +1 //; s/^ +([0-9]+) /(\1 times) /'
