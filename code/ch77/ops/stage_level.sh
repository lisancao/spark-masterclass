#!/usr/bin/env bash
# Chapter 77, Step 15: workers that report GPUs through a discovery script, and an application that
# asks for them in one stage only.
#   ch77/ops/stage_level.sh        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
source ../../ch70/env.sh
D=$(pwd)
../../ch70/standalone.sh down
CH80_WORKER_OPTS="-Dspark.worker.resource.gpu.amount=2 -Dspark.worker.resource.gpu.discoveryScript=$D/getGpus.sh" \
  ../../ch70/standalone.sh up
echo "worker 1 log: $(grep -h -o "Custom resources for worker:.*\|gpu -> \[.*" "$(command ls "$SPARK_LOG_DIR"/*Worker-1-*.out | head -1)" | tail -1)"
sed "s#/opt/getGpus.sh#$D/getGpus.sh#" stage_level.py > "$CH80_WORK/stage_level.py"
"$SPARK_HOME/bin/spark-submit" --master "$MASTER" \
  --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.executor.cores=2 --conf spark.dynamicAllocation.executorIdleTimeout=60s \
  "$CH80_WORK/stage_level.py" 2>&1 | grep -v -E "WARN|INFO|ERROR|incubator|log4j|setLogLevel|StacklessClosed|^\s+at |^\s*$" | sed "s#$D#ch77/ops#g"
