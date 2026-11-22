#!/usr/bin/env bash
# Chapter 24: time the taxi queries on every engine, and on the CPU of each Spark version
# that an engine needs, one run at a time under the bench lock.
# Run from code/:  BOOK_DATA=~/spark-masterclass/data SPARK_UI_PORT=4062 bash ch24/run_bench.sh
set -uo pipefail
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-127.0.0.1}
V=$BOOK_DATA/ch24; O=ch24/output; LOCK=$BOOK_DATA/.bench.lock
run() {   # run <engine> <python>
  flock "$LOCK" "$2" ch24/bench.py "$1" 2>/dev/null | sed "s#$HOME#~#g"
}
{
  echo "machine: $(nproc) cores, $(free -g | awk '/Mem:/{print $2}') GB RAM," \
       "$(nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader)"
  echo "java: $(java -version 2>&1 | head -1)"
  run cpu    .venv/bin/python
  run cpu    "$V/venv-4.2.0/bin/python"
  run comet  "$V/venv-4.2.0/bin/python"
  run rapids "$V/venv-4.2.0/bin/python"
  run cpu    "$V/venv-4.1.1/bin/python"
  run gluten "$V/venv-4.1.1/bin/python"
} | tee $O/bench.txt
