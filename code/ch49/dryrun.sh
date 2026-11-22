#!/usr/bin/env bash
# Chapter 49, Steps 3 and 4: dry-run each version of the gold flow, then run two of them for real.
#   ch49/dryrun.sh       (from code/)
set -uo pipefail
cd "$(dirname "$0")"
export SPARK_LOCAL_IP=127.0.0.1 PATH=$PWD/../.venv/bin:$PATH
SP="uv run --quiet --with pyyaml spark-pipelines"      # the pipelines CLI needs PyYAML
python3 gen_orders.py >/dev/null
cd pipeline
verdict() { grep -E "Run is|Error|Exception|\[[A-Z_]{6,}" | grep -v "^\s*at " | sed -E 's/^[0-9-]+ [0-9:.]+ //' | cut -c1-150 | head -3; }
for v in typo column conf epoch ok; do
  cp ../variants/gold_$v.py transformations/gold.py
  rm -rf spark-warehouse metastore_db /tmp/ch49-pipeline/storage
  echo "--- dry-run, gold_$v.py"
  $SP dry-run 2>&1 | verdict
done
for v in epoch ok; do
  cp ../variants/gold_$v.py transformations/gold.py
  rm -rf spark-warehouse metastore_db /tmp/ch49-pipeline/storage
  echo "--- run, gold_$v.py"
  $SP run 2>&1 | grep -E "Run is" | sed -E 's/^[0-9-]+ [0-9:.]+ //'
  ../../.venv/bin/python ../show.py 2>/dev/null
done
rm -f transformations/gold.py; rm -rf spark-warehouse metastore_db
