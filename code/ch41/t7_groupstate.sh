#!/usr/bin/env bash
# Run t7_groupstate.scala in spark-shell (from the pyspark package) over the four checkout drops.
# Run from code/:  ch41/t7_groupstate.sh
set -euo pipefail
cd "$(dirname "$0")/.."
WORK=$(pwd)/../data/work/ch41/t7
rm -rf "$WORK"; mkdir -p "$WORK/inbox"
i=1; for f in data/checkout_events/drop_*.jsonl; do cp "$f" "$WORK/inbox/"; touch -d "@$((1000000 + i))" "$WORK/inbox/$(basename "$f")"; i=$((i+1)); done
export INBOX=$WORK/inbox WORK
SPARK_HOME=$(.venv/bin/python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))") \
  .venv/bin/spark-shell --master "local[2]" --conf spark.ui.port="${SPARK_UI_PORT:-4040}" \
  --conf spark.sql.shuffle.partitions=2 --conf spark.sql.session.timeZone=UTC \
  -i ch41/t7_groupstate.scala </dev/null 2>"$WORK/stderr.log" | sed -n '/^1\. /,$p'
