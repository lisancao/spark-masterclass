#!/usr/bin/env bash
# Chapter 34: run the crm pipeline three times. Drop 1 has seq 1, 2, 3, 5. Drop 2 brings the late
# seq 4, the delete (seq 6), a new customer (seq 7) and a repeat of c-03 (seq 8). Drop 3 changes only
# the name of c-03 (seq 9) and moves c-04 to Hanoi (seq 10).
# Run from code/:  ch34/drops.sh > ch34/output/drops.txt
set -euo pipefail
cd "$(dirname "$0")/.."
W="${BOOK_DATA:-../data}/work/part06/ch34-crm"
show() { .venv/bin/python ch34/show_targets.py 2>/dev/null | sed "s#$HOME#~#g"; }
echo "== drop 1 lands"
FRESH=1 DATA=crm ICEBERG=1 common/pipeline.sh ch34/crm part06/ch34-crm prepare
.venv/bin/python common/lake.py "$W/lake-warehouse" crm 2>/dev/null
mkdir -p "$W/data/landing" && cp "$W/data/drop1/"* "$W/data/landing/"
ICEBERG=1 common/pipeline.sh ch34/crm part06/ch34-crm run | grep -E "Flow .* (COMPLETED|FAILED)|Run is"
show
for n in 2 3; do
  echo "== drop $n lands"
  cp "$W/data/drop$n/"* "$W/data/landing/"
  ICEBERG=1 common/pipeline.sh ch34/crm part06/ch34-crm run | grep -E "Flow .* (COMPLETED|FAILED)|Run is"
  show
done
