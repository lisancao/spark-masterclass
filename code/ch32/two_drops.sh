#!/usr/bin/env bash
# Chapter 32: run the crm pipeline twice. Drop 1 has seq 1, 2, 3, 5. Drop 2 brings the late
# seq 4, the delete (seq 6), a new customer (seq 7) and a repeat of c-03 (seq 8).
# Run from code/:  ch32/two_drops.sh > ch32/output/two_drops.txt
set -euo pipefail
cd "$(dirname "$0")/.."
W="${BOOK_DATA:-../data}/work/part06/ch32-crm"
show() { .venv/bin/python ch32/show_targets.py 2>/dev/null | sed "s#$HOME#~#g"; }
echo "== drop 1 lands"
FRESH=1 DATA=crm ICEBERG=1 common/pipeline.sh ch32/crm part06/ch32-crm prepare
.venv/bin/python common/lake.py "$W/lake-warehouse" crm 2>/dev/null
mkdir -p "$W/data/landing" && cp "$W/data/drop1/"* "$W/data/landing/"
ICEBERG=1 common/pipeline.sh ch32/crm part06/ch32-crm run | grep -E "Flow .* (COMPLETED|FAILED)|Run is"
show
echo "== drop 2 lands"
cp "$W/data/drop2/"* "$W/data/landing/"
ICEBERG=1 common/pipeline.sh ch32/crm part06/ch32-crm run | grep -E "Flow .* (COMPLETED|FAILED)|Run is"
show
