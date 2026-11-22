#!/usr/bin/env bash
# Chapter 33: run the shop pipeline, add one day of orders, and run it again.
# Run from code/:  ch33/two_runs.sh > ch33/output/two_runs.txt
set -uo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/part06/ch33-shop"
export SERVER_CONF="spark.sql.catalogImplementation=hive" EVENTLOG=1
flows() { grep -E 'has (COMPLETED|FAILED)|is SKIPPED|Run is|Error' | sed -E 's/^[0-9-]+ //; s/spark_catalog\.default\.//'; }
show() {
  .venv/bin/python ch32/show_tables.py "$D" bronze_orders silver_orders quarantine_orders \
    gold_daily_revenue 2>/dev/null | grep rows
  .venv/bin/python ch33/peek.py "$D" 2>/dev/null
}
echo "== run 1: orders_2026-10-01.json and orders_2026-10-02.json"
FRESH=1 DATA=retail common/pipeline.sh ch33/shop part06/ch33-shop run | flows
show
echo; echo "== orders_2026-10-03.json arrives"
cp "$D/data/later/orders_2026-10-03.json" "$D/data/orders/"
ls "$D/data/orders"
echo; echo "== run 2"
DATA=retail common/pipeline.sh ch33/shop part06/ch33-shop run | flows
show
echo; echo "== streaming progress in the Spark event log"
.venv/bin/python ch33/progress.py "$D"
