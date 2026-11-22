#!/usr/bin/env bash
# Chapter 31: the quality pipeline on two good days, then on a broken third day. First with the
# default catalog (Parquet tables, Hive metastore), then with Iceberg tables.
# Run from code/:  ch31/gate.sh > ch31/output/gate.txt
set -uo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/part06/ch31-quality"
I="${BOOK_DATA:-../data}/work/part06/ch31-quality-ice"
HIVE="spark.sql.catalogImplementation=hive"
flows() { grep -E 'has (COMPLETED|FAILED)|SKIPPED|Run is|Error|Exception' | grep -v -E '^\s+(File|raise)' \
          | sed -E 's/^[0-9-]+ //; s/spark_catalog\.default\.//; s/lake\.retail\.//; s/^.*(pyspark\.errors[a-z.]+Exception)/\1/' | cut -c1-200; }
show() { .venv/bin/python ch30/show_tables.py "$D" bronze_orders quarantine_orders silver_orders revenue_by_currency 2>/dev/null | grep rows
         SHOW=1 .venv/bin/python ch30/show_tables.py "$D" quality_report 2>/dev/null | grep -v -E '^(quality_report: |tables:)'; }
echo "== run 1: two days of orders"
FRESH=1 DATA=quality SERVER_CONF=$HIVE common/pipeline.sh ch31/quality part06/ch31-quality run | flows
show
SHOW=1 .venv/bin/python ch31/quarantine.py "$D" 2>/dev/null
echo "== orders_2026-10-03.csv arrives (a broken export)"
cp "$D/data/incoming/orders_2026-10-03.csv" "$D/data/orders/"
echo "== run 2"
SERVER_CONF=$HIVE common/pipeline.sh ch31/quality part06/ch31-quality run | flows
show

echo; echo "== the same two runs with Iceberg tables"
FRESH=1 DATA=quality common/pipeline.sh ch31/quality part06/ch31-quality-ice prepare
printf '  spark.sql.catalog.lake: org.apache.iceberg.spark.SparkCatalog\n  spark.sql.catalog.lake.type: hadoop\n  spark.sql.catalog.lake.warehouse: file://%s/lake-warehouse\n' "$I" >> "$I/spark-pipeline.yml"
cat >> "$I/spark-pipeline.yml" <<YML
catalog: lake
database: retail
YML
.venv/bin/python common/lake.py "$I/lake-warehouse" retail >/dev/null 2>&1
ICEBERG=1 common/pipeline.sh ch31/quality part06/ch31-quality-ice run | flows | grep -E 'Run is|FAILED'
LAKE=1 .venv/bin/python ch30/show_tables.py "$I" silver_orders revenue_by_currency 2>/dev/null | grep rows | sed 's/^/  run 1: /'
cp "$I/data/incoming/orders_2026-10-03.csv" "$I/data/orders/"
ICEBERG=1 common/pipeline.sh ch31/quality part06/ch31-quality-ice run | flows | grep -E 'Run is|FAILED|SKIPPED'
LAKE=1 .venv/bin/python ch30/show_tables.py "$I" silver_orders revenue_by_currency 2>/dev/null | grep rows | sed 's/^/  run 2: /'
