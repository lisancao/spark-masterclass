#!/usr/bin/env bash
# Chapter 49: two append flows into one streaming table (and two SQL flows into another),
# run three ways: Parquet tables, Parquet tables with one flow at a time, Iceberg tables.
# Run from code/:  ch49/flows.sh > ch49/output/flows.txt
set -uo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/part06"
HIVE="spark.sql.catalogImplementation=hive"
events() { grep -E 'FAILED|Error:|Run is' | sed -E 's/^Error: (.{0,230}).*/  Error: \1.../'; }
echo "== 1. default catalog (Parquet tables), flows run at the same time"
FRESH=1 SERVER_CONF=$HIVE common/pipeline.sh ch49/flows part06/ch49-flows run | events
.venv/bin/python ch49/show_tables.py "$D/ch49-flows" orders_raw order_events 2>/dev/null | grep rows
echo "  Parquet files on disk in orders_raw: $(ls "$D"/ch49-flows/spark-warehouse/orders_raw/*.parquet | wc -l)"

echo; echo "== 2. default catalog, spark.sql.pipelines.execution.maxConcurrentFlows = 1"
FRESH=1 common/pipeline.sh ch49/flows part06/ch49-flows-serial prepare
printf 'configuration:\n  spark.sql.pipelines.execution.maxConcurrentFlows: "1"\n' >> "$D/ch49-flows-serial/spark-pipeline.yml"
SERVER_CONF=$HIVE common/pipeline.sh ch49/flows part06/ch49-flows-serial run | events
.venv/bin/python ch49/show_tables.py "$D/ch49-flows-serial" orders_raw order_events 2>/dev/null | grep rows

echo; echo "== 3. Iceberg catalog, flows run at the same time"
FRESH=1 common/pipeline.sh ch49/flows part06/ch49-flows-ice prepare
cat >> "$D/ch49-flows-ice/spark-pipeline.yml" <<YML
catalog: lake
database: retail
configuration:
  spark.sql.catalog.lake: org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.lake.type: hadoop
  spark.sql.catalog.lake.warehouse: file://$D/ch49-flows-ice/lake-warehouse
YML
.venv/bin/python common/lake.py "$D/ch49-flows-ice/lake-warehouse" retail 2>/dev/null
ICEBERG=1 common/pipeline.sh ch49/flows part06/ch49-flows-ice run | events
LAKE=1 .venv/bin/python ch49/show_tables.py "$D/ch49-flows-ice" orders_raw order_events 2>/dev/null | grep rows
