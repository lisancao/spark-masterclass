#!/usr/bin/env bash
# Chapter 30: run the retail pipeline. First in the default in-memory catalog (twice), then with
# a Hive metastore (twice), then with refresh selections. Writes four files in ch30/output/.
# Run from code/:  ch30/run_retail.sh
set -uo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/part06"
O=ch30/output
HIVE="spark.sql.catalogImplementation=hive"
short() { sed -E 's/^(.*(Exception|Error)[^:]*: .{0,250}).*/\1.../' | grep -v -E '^\s+(File|raise|\^)|^Traceback|^\s+(main|run|handle|for result)'; }
flows() { grep -E 'Flow|Run is' | sed -E 's/^[0-9-]+ //'; }

{ echo "== run 1, in-memory catalog"
  FRESH=1 common/pipeline.sh ch30/retail part06/ch30-retail run | short
  echo; echo "== run 2, in-memory catalog"
  common/pipeline.sh ch30/retail part06/ch30-retail run | grep -v -E 'Loading|Creating|Registering|Found|Importing|called' | short
} > $O/run_default.txt

{ echo "== run 1, Hive metastore"
  FRESH=1 SERVER_CONF=$HIVE common/pipeline.sh ch30/retail part06/ch30-retail-hive run | flows
  echo; echo "== run 2, Hive metastore"
  SERVER_CONF=$HIVE common/pipeline.sh ch30/retail part06/ch30-retail-hive run | flows
  echo; .venv/bin/python ch30/show_tables.py "$D/ch30-retail-hive" orders_raw 2>/dev/null
  SHOW=1 .venv/bin/python ch30/show_tables.py "$D/ch30-retail-hive" orders_by_status placed_by_customer products_by_category 2>/dev/null \
    | sed "s#$HOME#~#g"
} > $O/run_hive.txt

( cd "$D/ch30-retail-hive"
  echo '$ find pipeline-storage | sort'
  find pipeline-storage | sort
  echo; echo '$ ls spark-warehouse spark-warehouse/orders_raw'
  ls spark-warehouse; ls spark-warehouse/orders_raw
  echo; echo '$ cat pipeline-storage/_checkpoints/.../ingest_orders/0/offsets/0'
  f=$(find pipeline-storage -path '*ingest_orders/*/offsets/0' | head -1); [ -n "$f" ] && sed "s#$HOME#~#g" "$f" | cut -c1-110
  echo; echo '$ cat pipeline-storage/_checkpoints/.../ingest_orders/0/sources/0/0'
  f=$(find pipeline-storage -path '*ingest_orders/*/sources/0/0' | head -1); [ -n "$f" ] && sed "s#$HOME#~#g" "$f"
) > $O/storage.txt

{ echo "== run --refresh products_by_category"
  SERVER_CONF=$HIVE common/pipeline.sh ch30/retail part06/ch30-retail-hive run --refresh products_by_category | flows
  echo; echo "== run --full-refresh orders_raw"
  SERVER_CONF=$HIVE common/pipeline.sh ch30/retail part06/ch30-retail-hive run --full-refresh orders_raw | flows
  .venv/bin/python ch30/show_tables.py "$D/ch30-retail-hive" orders_raw 2>/dev/null | grep rows
  echo "checkpoint folders of ingest_orders: $(ls "$D"/ch30-retail-hive/pipeline-storage/_checkpoints/spark_catalog/default/orders_raw/ingest_orders | xargs)"
  echo; echo "== run --full-refresh-all --refresh products"
  SERVER_CONF=$HIVE common/pipeline.sh ch30/retail part06/ch30-retail-hive run --full-refresh-all --refresh products | grep -v -E '^\s+(File|raise|\^)|^Traceback|^\s+(main|run|handle|for result)|^[0-9]{4}-' 
} > $O/refresh.txt
