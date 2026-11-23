#!/usr/bin/env bash
# Chapter 60: re-test two Spark 4.2.0 notes on 4.3.0. Replace orders_typed in the shop pipeline
# with a variant, run, add the third day, run again, and count silver_orders after each run.
# Run from code/:  ch60/variants.sh > ch60/output/variants.txt
set -uo pipefail
cd "$(dirname "$0")/.."
export SERVER_CONF="spark.sql.catalogImplementation=hive"
for v in dedupe with_column; do
  W="${BOOK_DATA:-../data}/work/part06/ch60-$v"
  echo "== variants/$v.py"
  FRESH=1 DATA=retail common/pipeline.sh ch60/shop part06/ch60-$v prepare
  python3 - "$W/transformations/silver.py" <<'PY'
import re, sys
p = sys.argv[1]; s = open(p).read()
s = re.sub(r"@dp.temporary_view\ndef orders_typed\(\).*?(?=\n\n\n@)", "", s, flags=re.S)
open(p, "w").write(s)
PY
  cp "ch60/variants/$v.py" "$W/transformations/variant_$v.py"
  for run in 1 2; do
    [ $run = 2 ] && cp "$W/data/later/orders_2026-10-03.json" "$W/data/orders/"
    common/pipeline.sh ch60/shop part06/ch60-$v run | grep -E 'Error|Exception|Run is' | grep -v -E '^\s+(File|raise)' | sed -E 's/^.*(pyspark\.errors[a-z.]+Exception)/\1/' | cut -c1-220 | sed -E 's/^[0-9-]+ //'
    .venv/bin/python ch59/show_tables.py "$W" bronze_orders silver_orders 2>/dev/null | grep rows | sed "s/^/  run $run: /"
  done
  echo
done
