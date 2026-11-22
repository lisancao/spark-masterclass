#!/usr/bin/env bash
# Chapter 30: dry-run a pipeline with three bugs, and fix one bug after each run.
# Run from code/:  ch30/dry_run_errors.sh > ch30/output/dry_run_errors.txt
set -uo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/part06/ch30-broken"
F="$D/transformations/revenue.py"
FRESH=1 common/pipeline.sh ch30/broken part06/ch30-broken prepare
dry() {
  common/pipeline.sh ch30/broken part06/ch30-broken dry-run \
    | grep -v -E '^\s+(File|raise|\^)|Traceback|handle_pipeline_events|for result in|main\(\)|run\($|_handle|^\s*$' \
    | grep -v -E 'Loading|Creating|Registering|Found|Importing|Starting run' | fold -s -w 120
}
echo "== dry run 1"; dry
sed -i 's/spark.table("top_customers").groupBy/spark.table("orders_enriched").groupBy/' "$F"
echo; echo "== fix 1: revenue_by_customer reads orders_enriched. dry run 2"; dry
sed -i 's/"orders_bronz"/"orders_bronze"/' "$F"
echo; echo "== fix 2: orders_bronz -> orders_bronze. dry run 3"; dry
sed -i 's/groupBy("region")/groupBy("city")/' "$F"
echo; echo "== fix 3: region -> city. dry run 4"; dry
echo; echo "== what the dry run wrote"
echo "files under spark-warehouse/ and pipeline-storage/: $(find "$D/spark-warehouse" "$D/pipeline-storage" -type f 2>/dev/null | wc -l)"
