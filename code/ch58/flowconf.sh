#!/usr/bin/env bash
# Chapter 58: does a decorator spark_conf win over the spec configuration? Count output files.
# Output: output/flowconf.txt
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="$HERE/output"
. "$HERE/../common/sdp.sh"
sdp_venv
W="$BOOK_DATA/work/sdp/ch79f"; rm -rf "$W"; mkdir -p "$W"
R="--remote sc://localhost:$PORT"
trap sdp_stop EXIT
sdp_project "$W/taxi_etl"; P="$W/taxi_etl"; cd "$P"
cat > transformations/conf_test.py <<'PY'
from pyspark import pipelines as dp
from pyspark.sql import DataFrame


@dp.materialized_view(spark_conf={"spark.sql.shuffle.partitions": "2"})
def by_zone_decorator() -> DataFrame:
    return spark.table("clean_trips").groupBy("pickup_zone").count()


@dp.materialized_view
def by_zone_plain() -> DataFrame:
    return spark.table("clean_trips").groupBy("pickup_zone").count()
PY
sdp_server "$P" --conf spark.sql.catalogImplementation=hive
exec > >(clean > "$OUT/flowconf.txt") 2>&1
files() { for t in "$@"; do printf '%-18s %s part files\n' "$t" "$(ls spark-warehouse/$t | grep -c '^part-')"; done; }
x 'cat transformations/conf_test.py'
spec() { printf 'name: taxi_etl\nstorage: file://%s/pipeline-storage\nlibraries:\n  - glob:\n      include: transformations/**\nconfiguration:\n%s\n' "$P" "$1" > spark-pipeline.yml; }
spec '  spark.sql.adaptive.enabled: "false"'
x 'tail -2 spark-pipeline.yml'
x "spark-pipelines run $R 2>&1 | grep -E 'Run is|Exception'"
x 'files by_zone_decorator by_zone_plain'
spec '  spark.sql.adaptive.enabled: "false"
  spark.sql.shuffle.partitions: "3"'
x 'tail -3 spark-pipeline.yml'
x "spark-pipelines run $R 2>&1 | grep -E 'Run is|Exception'"
x 'files by_zone_decorator by_zone_plain'
