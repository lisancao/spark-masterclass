#!/usr/bin/env bash
# Chapter 57: each spec key, and the error for each kind of bad spec (dry runs).
# Output: output/spec.txt.   CONNECT_PORT=15104 SPARK_UI_PORT=4144 bash ch57/spec.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="$HERE/output"
. "$HERE/../common/sdp.sh"
sdp_venv
W="$BOOK_DATA/work/sdp/ch78s"; rm -rf "$W"; mkdir -p "$W"
trap sdp_stop EXIT
sdp_project "$W/taxi_etl"; P="$W/taxi_etl"; cd "$P"
sdp_server "$P" --conf spark.sql.catalogImplementation=hive
python - "$PORT" <<'PY' >/dev/null 2>&1
import sys
from pyspark.sql import SparkSession
SparkSession.builder.remote(f"sc://localhost:{sys.argv[1]}").getOrCreate().sql("CREATE DATABASE taxi_dev")
PY
printf "print(\"current database:\", spark.catalog.currentDatabase())\n" > transformations/where.py
exec > >(clean > "$OUT/spec.txt") 2>&1
S="storage: file://$P/pipeline-storage"
L=$'libraries:\n  - glob:\n      include: transformations/**'
try() {   # $1 = label, $2 = spec text
  echo "== $1"
  printf '%s\n' "$2" > spark-pipeline.yml
  sed "s#$HOME#~#g" spark-pipeline.yml | sed 's/^/  | /'
  spark-pipelines dry-run --remote "sc://localhost:$PORT" 2>&1 \
    | grep -E 'Error|Exception|Run is|current' | grep -v 'raise PySpark' \
    | sed -E 's/^.*(pyspark\.errors\.exceptions\.[a-z]+\.)/\1/; s/^[0-9-]+ [0-9:]+: //' | cut -c1-200
  echo
}
try "the smallest spec" "name: taxi_etl
$S
$L"
try "no name" "$S
$L"
try "no storage" "name: taxi_etl
$L"
try "an unknown key" "name: taxi_etl
owner: data-team
$S
$L"
try "a relative storage path" "name: taxi_etl
storage: pipeline-storage
$L"
try "schema, another name for database" "name: taxi_etl
$S
schema: taxi_dev
$L"
try "database and schema together" "name: taxi_etl
$S
database: default
schema: taxi_dev
$L"
try "configuration as a list" "name: taxi_etl
$S
configuration:
  - spark.sql.shuffle.partitions=8
$L"
try "a key that is not a string" "name: taxi_etl
$S
configuration:
  8: spark.sql.shuffle.partitions
$L"
try "libraries with no glob key" "name: taxi_etl
$S
libraries:
  - transformations/**"
try "no libraries" "name: taxi_etl
$S"
