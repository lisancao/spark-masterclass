#!/usr/bin/env bash
# Chapter 78: with no --remote, the CLI starts its own server (port 15002). Where do relative
# paths, spark-warehouse/ and metastore_db/ go when you run the CLI from a subfolder?
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="$HERE/output"
. "$HERE/../common/sdp.sh"
sdp_venv
if (exec 3<>/dev/tcp/127.0.0.1/15002) 2>/dev/null; then echo "15002 is in use" >&2; exit 1; fi
W="$BOOK_DATA/work/sdp/ch78l"; rm -rf "$W"; mkdir -p "$W"
sdp_project "$W/taxi_etl"; cd "$W/taxi_etl"
exec > >(clean > "$OUT/local-cwd.txt") 2>&1
x 'mkdir -p transformations/sub && cd transformations/sub'
x "spark-pipelines --conf spark.ui.port=$UI run 2>&1 | grep -E 'Loading pipeline spec|Exception|Run is' | cut -c1-160"
x 'cd ../..'
x "spark-pipelines --conf spark.ui.port=$UI --conf spark.sql.catalogImplementation=hive run 2>&1 | grep -E 'Exception|Run is' | cut -c1-160"
x 'ls'
