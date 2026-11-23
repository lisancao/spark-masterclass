#!/usr/bin/env bash
# Chapter 47, extra checks: a spec in another folder, helper modules, what --verbose shows,
# and relative paths on a server in another folder. Output: output/<section>.txt
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="$HERE/output"
. "$HERE/../common/sdp.sh"
sdp_venv
W="$BOOK_DATA/work/sdp/ch78x"; rm -rf "$W"; mkdir -p "$W"
R="--remote sc://localhost:$PORT"
section() { exec > >(clean > "$OUT/$1.txt") 2>&1; }
trap sdp_stop EXIT
sdp_project "$W/taxi_etl"
sdp_server "$W/taxi_etl" --conf spark.sql.catalogImplementation=hive
cd "$W/taxi_etl"

section spec-elsewhere
x 'mkdir -p ../specs && cp spark-pipeline.yml ../specs/dev.yml'
x "spark-pipelines dry-run $R --spec ../specs/dev.yml 2>&1 | grep -E 'Loading pipeline spec|Root directory|Found|Run is|Exception' | cut -c1-200"
x "sed -i 's#include: transformations/\*\*#include: ../taxi_etl/transformations/**#' ../specs/dev.yml && tail -2 ../specs/dev.yml"
x "spark-pipelines dry-run $R --spec ../specs/dev.yml 2>&1 | grep -E 'Root directory|Found|Run is|Exception' | cut -c1-200"
x 'rm -r ../specs'

section helpers
x 'mkdir -p lib && printf "MIN_FARE = 0\n" > lib/rules.py'
x 'sed -i "s/^from pyspark import pipelines as dp/&\nfrom lib.rules import MIN_FARE/" transformations/raw.py'
x "spark-pipelines dry-run $R 2>&1 | grep -E 'Error|Run is'"
x "PYTHONPATH=/opt/other spark-pipelines dry-run $R 2>&1 | grep -E 'Error|Run is'"
x "PYTHONPATH=\$PWD spark-pipelines dry-run $R 2>&1 | grep -E 'Error|Run is'"
x 'sed -i "/from lib.rules/d" transformations/raw.py && rm -r lib'

section verbose
x "spark-pipelines $R --conf spark.sql.shuffle.partitions=8 --verbose dry-run 2>&1 | grep -A6 -E '^Main class:' | cut -c1-120"

section remote
sdp_stop
mkdir -p "$W/elsewhere"
sdp_server "$W/elsewhere" --conf spark.sql.catalogImplementation=hive
x "spark-pipelines run $R 2>&1 | grep -v -E '^  File|^    ' | tail -8 | cut -c1-200; echo \"exit code: \${PIPESTATUS[0]}\""
x 'ls ../elsewhere'
sed -i "s#\"data/#\"$W/taxi_etl/data/#" transformations/raw.py
x 'grep -n "data/" transformations/raw.py'
x "spark-pipelines run $R 2>&1 | grep -E 'Run is'; echo \"exit code: \${PIPESTATUS[0]}\""
x 'ls ../elsewhere ../elsewhere/spark-warehouse'
