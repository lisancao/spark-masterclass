#!/usr/bin/env bash
# Run spark-pipelines on a copy of a book pipeline project (Part VI, chapters 30 to 34).
#
#   common/pipeline.sh <project> <work-name> [spark-pipelines arguments...]
#
# <project> is a folder under code/ with a spark-pipeline.yml. The first time, or with FRESH=1,
# it is copied to $BOOK_DATA/work/<work-name> together with code/data/$DATA (default: retail)
# as data/, and @WORK@ in the spec becomes the copy's absolute path. Then spark-pipelines runs there, so relative paths such as data/orders resolve.
#   <spark-pipelines arguments> = prepare only copies the project and exits.
#   ICEBERG=1   add the Iceberg runtime for Spark 4.3 (an unreleased build, see notes/part06.md)
#   RAW=1       print the full log (by default WARN lines and Java stack frames are removed)
#   PORT=15002  the local Connect server port the CLI uses (fixed by pyspark, see the chapter)
set -euo pipefail
CODE="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${BOOK_DATA:-$CODE/../data}"
project="$1"; work="$DATA_DIR/work/$2"; shift 2
if [[ "${FRESH:-0}" == 1 ]]; then rm -rf "$work"; fi
if [[ ! -d "$work" ]]; then
  mkdir -p "$(dirname "$work")"
  cp -r "$CODE/$project" "$work"
  cp -r "$CODE/data/${DATA:-retail}" "$work/data"
  # specs in the book say storage: file://@WORK@/...; point them at this copy
  sed -i "s#@WORK@#$work#g" "$work/spark-pipeline.yml"
fi
if [[ "${1:-}" == prepare ]]; then exit 0; fi   # only copy the project
jars=()
if [[ "${ICEBERG:-0}" == 1 ]]; then
  jars=(--jars "$DATA_DIR/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar")
fi
cd "$work"
export SPARK_LOCAL_IP=127.0.0.1
run() { uv run --quiet --project "$CODE" --with pyyaml spark-pipelines "${jars[@]}" "$@" 2>&1; }
if [[ "${RAW:-0}" == 1 ]]; then run "$@"; exit; fi
set +e
run "$@" | grep --line-buffered -v -E '^\s+at |WARN|^Using Spark|^To adjust|^\[Stage|^WARNING: Using incubator|^\s*$' \
  | sed -u -e "s#$HOME#~#g" -e 's/\r.*\r//'
exit "${PIPESTATUS[0]}"
