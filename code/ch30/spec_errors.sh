#!/usr/bin/env bash
# Chapter 30: three spec files that the CLI refuses, each with a dry run.
# Run from code/ after ch30/init.sh:  ch30/spec_errors.sh > ch30/output/spec_errors.txt
set -uo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/part06"
rm -rf "$D/ch30-spec" && cp -r "$D/ch30-init/retail_demo" "$D/ch30-spec"
STORAGE="storage: file://$D/ch30-spec/pipeline-storage"
LIBS=$'libraries:\n  - glob:\n      include: transformations/**'
try() {   # $1 = label, $2 = spec text
  echo "== $1"
  printf '%s\n' "$2" | sed "s#$HOME#~#g"
  printf '%s\n' "$2" > "$D/ch30-spec/spark-pipeline.yml"
  echo "--"
  common/pipeline.sh unused part06/ch30-spec dry-run \
    | grep -E 'Error|Exception|Run is' | grep -v 'raise PySpark' | sed -E 's/^.*(pyspark\.errors|Error:)/\1/' | cut -c1-240
  echo
}
try "an unknown key" "name: retail_demo
owner: data-team
$STORAGE
$LIBS"
try "no storage" "name: retail_demo
$LIBS"
try "no libraries" "name: retail_demo
$STORAGE"
