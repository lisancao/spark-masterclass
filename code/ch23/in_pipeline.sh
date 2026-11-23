#!/usr/bin/env bash
# Chapter 23: try to declare a metric view in a pipeline SQL file (dry run).
# Run from code/:  ch23/in_pipeline.sh > ch23/output/in_pipeline.txt
set -uo pipefail
cd "$(dirname "$0")/.."
FRESH=1 common/pipeline.sh ch23/pipeline part06/ch23-pipeline dry-run \
  | grep -E 'Error|Exception|Run is|Registering SQL' | grep -v -E '^\s+(File|raise)' \
  | sed -E 's/^.*(pyspark\.errors[a-z.]+Exception)/\1/' | fold -s -w 110
