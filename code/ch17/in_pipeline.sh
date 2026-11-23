#!/usr/bin/env bash
# Chapter 17: try to declare a metric view in a pipeline SQL file (dry run).
# Run from code/:  ch17/in_pipeline.sh > ch17/output/in_pipeline.txt
set -uo pipefail
cd "$(dirname "$0")/.."
FRESH=1 common/pipeline.sh ch17/pipeline part06/ch17-pipeline dry-run \
  | grep -E 'Error|Exception|Run is|Registering SQL' | grep -v -E '^\s+(File|raise)' \
  | sed -E 's/^.*(pyspark\.errors[a-z.]+Exception)/\1/' | fold -s -w 110
