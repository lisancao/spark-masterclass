#!/usr/bin/env bash
# Chapter 59: what pipeline code may not do with the session, one variant file at a time.
# Run from code/:  ch59/session_rules.sh > ch59/output/session_rules.txt
set -uo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/part06/ch59-session"
FRESH=1 common/pipeline.sh ch59/session_rules part06/ch59-session prepare
for v in conf_set temp_view builder count called; do
  echo "== variants/$v.py"
  sed 's/^/  | /' "ch59/session_rules/variants/$v.py"
  rm -f "$D"/transformations/variant_*.py
  cp "ch59/session_rules/variants/$v.py" "$D/transformations/variant_$v.py"
  common/pipeline.sh ch59/session_rules part06/ch59-session dry-run \
    | grep -E 'Exception|Error|Run is|builder|module scope|query function' | grep -v -E 'raise PySpark|^ {4}' \
    | sed -E 's/^.*(pyspark\.errors\.exceptions\.[a-z]+\.)/\1/' | fold -s -w 120
  echo
done
