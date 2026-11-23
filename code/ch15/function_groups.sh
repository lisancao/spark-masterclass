#!/usr/bin/env bash
# Chapter 15: count the @ExpressionDescription groups in the Spark source at tag v4.3.0.
# One annotation is one expression class. A class can have more than one SQL name
# (for example avg and mean), so the registry in functions.py counts more names than this.
# Run from code/:  ch15/function_groups.sh > ch15/output/function_groups.txt
set -euo pipefail
SRC=${SPARK_SRC:-$HOME/repos/spark}
git -C "$SRC" grep -h -o -E 'group = "[a-z_]+"' v4.3.0 -- '*/src/main/*.scala' \
  | sed -E 's/group = "([a-z_]+)"/\1/' | sort | uniq -c | sort -rn \
  | awk '{printf "  %-20s %4d\n", $2, $1; t += $1} END {printf "  %-20s %4d\n", "total", t}'
