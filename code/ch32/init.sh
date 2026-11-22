#!/usr/bin/env bash
# Chapter 32: what `spark-pipelines init` makes, and a dry run and a run of it.
# Run from code/:  ch32/init.sh > ch32/output/init.txt
set -euo pipefail
CODE="$(cd "$(dirname "$0")/.." && pwd)"
W="${BOOK_DATA:-$CODE/../data}/work/part06/ch32-init"
rm -rf "$W" && mkdir -p "$W" && cd "$W"
echo '$ spark-pipelines init --name retail_demo'
uv run --quiet --project "$CODE" --with pyyaml spark-pipelines init --name retail_demo 2>&1 \
  | grep -v -E 'WARN|^Using Spark' | sed "s#$HOME#~#g"
echo; echo '$ find retail_demo -type f'
find retail_demo -type f | sort
for f in retail_demo/spark-pipeline.yml retail_demo/transformations/*; do
  echo; echo "\$ cat $f"; sed "s#$HOME#~#g" "$f"
done
