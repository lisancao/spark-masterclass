#!/usr/bin/env bash
# Run failures.py with the worker's stderr kept, and keep the output readable:
# drop JVM log lines and progress bars, shorten the pyspark.zip paths, keep the first traceback dump.
#   ch89/run_failures.sh > ch89/output/failures.txt
cd "$(dirname "$0")/.."
uv run python ch89/failures.py 2>&1 | tr -d "\r" | sed "s#$HOME#~#g" | sed -E 's/\[Stage [^]]*\] *//g' \
  | grep -v -E '^(WARN|Using Spark|To adjust|Setting default|[0-9]{2}/[0-9]{2}/[0-9]{2} )' \
  | sed -E 's#File "[^"]*/pyspark\.zip/#File ".../pyspark.zip/#; s#File "[^"]*/ch89/#File "ch89/#' \
  | awk '/^Timeout \(/{n++} n>=2 && /^Timeout \(|^Thread|^  File/{next} {print}
         END{if(n>1) print "(the same dump again every 2 s; " n " dumps in all)"}' | cat -s
