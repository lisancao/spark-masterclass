#!/usr/bin/env bash
# Chapter 61, Step 11: write a JSON driver log, then read it back as a DataFrame with SPARK_LOG_SCHEMA.
#   ch61/logs_df.sh      (from code/)
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH=$PWD/.venv/bin:$PATH SPARK_LOCAL_IP=127.0.0.1
W=$PWD/../data/work/ch61-logs; rm -rf "$W"; mkdir -p "$W"
spark-submit --master 'local[2,2]' --conf spark.ui.port="${SPARK_UI_PORT:-4040}" \
  --conf spark.log.structuredLogging.enabled=true ch61/logs_df_app.py > "$W/app.out" 2> "$W/driver.log"
echo "driver.log: $(wc -l < "$W/driver.log") lines; a WARN line as written:"
grep -m1 '"level":"WARN"' "$W/driver.log" | cut -c1-300
python ch61/logs_df.py "$W/driver.log"
