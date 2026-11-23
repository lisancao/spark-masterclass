#!/usr/bin/env bash
# Run ch14/DatasetsConnect.sc in the Scala Connect REPL (spark-shell --remote) against sc://localhost:15117.
# The Ammonite-based REPL needs a terminal, so script(1) provides one; the lines are pasted after start-up.
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_LOCAL_IP=127.0.0.1
{ sleep 25; grep -v '^//' ch14/DatasetsConnect.sc; sleep 60; echo 'exit'; } |
  script -qfc "uv run spark-shell --remote sc://localhost:15117" /dev/null 2>&1 |
  sed 's/\x1b\[[0-9;?]*[a-zA-Z]//g' | tr -d '\r' |
  grep -E '^(CLASS|FIRST|COUNT|GBK_COUNT|SHARE|== Physical|[*+ ]+.*(Serialize|MapElements|MapPartitions|Deserialize|Filter|FileScan|ColumnarToRow))' | cut -c1-150
