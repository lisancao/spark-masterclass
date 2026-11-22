#!/usr/bin/env bash
# Chapter 74: the `configuration` key of a pipeline spec and the spark_conf of one flow.
# Runs the pipeline, then reads the server's event log to find the shuffle partitions
# that each flow's plan used.
# Run from code/:  ch74/sdp_conf.sh > ch74/output/sdp_conf.txt
set -euo pipefail
cd "$(dirname "$0")/.."
D="${BOOK_DATA:-../data}/work/ch74-sdp"
echo "== spark-pipeline.yml"; sed 's/^/  | /' ch74/sdp_conf/spark-pipeline.yml
echo "== run"
FRESH=1 EVENTLOG=1 CONNECT_PORT=${CONNECT_PORT:-15102} common/pipeline.sh ch74/sdp_conf ch74-sdp run \
  | grep -E 'module scope|Flow .* (COMPLETED|FAILED)|Run is' | sed -E 's/^[0-9-]+ [0-9:]+: //'
echo "== shuffle partitions in each flow's initial plan (from the event log)"
uv run --quiet python - "$D/spark-events" <<'PY'
import json, re, sys, pathlib, zstandard
for f in sorted(pathlib.Path(sys.argv[1]).rglob("events_*")):
    raw = open(f, "rb").read()
    text = zstandard.ZstdDecompressor().stream_reader(raw).read().decode() if f.suffix == ".zstd" else raw.decode()
    for line in text.splitlines():
        e = json.loads(line)
        if e["Event"].endswith("SQLExecutionStart"):
            plan = e["physicalPlanDescription"]
            parts = re.findall(r"hashpartitioning\([^,]*, (\d+)\)", plan)
            table = re.findall(r"by_key_\w+", plan)
            if parts and table:
                print(f"  {table[0]:<12} Exchange hashpartitioning(..., {parts[0]})")
PY
