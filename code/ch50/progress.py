"""Chapter 50: the streaming progress that each pipeline run wrote to the Spark event log.

  python ch50/progress.py <work folder>
Each server start writes one event log (a folder eventlog_v2_* with zstd-compressed files) in
<work>/spark-events, so there is one log for each run. The script reads them oldest first and
prints one line for each micro-batch of each streaming flow.
"""
import json
import subprocess
import sys
from pathlib import Path

logs = sorted(Path(sys.argv[1], "spark-events").glob("eventlog_v2_*"), key=lambda p: p.stat().st_mtime)
for run, folder in enumerate(logs, 1):
    print(f"run {run}:")
    batches = 0
    for part in sorted(folder.glob("events_*")):
        text = subprocess.run(["zstd", "-dc", str(part)], capture_output=True, text=True).stdout
        for line in text.splitlines():
            e = json.loads(line)
            if e["Event"].endswith("QueryStartedEvent"):
                print(f"  started   {e['name']}")
            elif e["Event"].endswith("QueryProgressEvent"):
                p, batches = e["progress"], batches + 1
                src = p["sources"][0]
                print(f"  progress  {p['name']}  batch {p['batchId']}  input rows {src['numInputRows']}"
                      f"  offsets {src['startOffset']} -> {src['endOffset']}")
            elif e["Event"].endswith("QueryTerminatedEvent"):
                print(f"  stopped   {e['id'][:8]}...  exception: {e.get('exception')}")
    if batches == 0:
        print("  (no micro-batch)")
