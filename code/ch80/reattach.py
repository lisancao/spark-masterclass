"""Chapter 80: a large result on the wire. Arrow batch sizes, and ReattachExecute when a stream ends early.

Start the server with a small stream limit, so that the client must reattach:
  ch04/server.sh start --conf spark.connect.execute.reattachable.senderMaxStreamSize=16m
Run from code/:  uv run python ch80/reattach.py
"""
import json, os, statistics, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from common.book import ROOT, TRIPS
import proxy

PORT = int(os.environ.get("CONNECT_PORT", "15002"))
PROXY = int(os.environ.get("PROXY_PORT", "25002"))
LOG = ROOT / "data" / "ch80-wire" / "reattach.jsonl"
LOG.parent.mkdir(parents=True, exist_ok=True)
server, px = proxy.start(PROXY, f"localhost:{PORT}", LOG)

COLS = ["PULocationID", "DOLocationID", "trip_distance", "fare_amount", "tpep_pickup_datetime"]
with SparkSession.builder.remote(f"sc://localhost:{PROXY}").create() as spark:
    pdf = spark.read.parquet(str(TRIPS)).select(*COLS).toPandas()
    print(f"toPandas: {len(pdf):,} rows, {pdf.memory_usage(index=False).sum() / 2**20:.0f} MiB in pandas")
server.stop(1)

recs = [json.loads(l) for l in LOG.read_text().splitlines()]
calls = Counter(r["method"] for r in recs if r["dir"] == "req")
print("RPCs:", dict(calls))
batches = [r for r in recs if "arrow_bytes=" in r["summary"]]
rows = [int(r["summary"].split("rows=")[1].split()[0]) for r in batches]
size = [int(r["summary"].split("arrow_bytes=")[1].split()[0]) for r in batches]
print(f"Arrow batches: {len(batches)}, rows per batch median {statistics.median(rows):,.0f}"
      f" (max {max(rows):,}), bytes per batch median {statistics.median(size):,.0f}")
print(f"Arrow bytes in total: {sum(size) / 2**20:.0f} MiB for {sum(rows):,} rows"
      f" = {sum(size) / sum(rows):.1f} bytes per row")
print("\nThe stream, call by call (ExecutePlan, then each ReattachExecute):")
per = {}
for r in recs:
    if r["method"] in ("ExecutePlan", "ReattachExecute") and r["dir"] == "resp":
        c = per.setdefault(r["call"], {"m": r["method"], "n": 0, "b": 0, "last": ""})
        c["n"] += 1; c["b"] += r["bytes"]; c["last"] = r["summary"].replace("response_type=", "").split()[0] if r["summary"] else ""
for n, c in list(per.items())[:4] + [("...", None)] + list(per.items())[-2:]:
    if c is None:
        print("  ...")
        continue
    print(f"  {c['m']:16} {c['n']:>4} responses {c['b'] / 2**20:6.1f} MiB  last: {c['last']}")
