"""Chapter 2: one application on a small real cluster, read from the outside.

local-cluster[2,2,1024] starts a standalone master, two workers and two executor JVMs on
this machine, so the driver, the executors, the BlockManagers and the shuffle are separate
processes, as on a real cluster. The script runs one aggregation with a shuffle, then reads:
the executors and the tasks from the UI's REST API, the shuffle files that the map tasks
wrote on disk, and the event log that the driver wrote.

Classic only. Run from code/:  uv run python ch02/anatomy.py
"""
import collections
import json
import struct
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, ui_api, work, ROOT, TRIPS

w = work("ch02")
(w / "events").mkdir()
(w / "local").mkdir()
spark = session(
    "ch02-anatomy",
    spark_master="local-cluster[2,2,1024]",
    spark_local_dir=str(w / "local"),               # executors put their blockmgr-* folders here
    spark_eventLog_enabled="true",
    spark_eventLog_dir=str(w / "events"),
    spark_sql_shuffle_partitions="6",
    spark_sql_adaptive_enabled="false",              # keep 6 reduce tasks, so each one is visible
    spark_shuffle_service_enabled="false",
    spark_ui_showConsoleProgress="false",
)
sc = spark.sparkContext
import time
while sc._jsc.sc().getExecutorMemoryStatus().size() < 3:   # the driver plus two executors
    time.sleep(0.2)
print(f"master: {sc.master}   default parallelism: {sc.defaultParallelism}\n")

print("executors (REST: /executors)")
for e in sorted(ui_api("executors"), key=lambda e: e["id"]):
    print(f"  {e['id']:>6}  {e['hostPort']:<20} cores {e['totalCores']}  "
          f"storage memory {e['maxMemory'] / 2**20:6.1f} MiB")

trips = spark.read.parquet(str(TRIPS))
by_zone = (trips.groupBy("PULocationID")
           .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue")))
sc.setJobDescription("trips by zone")
rows = by_zone.collect()
print(f"\n{len(rows)} zones back")

print("\njobs and stages (REST: /jobs, /stages)")
jobs = [j for j in ui_api("jobs") if j.get("description") == "trips by zone"]
for j in jobs:
    print(f"  job {j['jobId']}: stages {sorted(j['stageIds'])}")
    for sid in sorted(j["stageIds"]):
        st = ui_api(f"stages/{sid}")[0]
        print(f"    stage {sid}: {st['numTasks']:>2} tasks  shuffle write {st['shuffleWriteBytes']:>7,} B"
              f"  shuffle read {st['shuffleReadBytes']:>7,} B  {st['name'].split(' at ')[0]}")

print("\ntasks: where each one ran, its locality, and where its shuffle bytes came from")
for sid in sorted(s for j in jobs for s in j["stageIds"]):
    tasks = sorted(ui_api(f"stages/{sid}/0/taskList?length=100"), key=lambda t: t["index"])
    print(f"  stage {sid}")
    for t in tasks:
        m = t["taskMetrics"]
        sr = m["shuffleReadMetrics"]
        line = (f"    task {t['index']:>2}  executor {t['executorId']}  {t['taskLocality']:<13}"
                f" {t['duration']:>4} ms")
        if m["inputMetrics"]["recordsRead"]:
            line += f"  rows in {m['inputMetrics']['recordsRead']:>9,}"
        if m["shuffleWriteMetrics"]["bytesWritten"]:
            line += f"  shuffle write {m['shuffleWriteMetrics']['bytesWritten']:>6,} B"
        if sr["localBytesRead"] or sr["remoteBytesRead"]:
            line += (f"  fetched local {sr['localBytesRead']:>5,} B"
                     f" remote {sr['remoteBytesRead']:>5,} B")
        print(line)

print("\nshuffle files on disk (each map task writes one .data file and one .index file)")
files = sorted((w / "local").rglob("shuffle_*"), key=lambda p: p.name)
for p in files:
    rel = p.relative_to(w / "local")
    parts = rel.parts
    short = "/".join(x if len(x) < 12 or "-" not in x else x[:x.index("-") + 5] + "..." for x in parts[1:])
    print(f"  {short:<64} {p.stat().st_size:>6,} B")
print("\nreduce partition sizes in each map output, from the .index files:")
for ix in (p for p in files if p.suffix == ".index"):
    o = struct.unpack(f">{ix.stat().st_size // 8}q", ix.read_bytes())
    print(f"  {ix.name:<22} {[y - x for x, y in zip(o, o[1:])]}  total {o[-1]:,} B")
idx = next(p for p in files if p.suffix == ".index")
raw = idx.read_bytes()
offsets = struct.unpack(f">{len(raw) // 8}q", raw)
print(f"\n{idx.name}: {len(offsets)} big-endian longs = offsets of {len(offsets) - 1} reduce partitions")
print("  offsets:", list(offsets))
print("  bytes per reduce partition:", [b - a for a, b in zip(offsets, offsets[1:])])
print("\nfirst 32 bytes of", idx.name, "as hex:")
print("  " + raw[:32].hex(" ", 8))

print("\ncache the two columns, then scan them again (BlockManager, PROCESS_LOCAL)")
small = trips.select("PULocationID", "total_amount").cache()
sc.setJobDescription("fill cache")
small.count()
sc.setJobDescription("read cache")
small.groupBy("PULocationID").count().collect()
rdd = ui_api("storage/rdd")[0]
print(f"  cached: {rdd['name'].splitlines()[-1].strip()[:34]}...  {rdd['numCachedPartitions']} partitions, "
      f"{rdd['memoryUsed']:,} B in memory")
for part in sorted(ui_api(f"storage/rdd/{rdd['id']}")["partitions"], key=lambda p: p["blockName"]):
    print(f"    block {part['blockName']:<10} {part['memoryUsed']:>10,} B  on executor(s) {part['executors']}")
job = [j for j in ui_api("jobs") if j.get("description") == "read cache"][0]
scan = min(job["stageIds"])
for t in sorted(ui_api(f"stages/{scan}/0/taskList?length=100"), key=lambda t: t["index"]):
    print(f"    read cache: task {t['index']}  executor {t['executorId']}  {t['taskLocality']}")

spark.stop()

import zstandard
logdir = next((w / "events").iterdir())
print(f"\nevent log folder {logdir.name}/")
for f in sorted(logdir.iterdir()):
    print(f"  {f.name}  ({f.stat().st_size:,} B)")
log = next(logdir.glob("events_*"))
text = zstandard.ZstdDecompressor().stream_reader(log.open("rb")).read().decode()
events = [json.loads(line) for line in text.splitlines()]
counts = collections.Counter(e["Event"].rsplit(".", 1)[-1] for e in events)
print(f"\n{log.name} after zstd: {len(events)} events")
for name, n in counts.most_common():
    print(f"  {n:>4}  {name}")

print("\none SparkListenerTaskEnd event, trimmed:")
te = next(e for e in events if e["Event"].endswith("TaskEnd") and e["Stage ID"] == 2)
ti = te["Task Info"]
print(json.dumps({"Event": te["Event"], "Stage ID": te["Stage ID"],
                  "Task Info": {k: ti[k] for k in ("Task ID", "Index", "Executor ID", "Host", "Locality")},
                  "Shuffle Read Metrics": {k: v for k, v in te["Task Metrics"]["Shuffle Read Metrics"].items()
                                           if k in ("Remote Blocks Fetched", "Local Blocks Fetched",
                                                    "Remote Bytes Read", "Local Bytes Read", "Total Records Read")}},
                 indent=1))
