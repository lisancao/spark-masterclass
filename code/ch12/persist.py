"""Chapter 12: RDD persistence levels and checkpoints.

The scan times are medians of 3 after a warmup; quoted timings come from a run under the bench lock.

Each level persists the same RDD of (zone, fare, tip) tuples; the REST API reports where the blocks went.
Run from code/:  uv run python ch12/persist.py
"""
import statistics
import sys
import tempfile
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark import StorageLevel
from common.book import session, ui_api, TRIPS

spark = session("ch12-persist", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
PORT = sc.uiWebUrl.rsplit(":", 1)[1]                        # the port this app got
here = str(Path(__file__).resolve().parent) + "/"
rows = spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount", "tip_amount").rdd.map(tuple)

print("== 1. the levels that PySpark defines")
for name in ("MEMORY_ONLY", "MEMORY_AND_DISK", "MEMORY_AND_DISK_DESER", "DISK_ONLY", "MEMORY_ONLY_2", "OFF_HEAP"):
    lv = getattr(StorageLevel, name)
    print(f"  {name:22} disk={lv.useDisk!s:5} memory={lv.useMemory!s:5} offHeap={lv.useOffHeap!s:5} "
          f"deserialized={lv.deserialized!s:5} replication={lv.replication}")
print("rdd.cache() uses:", rows.cache().getStorageLevel()); rows.unpersist(blocking=True)


def scan(r):
    r.map(lambda x: x[1] or 0.0).sum()                       # warmup
    t = []
    for _ in range(3):
        s = time.perf_counter(); r.map(lambda x: x[1] or 0.0).sum(); t.append(time.perf_counter() - s)
    return statistics.median(t)


print("\n== 2. the same RDD at four levels (REST /storage/rdd)")
print(f"  {'level':22} {'memory':>10} {'disk':>10} {'cached':>7} {'scan':>7}")
print(f"  {'no persist (from file)':22} {'':>10} {'':>10} {'':>7} {scan(rows):6.2f}s")
for name in ("MEMORY_ONLY", "MEMORY_AND_DISK", "DISK_ONLY", "MEMORY_ONLY_2"):
    p = rows.map(lambda x: x).setName(name).persist(getattr(StorageLevel, name))
    p.count()
    info = [r for r in ui_api("storage/rdd", PORT) if r["name"] == name][0]
    print(f"  {name:22} {info['memoryUsed'] / 2**20:7.1f} MiB {info['diskUsed'] / 2**20:7.1f} MiB "
          f"{info['numCachedPartitions']:>3}/{info['numPartitions']:<3} {scan(p):6.2f}s")
    p.unpersist(blocking=True)
print("  (local mode has one executor, so _2 cannot place a second copy)")

print("\n== 3. checkpoint() cuts the lineage and writes files")
ckdir = Path(tempfile.mkdtemp(prefix="ch12-ck-"))
sc.setCheckpointDir(str(ckdir))
loop = rows.map(lambda x: (x[0], x[1] or 0.0))
for i in range(3):
    loop = loop.mapValues(lambda v: v * 1.01).reduceByKey(lambda a, b: a + b)
print("before, lineage lines:", len(loop.toDebugString().decode().splitlines()))
loop.checkpoint()
print("isCheckpointed before an action:", loop.isCheckpointed())
loop.count()
print("isCheckpointed after count():  ", loop.isCheckpointed())
print(loop.toDebugString().decode().replace(here, "").replace(str(ckdir), "<ckdir>"))
files = sorted(p.relative_to(ckdir) for p in ckdir.rglob("part-*"))
print("files:", len(files), "part files, for example", str(files[0]).split("/", 1)[1])

print("\n== 4. localCheckpoint() keeps the blocks on the executors instead")
loop2 = rows.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
loop2.localCheckpoint(); loop2.count()
print("localCheckpoint(), then count():           isCheckpointed =", loop2.isCheckpointed())
loop3 = rows.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
loop3.cache().localCheckpoint(); loop3.count()
print("cache(), localCheckpoint(), then count():  isCheckpointed =", loop3.isCheckpointed())
print(loop3.toDebugString().decode().replace(here, ""))
