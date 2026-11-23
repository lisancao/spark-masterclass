"""Chapter 77, Step 14: graceful decommission on a standalone cluster.

The application caches an RDD on disk and writes shuffle files on two executors. ch77/ops/decom.sh
then sends SIGPWR to one worker. The executor on that worker moves its blocks to the other executor
before it exits, so the next two actions neither recompute the cache nor rerun the map stage.
"""
import json
import time
import urllib.request

from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch77-decom").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("INFO")       # decom.sh reads the decommission lines from the driver log
api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"


def get(path):
    with urllib.request.urlopen(f"{api}/{path}") as r:
        return json.load(r)


def blocks(rdd_id):
    where = {}
    for p in get(f"storage/rdd/{rdd_id}")["partitions"]:
        for e in p["executors"]:
            where[e] = where.get(e, 0) + 1
    return dict(sorted(where.items()))


def executors():
    return sorted(e["id"] for e in get("executors") if e["id"] != "driver")


t0 = time.time()
computed = sc.accumulator(0)                                # rows computed by the map function


def tag(x):
    computed.add(1)
    return (x % 50, x)


cached = sc.parallelize(range(400_000), 8).map(tag).persist(StorageLevel.DISK_ONLY)
shuffled = cached.reduceByKey(lambda a, b: a + b, 4)
n1, n2 = cached.count(), shuffled.count()
print(f"{time.time() - t0:5.1f}s  cache {n1:,} rows, shuffle {n2} keys, rows computed {computed.value:,}")
print(f"        executors {executors()}  cached partitions per executor {blocks(cached.id())}")
print("READY", flush=True)                                  # decom.sh waits for this line
while len(executors()) > 1:
    time.sleep(0.5)
print(f"{time.time() - t0:5.1f}s  one executor left {executors()}  "
      f"cached partitions per executor {blocks(cached.id())}")
n1, n2 = cached.count(), shuffled.count()
print(f"{time.time() - t0:5.1f}s  again: cache {n1:,} rows, shuffle {n2} keys, rows computed {computed.value:,}")
for st in sorted(get("stages"), key=lambda st: (st["stageId"], st["attemptId"])):
    print(f"        stage {st['stageId']}.{st['attemptId']}  {st['status']:<8} tasks {st['numCompleteTasks']}/{st['numTasks']}  "
          f"{st['name'].split(' at ')[0]:<12}  shuffle write {st['shuffleWriteBytes']:>6,} B")
spark.stop()
