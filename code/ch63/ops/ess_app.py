"""Chapter 63, Step 12: dynamic allocation on a standalone cluster with the external shuffle service.

The first action writes shuffle files. Then the application waits until dynamic allocation removes
every executor. The second action reads the same shuffle: new executors fetch the map output from
the shuffle service on each worker, and Spark skips the map stage.
"""
import json
import sys
import time
import urllib.request

from pyspark.sql import SparkSession

trips = sys.argv[1]
spark = SparkSession.builder.appName("ch63-ess").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"


def get(path):
    with urllib.request.urlopen(f"{api}/{path}") as r:
        return json.load(r)


def live():
    return sorted(e["id"] for e in get("executors") if e["id"] != "driver")


t0 = time.time()
rdd = spark.read.parquet(trips).select("PULocationID").rdd.map(lambda r: (r[0], 1)) \
    .reduceByKey(lambda a, b: a + b, 8)
print(f"{time.time() - t0:5.1f}s  action 1: {rdd.count()} zones   executors {live()}")
while live():
    time.sleep(1)
print(f"{time.time() - t0:5.1f}s  idle timeout passed            executors {live()}")
print(f"{time.time() - t0:5.1f}s  action 2: {rdd.count()} zones   executors {live()}")
for s in sorted(get("stages?status=SKIPPED") + get("stages?status=COMPLETE"), key=lambda s: s["stageId"]):
    print(f"        stage {s['stageId']}  {s['status']:<8}  tasks {s['numTasks']:>2}  "
          f"shuffle read {s['shuffleReadBytes']:>7,} B  write {s['shuffleWriteBytes']:>7,} B")
removed = [e for e in get("allexecutors") if not e["isActive"]]
print(f"        executors removed during the run: {len(removed)}, "
      f"reasons: {sorted({e.get('removeReason', '')[:40] for e in removed})}")
spark.stop()
