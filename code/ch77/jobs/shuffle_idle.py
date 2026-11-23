"""A job for Chapter 77's Celeborn step: one shuffle, an idle gap, then a second action on the same result.

    spark-submit jobs/shuffle_idle.py <taxi dir> <idle seconds>

With dynamic allocation on, executors with no tasks go away after spark.dynamicAllocation.executorIdleTimeout.
The job prints how many executors are alive after the first action and after the gap, then runs the
same action again on the same DataFrame, which needs the shuffle output of the first one. At the end it
prints each stage and each executor from the driver's own REST API, so the output shows which stages
ran, which stages Spark skipped, and why each executor was removed.
"""
import json
import sys
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F

taxi, idle = sys.argv[1], int(sys.argv[2])
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"


def rest(path):
    with urllib.request.urlopen(f"{api}/{path}") as r:
        return json.load(r)


def executors():
    return [e for e in rest("executors") if e["id"] != "driver"]


def say(msg):
    print(f"SHUFFLE t+{time.time() - t0:3.0f}s  {msg}", flush=True)


t0 = time.time()
manager = spark.conf.get("spark.shuffle.manager", "sort")
say(f"spark {spark.version}, shuffle manager {manager.split('.')[-1]}, "
    f"shuffle tracking {spark.conf.get('spark.dynamicAllocation.shuffleTracking.enabled', 'true')}")

by_zone = (spark.read.parquet(f"{taxi}/yellow")
           .groupBy("PULocationID")
           .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue")))

rows = by_zone.collect()                    # action 1: the shuffle runs and writes its output
top = max(rows, key=lambda r: r.trips)
say(f"action 1 done: busiest zone {top.PULocationID}, {top.trips:,} trips; executors alive: {len(executors())}")
time.sleep(idle)
say(f"after {idle} s with no tasks: executors alive: {len(executors())}")
rows = by_zone.collect()                    # action 2: the same DataFrame, so the same shuffle
n = sum(1 for r in rows if r.revenue > 1_000_000)
say(f"action 2 done: {n} zones over $1M; executors alive: {len(executors())}")

for s in sorted(rest("stages"), key=lambda s: s["stageId"]):
    say(f"stage {s['stageId']:>2} {s['status']:<9} tasks={s['numTasks']:<3} "
        f"shuffle write={s['shuffleWriteBytes']:>9,} B  read={s['shuffleReadBytes']:>9,} B")
for e in sorted(rest("allexecutors"), key=lambda e: e["id"]):
    if e["id"] != "driver":
        say(f"executor {e['id']:>2} active={str(e['isActive']):<5} removed: {e.get('removeReason', '-')}")
spark.stop()
