"""Chapter 19: a workload for the Spark UI walkthrough. It runs, prints what the UI will show, and waits.

The workload, on the default 1g heap: cache the cleaned June trips (Storage tab), a sort that spills (Stages tab),
and an aggregate with AQE (SQL / DataFrame tab). Then it keeps the application alive until the
file named in UI_TOUR_DONE exists, so that a browser can record the UI.
Run from code/:  uv run python ch19/ui_tour.py
"""
import os, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, ui_api, ui_port, TAXI, TRIPS

spark = session("ch19-ui-tour", spark_sql_session_timeZone="UTC", spark_driver_memory="4g",
                spark_ui_showConsoleProgress="false")
sc = spark.sparkContext

sc.setJobDescription("1. sort six months by distance, 2 partitions, AQE off")
spark.conf.set("spark.sql.shuffle.partitions", "2")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.read.parquet(str(TAXI / "yellow-h1")).orderBy("trip_distance").write.format("noop").mode("overwrite").save()
spark.conf.unset("spark.sql.shuffle.partitions")
spark.conf.unset("spark.sql.adaptive.enabled")

sc.setJobDescription("2. cache the cleaned June trips")
clean = (spark.read.parquet(str(TRIPS))
         .where((F.col("fare_amount") > 0) & F.col("trip_distance").between(0.1, 100))
         .select("tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount", "tip_amount")
         .cache())
clean.count()

sc.setJobDescription("3. tips by pickup zone, from the cache")
clean.groupBy("PULocationID").agg(F.avg("tip_amount").alias("avg_tip"), F.count("*").alias("n")) \
     .orderBy(F.desc("n")).limit(10).collect()
sc.setJobDescription(None)

# What the UI shows, from the same REST API that the pages read.
print(f"UI on port {ui_port()}")
for j in sorted(ui_api("jobs"), key=lambda j: j["jobId"]):
    print(f"  job {j['jobId']:>2}  {j['status']:<10} stages {len(j['stageIds'])}  tasks {j['numTasks']:>3}  "
          f"{(j.get('description') or '')[:50]}")
stages = sorted(ui_api("stages?status=complete"), key=lambda s: s["stageId"])
for s_ in stages:
    print(f"  stage {s_['stageId']:>2}  tasks {s_['numTasks']:>3}  spill memory {s_['memoryBytesSpilled'] / 2**20:6,.0f} MiB  "
          f"disk {s_['diskBytesSpilled'] / 2**20:5,.0f} MiB  {(s_.get('description') or '')[:40]}")
sort = max(stages, key=lambda s: s["memoryBytesSpilled"])
print(f"the stage that spilled: {sort['stageId']}, {sort['numTasks']} tasks, "
      f"spill memory {sort['memoryBytesSpilled'] / 2**20:,.0f} MiB, disk {sort['diskBytesSpilled'] / 2**20:,.0f} MiB")
st = ui_api("storage/rdd")[0]
print(f"storage: {st['numCachedPartitions']}/{st['numPartitions']} partitions, "
      f"{st['memoryUsed'] / 2**20:,.0f} MiB in memory, level {st['storageLevel']}")
sql = [e for e in ui_api("sql?details=false") if e.get("description", "").startswith("3.")][-1]
print(f"SQL execution {sql['id']} ('{sql['description']}'), jobs {sql['successJobIds']}")
Path(os.environ.get("UI_TOUR_READY", "/tmp/masterclass-ch19/ui_ready")).write_text(f"{sort['stageId']} {sql['id']}")
sys.stdout.flush()

done = Path(os.environ.get("UI_TOUR_DONE", "/tmp/masterclass-ch19/ui_done"))
for _ in range(900):
    if done.exists():
        break
    time.sleep(1)
