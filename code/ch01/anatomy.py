"""Chapter 1: one query, and the jobs, stages and tasks Spark turned it into.

Classic only: it labels jobs through spark.sparkContext, which Spark Connect does not have.
Run from code/:  uv run python ch01/anatomy.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, MONTH_START, MONTH_END

spark = session("ch01-anatomy")

trips = spark.read.parquet(str(TRIPS))
by_hour = (trips
           .where((F.col("tpep_pickup_datetime") >= MONTH_START) &
                  (F.col("tpep_pickup_datetime") < MONTH_END))
           .groupBy(F.hour("tpep_pickup_datetime").alias("hour"))
           .agg(F.count("*").alias("trips"),
                F.round(F.avg("total_amount"), 2).alias("avg_total"))
           .orderBy("hour"))

print("Nothing has run yet. Spark has a plan:\n")
print(plan(by_hour, "simple"))

spark.sparkContext.setJobDescription("busiest hours")
rows = by_hour.collect()          # the action: now Spark schedules work
print(f"{len(rows)} rows back, e.g.\n  {rows[8].asDict()}\n  {rows[18].asDict()}\n")

print("What that one collect() became, from the Spark UI's REST API:\n")
jobs = [j for j in ui_api("jobs") if j.get("description") == "busiest hours"]
for job in sorted(jobs, key=lambda j: j["jobId"]):
    print(f"job {job['jobId']}: {job['status']}, stages {job['stageIds']}")
    for sid in sorted(job["stageIds"]):
        for st in ui_api(f"stages/{sid}"):
            if st["status"] != "COMPLETE":
                continue
            print(f"  stage {sid:>2}  tasks {st['numCompleteTasks']:>3}  "
                  f"input rows {st['inputRecords']:>9,}  "
                  f"shuffle write {st['shuffleWriteRecords']:>6,}  "
                  f"shuffle read {st['shuffleReadRecords']:>6,}")

print("\nInside the scan stage, task by task:\n")
scan = min(s for j in jobs for s in j["stageIds"])
tasks = ui_api(f"stages/{scan}/0/taskList?length=100")
busy = [t for t in tasks if t["taskMetrics"]["inputMetrics"]["recordsRead"] > 0]
for t in sorted(tasks, key=lambda t: t["index"]):
    m = t["taskMetrics"]
    print(f"  task {t['index']:>2}  "
          f"rows {m['inputMetrics']['recordsRead']:>9,}  {t['duration']:>4} ms")
print(f"\n{len(busy)} of {len(tasks)} tasks read any rows.")

import pyarrow.parquet as pq
for f in sorted(TRIPS.glob("*.parquet")):
    md = pq.ParquetFile(f).metadata
    print(f"{f.name}: {f.stat().st_size:,} bytes, {md.num_row_groups} row groups, "
          f"{md.num_rows:,} rows")
