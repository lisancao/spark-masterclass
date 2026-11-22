"""Chapter 37: what single-task execution does to a plan, to AQE, and to what the Spark UI records.

Classic only (job labels and the UI's REST API). Run from code/:  uv run python ch37/plans_aqe.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, ZONES

spark = session("ch37-plans", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"
zones = lambda: spark.read.option("header", True).csv(str(ZONES))
query = lambda: zones().groupBy("Borough").count().orderBy("Borough")


def short(text, keep=("AdaptiveSparkPlan", "Final Plan", "Initial Plan", "QueryStage", "AQEShuffleRead",
                      "Exchange", "Sort", "HashAggregate", "FileScan", "Scan csv")):
    """The plan lines that matter here, cut at the scan's options."""
    out = []
    for line in text.splitlines():
        if any(k in line for k in keep):
            out.append(line.split(" Batched:")[0].split(", Format:")[0].rstrip()[:96])
    return "\n".join(out)


for enabled in ("false", "true"):
    spark.conf.set(FLAG, enabled)
    print(f"==================== {FLAG}={enabled}")
    df = query()
    print("-- before the run (explain):")
    print(short(plan(df, "simple")))
    label = f"plans {enabled}"
    spark.sparkContext.setJobDescription(label)
    df.collect()
    spark.sparkContext.setJobDescription(None)
    print("-- after collect():")
    print(short(plan(df, "simple")))
    print("-- the aggregate in formatted mode:")
    fmt = plan(df, "formatted").split("\n\n")
    for block in fmt:
        if block.lstrip().startswith("(") and "HashAggregate" in block.splitlines()[0]:
            print("\n".join(l for l in block.splitlines() if l.startswith(("(", "Functions", "Aggregate A"))))
    jobs = [j for j in ui_api("jobs") if j.get("description") == label]
    stages = [s for s in ui_api("stages") if s.get("description") == label]
    print(f"-- the UI: {len(jobs)} jobs, {len(stages)} stages, "
          f"{sum(s['numTasks'] for s in stages if s['status'] == 'COMPLETE')} tasks; "
          f"shuffle write {sum(s['shuffleWriteBytes'] for s in stages):,} B")
    sql = [e for e in ui_api("sql?details=false") if e.get("description") == label][-1]
    print(f"-- SQL tab: execution {sql['id']}, jobs {sql['successJobIds']}, "
          f"duration {sql['duration']} ms")
    print()

# One task for the whole June file: what AQE can no longer do.
print("==================== the June trips (62 MB, 3,837,248 rows), groupBy pickup zone")
for enabled in ("false", "true"):
    spark.conf.set(FLAG, enabled)
    label = f"june {enabled}"
    spark.sparkContext.setJobDescription(label)
    df = spark.read.parquet(str(TRIPS)).groupBy("PULocationID").agg(F.count("*"), F.avg("fare_amount"))
    df.collect()
    spark.sparkContext.setJobDescription(None)
    stages = sorted((s for s in ui_api("stages?status=complete") if s.get("description") == label),
                    key=lambda s: s["stageId"])
    parts = ", ".join(f"stage {s['stageId']}: {s['numTasks']} tasks, "
                      f"{s['executorRunTime']:,} ms task time" for s in stages)
    rows = df.select(F.spark_partition_id().alias("p")).groupBy("p").count().count()
    print(f"{FLAG}={enabled}: {parts}")
    print(f"   partitions of the result that hold rows: {rows}")
    print("   " + " | ".join(l.strip()[:60] for l in plan(df, "simple").splitlines()
                              if "AQEShuffleRead" in l or "ShuffleQueryStage" in l or "ResultQueryStage" in l)
          or "   (no query stages)")
