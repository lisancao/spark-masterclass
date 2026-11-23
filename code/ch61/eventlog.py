"""Chapter 61, Steps 5 and 6: read the event log of a finished application with Spark itself.

Reads the log that observed_app.py wrote: the rolling folder, the event census, one TaskEnd event,
then one row for each task and a diagnosis for each stage, checked against the REST stage list
that observed_app.py saved while the application was alive.

Run from code/:  uv run python ch61/eventlog.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ROOT, session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

LOGS = ROOT / "data" / "ch61" / "eventlog"
PYTHON_TIME = "time to execute Python code"

# --- Step 5: the files -------------------------------------------------------------------------
folder = next(LOGS.glob("eventlog_v2_*"))
print(folder.name + "/")
for p in sorted(folder.iterdir()):
    print(f"  {p.name:52} {p.stat().st_size:>8} bytes")
print("first bytes of the events file:", next(folder.glob("events_*")).read_bytes()[:4].hex(" "))

spark = session("diagnosis", spark_sql_shuffle_partitions="4")
events = spark.read.json(str(folder / "events_*"))      # Spark 4.x reads the .zstd file directly
print(f"\n{events.count()} events, {len(events.columns)} top-level fields (the union of all event types)")
(events.groupBy("Event").count().orderBy(F.desc("count"), "Event")
 .withColumn("Event", F.regexp_replace("Event", "^org.apache.spark.sql.execution.", "o.a.s.s.e."))
 .show(30, truncate=False))

# One TaskEnd event, trimmed: names with spaces, numbers as strings in the accumulables.
raw = next(l for l in spark.read.text(str(folder / "events_*")).toLocalIterator()
           if '"SparkListenerTaskEnd"' in l.value and PYTHON_TIME in l.value)
ev = json.loads(raw.value)
acc = next(a for a in ev["Task Info"]["Accumulables"] if a["Name"] == PYTHON_TIME)
print("one TaskEnd event:")
print("  keys:", list(ev)[:8])
print("  Task Metrics keys:", list(ev["Task Metrics"])[:6], "...")
print("  accumulable:", json.dumps(acc))
print("  Executor Run Time:", json.dumps(ev["Task Metrics"]["Executor Run Time"]))

# --- Step 6: one row for each task, one row for each stage --------------------------------------
desc = (events.where(F.col("Event") == "SparkListenerStageSubmitted")
        .select(F.col("`Stage Info`.`Stage ID`").cast("long").alias("stage_id"),
                F.col("Properties.`spark.job.description`").alias("description"))
        .dropDuplicates(["stage_id"]))

tm, srm = "`Task Metrics`", "`Task Metrics`.`Shuffle Read Metrics`"
python_ms = F.aggregate(F.filter("`Task Info`.Accumulables", lambda a: a["Name"] == PYTHON_TIME),
                        F.lit(0).cast("long"), lambda s, a: s + a["Update"].cast("long"))
tasks = (events.where((F.col("Event") == "SparkListenerTaskEnd")
                      & (F.col("`Task End Reason`.Reason") == "Success"))
         .select(F.col("`Stage ID`").cast("long").alias("stage_id"),
                 F.col("`Task Info`.`Task ID`").cast("long").alias("task_id"),
                 F.col(f"{tm}.`Executor Run Time`").cast("long").alias("run_ms"),
                 (F.col(f"{tm}.`Input Metrics`.`Records Read`")
                  + F.col(f"{srm}.`Total Records Read`")).cast("long").alias("records_in"),
                 F.col(f"{tm}.`Disk Bytes Spilled`").cast("long").alias("disk_spill"),
                 python_ms.alias("python_ms")))

stages = (tasks.groupBy("stage_id").agg(
              F.count("*").alias("tasks"), F.sum("run_ms").alias("run_ms"),
              F.sum("records_in").alias("rows_in"), F.max("records_in").alias("max_rows"),
              F.percentile("records_in", 0.5).cast("long").alias("median_rows"),
              F.sum("disk_spill").alias("spill"), F.sum("python_ms").alias("python_ms"))
          .withColumn("skew", F.round(F.when(F.col("median_rows") > 0,
                                             F.col("max_rows") / F.col("median_rows")), 1))
          .withColumn("flags", F.array_compact(F.array(
              F.when((F.col("tasks") > 1) & (F.col("skew") >= 4), F.lit("skew")),
              F.when(F.col("spill") > 0, F.lit("spill")),
              F.when(F.col("python_ms") >= F.col("run_ms") / 2, F.lit("python")))))
          .join(desc, "stage_id", "left").orderBy("stage_id"))
stages.select("stage_id", "description", "tasks", "run_ms", "rows_in", "max_rows", "median_rows",
              "skew", "spill", "python_ms", "flags").show(truncate=False)

rest = {s["stageId"]: s for s in json.loads((ROOT / "data" / "ch61" / "rest_stages.json").read_text())}
same = all(r.tasks == rest[r.stage_id]["numCompleteTasks"]
           and r.run_ms == rest[r.stage_id]["executorRunTime"]
           and r.rows_in == rest[r.stage_id]["inputRecords"] + rest[r.stage_id]["shuffleReadRecords"]
           and r.spill == rest[r.stage_id]["diskBytesSpilled"] for r in stages.collect())
print("event log totals equal the REST API totals:", same)
spark.stop()
