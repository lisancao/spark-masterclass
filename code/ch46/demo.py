"""Chapter 46: a short demo for the film: one small query and the June file, single-task execution off and on.

Run from code/:  uv run python ch46/demo.py
"""
import statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, ZONES

spark = session("ch46-demo", spark_ui_showConsoleProgress="false")
FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"
QUERIES = {
    "zones CSV, groupBy+orderBy": lambda: (spark.read.option("header", True).csv(str(ZONES))
                                           .groupBy("Borough").count().orderBy("Borough")),
    "June 62 MB, groupBy": lambda: spark.read.parquet(str(TRIPS)).groupBy("PULocationID").count(),
}
for name, build in QUERIES.items():
    print(f"\n{name}")
    for flag in ("false", "true"):
        spark.conf.set(FLAG, flag)
        build().collect()
        label = f"{name} {flag}"
        spark.sparkContext.setJobDescription(label)
        ts = []
        for _ in range(7):
            t = time.perf_counter(); build().collect(); ts.append((time.perf_counter() - t) * 1000)
        spark.sparkContext.setJobDescription(None)
        jobs = [j for j in ui_api("jobs") if j.get("description") == label]
        tasks = sum(j["numTasks"] - j["numSkippedTasks"] for j in jobs) / 7
        ex = plan(build(), "simple").count("Exchange")
        print(f"  singleTask={flag:<6} Exchange nodes {ex}   jobs/query {len(jobs) / 7:.0f}   "
              f"tasks/query {tasks:4.0f}   {statistics.median(ts):6.0f} ms")
