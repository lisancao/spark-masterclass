"""Chapter 47: why bench.py builds a new DataFrame for each timed run.

Collects the same DataFrame two times, then a new DataFrame with the same query, and
counts the stages that each collect() ran and skipped (from the monitoring REST API).
Run from code/:  uv run python ch47/reuse.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ui_api
from ch47.engines import start, queries, OFFLOAD_FRIENDLY

spark = start("cpu", **OFFLOAD_FRIENDLY)
print(f"Spark {spark.version}, the boroughs query\n")
same = queries(spark)["boroughs"]
for label, df in [("first collect()", same), ("same DataFrame again", same),
                  ("new DataFrame, same query", queries(spark)["boroughs"])]:
    spark.sparkContext.setJobGroup(label, label)
    df.collect()
    jobs = [j for j in ui_api("jobs") if j.get("jobGroup") == label]
    done = sum(j["numCompletedStages"] for j in jobs)
    skipped = sum(j["numSkippedStages"] for j in jobs)
    rows = sum(s["inputRecords"] for s in ui_api("stages")
               if s["stageId"] in {i for j in jobs for i in j["stageIds"]})
    print(f"  {label:<27} stages run {done:>2}, skipped {skipped:>2}, input rows {rows:>11,}")
