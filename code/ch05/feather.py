"""Chapter 5: what a small query costs in tasks, with and without 4.3's single-task execution.

Classic only: it labels jobs through spark.sparkContext, which Spark Connect does not have.
Run from code/:  uv run python ch05/feather.py
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, ui_api, ZONES

spark = session("ch05-feather")
FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"

def zones_by_borough():
    # a fresh DataFrame each time, the way a notebook cell or a unit test builds one
    return (spark.read.option("header", True).csv(str(ZONES))
            .groupBy("Borough").count().orderBy("Borough"))

def run(enabled, runs=20, label="measure"):
    spark.conf.set(FLAG, enabled)
    spark.sparkContext.setJobDescription(f"{label} feather={enabled}")
    t = time.perf_counter()
    for _ in range(runs):
        rows = zones_by_borough().collect()
    ms = (time.perf_counter() - t) / runs * 1000
    jobs = [j for j in ui_api("jobs?status=succeeded") if j.get("description") == f"{label} feather={enabled}"]
    per_query = len(jobs) / runs
    tasks = sum(j["numTasks"] - j["numSkippedTasks"] for j in jobs) / runs
    return rows, ms, per_query, tasks

run("false", 10, "warmup"); run("true", 10, "warmup")            # warm both settings up first
for enabled in ("false", "true"):
    rows, ms, jobs, tasks = run(enabled)
    print(f"{FLAG}={enabled}: {ms:6.1f} ms per query, {jobs:.1f} jobs, {tasks:.1f} tasks")
print("\n" + "\n".join(f"  {r['Borough']:<14}{r['count']}" for r in rows))
