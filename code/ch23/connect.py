"""Chapter 23: single-task execution over Spark Connect: a per-session setting on the server.

Start the server first:  ch23/server.sh start
Run from code/:          uv run python ch23/connect.py
"""
import statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from common.book import plan, ui_api, ZONES

URL, UI = "sc://localhost:10240", 4124
FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"


def jobs_and_tasks(run):
    """Run run(), and count the jobs and tasks the server ran for it (from the server's REST API)."""
    before = {j["jobId"] for j in ui_api("jobs", port=UI)}
    run()
    new = [j for j in ui_api("jobs", port=UI) if j["jobId"] not in before]
    return len(new), sum(j["numTasks"] - j["numSkippedTasks"] for j in new)


a = SparkSession.builder.remote(URL).getOrCreate()
b = a.newSession()                       # a second session on the same server
print("client:", type(a).__module__, "| server runs Spark", a.version)
a.conf.set(FLAG, "true")
print(f"session a: {FLAG}={a.conf.get(FLAG)}")
print(f"session b: {FLAG}={b.conf.get(FLAG)}")

rows = [(i % 7, float(i)) for i in range(500)]
SHAPES = [
    ("CSV: groupBy, orderBy", lambda s: s.read.option("header", True).csv(str(ZONES))
                                          .groupBy("Borough").count().orderBy("Borough")),
    ("Python list, 500 rows: groupBy", lambda s: s.createDataFrame(rows, "k int, v double").groupBy("k").count()),
    ("pandas, 500 rows: groupBy", lambda s: s.createDataFrame(pd.DataFrame(rows, columns=["k", "v"]))
                                              .groupBy("k").count()),
]
print(f"\n{'query':<32}{'session':>8}{'exchanges':>11}{'jobs':>6}{'tasks':>7}")
for name, build in SHAPES:
    for label, s in (("a", a), ("b", b)):
        df = build(s)
        ex = plan(df, "simple").count("Exchange")
        jobs, tasks = jobs_and_tasks(df.collect)
        print(f"{name:<32}{label:>8}{ex:>11}{jobs:>6}{tasks:>7}")

if "brief" in sys.argv:                  # the film shows the table only
    a.stop()
    sys.exit(0)

# The same small query, timed from the client. A new DataFrame each run, sessions alternating.
build = SHAPES[0][1]
for s in (a, b):
    for _ in range(5):
        build(s).collect()
ta, tb = [], []
for _ in range(20):
    t = time.perf_counter(); build(a).collect(); ta.append((time.perf_counter() - t) * 1000)
    t = time.perf_counter(); build(b).collect(); tb.append((time.perf_counter() - t) * 1000)
print(f"\nCSV groupBy/orderBy from the client, median of 20 after a warm-up:")
print(f"  session b (off): {statistics.median(tb):6.1f} ms")
print(f"  session a (on):  {statistics.median(ta):6.1f} ms")

a.stop()
