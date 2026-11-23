"""Chapter 46: the first query in a new session, with single-task execution off and on.

Each run is a new Python process with a new JVM. Settings alternate run by run.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch46/cold.py
"""
import json, statistics, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

RUNS = 7
if len(sys.argv) > 1:                                      # child
    t0 = time.perf_counter()
    from common.book import session, ZONES
    spark = session("ch46-cold", spark_sql_optimizer_singleTaskExecution_enabled=sys.argv[1],
                    spark_ui_showConsoleProgress="false")
    t1 = time.perf_counter()
    spark.read.option("header", True).csv(str(ZONES)).groupBy("Borough").count().orderBy("Borough").collect()
    t2 = time.perf_counter()
    print(json.dumps({"start": (t1 - t0) * 1000, "first": (t2 - t1) * 1000}))
    sys.exit(0)

res = {"false": [], "true": []}
for _ in range(RUNS):
    for flag in res:
        out = subprocess.run([sys.executable, __file__, flag], capture_output=True, text=True).stdout
        res[flag].append(json.loads(out.strip().splitlines()[-1]))
print(f"first query in a new process (zones CSV: groupBy, orderBy), median of {RUNS} processes per setting\n")
print(f"{'singleTaskExecution':<22}{'session start ms':>18}{'first query ms':>16}")
for flag, rs in res.items():
    print(f"{flag:<22}{statistics.median(r['start'] for r in rs):>18.0f}{statistics.median(r['first'] for r in rs):>16.0f}")
