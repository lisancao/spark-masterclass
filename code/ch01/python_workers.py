"""Chapter 1: where Python code runs when a query calls a Python function.

Run from code/:  uv run python ch01/python_workers.py
"""
import subprocess, sys, threading, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

spark = session("ch01-python-workers")
trips = spark.read.parquet(str(TRIPS))

@F.udf("string")
def fare_band(fare):              # plain Python, so it has to run in a Python process
    if fare is None or fare <= 0:
        return "invalid"
    return "under $10" if fare < 10 else "$10 to $30" if fare < 30 else "over $30"

banded = trips.groupBy(fare_band("fare_amount").alias("band")).count().orderBy("band")
print([line.strip() for line in plan(banded, "simple").splitlines() if "EvalPython" in line][0][:70])

def python_processes():
    """Python processes started by this application's JVM: the daemon and the workers it forks."""
    out = subprocess.run(["ps", "-eo", "args"], capture_output=True, text=True).stdout
    return sum(l.split()[1:3] == ["-m", "pyspark.daemon"] for l in out.splitlines() if l.split())

seen = [python_processes()]
done = threading.Event()
def watch():
    while not done.is_set():
        seen.append(python_processes()); time.sleep(0.05)
threading.Thread(target=watch, daemon=True).start()
t = time.perf_counter()
rows = banded.collect()
done.set()
print(f"{time.perf_counter() - t:.1f} s; Python processes before the query: {seen[0]}, at peak: {max(seen)}")
for r in rows:
    print(f"  {r['band']:<12}{r['count']:>10,}")
