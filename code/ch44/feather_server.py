"""Chapter 44, Step 8 (new in 4.3): Project Feather's single-task execution on a Connect server.
Each session can turn it on for itself. The script counts jobs and tasks for each operation from
the 4.3 REST endpoints, and the median time of 20 small queries in each mode.

Run from code/:  flock $HOME/spark-masterclass/data/.bench.lock uv run python ch44/feather_server.py
"""
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ZONES  # noqa: E402
from connect_util import server, session, close, stop, rest  # noqa: E402
from pyspark.sql.connect import functions as F  # noqa: E402

FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"
url = server()
off, on = session(url, user_id="classic-plan"), session(url, user_id="feather")
on.conf.set(FLAG, "true")
print(f"{FLAG}: session off = {off.conf.get(FLAG)}, session on = {on.conf.get(FLAG)}")


def query(s):
    zones = s.read.option("header", True).csv(str(ZONES))
    return zones.groupBy("Borough").agg(F.count("*").alias("zones")).orderBy(F.desc("zones"))


for s in (off, on):                                   # warm up both sessions
    query(s).collect()
for name, s in (("off", off), ("on", on)):
    times, ops = [], []
    for _ in range(20):
        df = query(s)
        t = time.perf_counter()
        df.collect()
        times.append((time.perf_counter() - t) * 1000)
        ops.append(df.executionInfo.operation_id)
    records = {o["operationId"]: o for o in rest("connect/operations")}
    jobs = statistics.mean(len(records[o]["jobIds"]) for o in ops)
    stage_tasks = {j["jobId"]: j["numTasks"] for j in rest("jobs")}
    tasks = statistics.mean(sum(stage_tasks[int(j)] for j in records[o]["jobIds"]) for o in ops)
    print(f"  single-task {name:3}: {statistics.median(times):6.1f} ms median of 20, "
          f"{jobs:.1f} jobs and {tasks:.1f} tasks per operation")
print("first row:", query(on).first())
close(off, on)
stop()
