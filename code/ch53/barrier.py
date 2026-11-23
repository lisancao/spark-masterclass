"""Chapter 53, Step 12: barrier execution mode. All tasks of a stage start together, or none starts.

Run from code/:  uv run python ch53/barrier.py
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark import BarrierTaskContext  # noqa: E402
from common.book import session, ui_api  # noqa: E402

spark = session("ch53-barrier", spark_master="local[4,2]", spark_ui_showConsoleProgress="false",
                **{"spark.scheduler.barrier.maxConcurrentTasksCheck.interval": "1s",      # default 15s
                   "spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures": "3"})   # default 40
sc = spark.sparkContext


def rendezvous(it):
    ctx = BarrierTaskContext.get()
    start = time.time()
    time.sleep(0.2 * ctx.partitionId())                  # tasks reach the barrier at different times
    arrived = time.time()
    msgs = ctx.allGather(f"{ctx.partitionId()}@{os.getpid()}")   # waits for every task of the stage
    left = time.time()
    peers = [i.address for i in ctx.getTaskInfos()]
    yield (ctx.partitionId(), start, arrived, left, msgs, len(peers))


print("== RDD.barrier().mapPartitions with allGather, 4 partitions on local[4]")
sc.setJobDescription("barrier allGather")
rows = sorted(sc.parallelize(range(4), 4).barrier().mapPartitions(rendezvous).collect())
t0 = min(r[1] for r in rows)                             # the first task start, as time zero
for pid, start, arrived, left, msgs, n in rows:
    print(f"  task {pid}: started {start - t0:.2f} s, at the barrier {arrived - t0:.2f} s, "
          f"left {left - t0:.2f} s, {n} peers, allGather -> {len(msgs)} messages")

print("\n== the same DataFrame API: mapInPandas(..., barrier=True)")


def tag(batches):
    ctx = BarrierTaskContext.get()
    ctx.barrier()
    for b in batches:
        b["task"] = ctx.partitionId()
        yield b


df = spark.range(8, numPartitions=4).mapInPandas(tag, "id long, task int", barrier=True)
print("  rows per task:", sorted((r.task, r["count"]) for r in df.groupBy("task").count().collect()))
print("  the plan:", [line.strip() for line in df._jdf.queryExecution().executedPlan().toString().splitlines()
                      if "MapInPandas" in line][0][:80])

print("\n== more barrier tasks than task slots: 6 partitions on local[4]")
sc.setLogLevel("OFF")
t0 = time.time()
try:
    sc.parallelize(range(6), 6).barrier().mapPartitions(rendezvous).collect()
except Exception as e:
    msg = " ".join(str(e).split())
    i = msg.find("[SPARK_JOB_CANCELLED]") if "[SPARK_JOB_CANCELLED]" in msg else msg.find("Barrier execution mode")
    print(f"  failed after {time.time() - t0:.1f} s: {type(e).__name__}")
    print("  " + msg[i:i + 200])

print("\n== a failed barrier task (task 1 fails once)")
marker = Path(os.environ.get("BOOK_DATA", "/tmp")) / "work" / "ch53-barrier-failed-once"
marker.parent.mkdir(parents=True, exist_ok=True)


def fail_once(it):
    ctx = BarrierTaskContext.get()
    if ctx.partitionId() == 1 and not marker.exists():
        marker.touch()
        raise RuntimeError("task 1 fails in the first attempt")
    ctx.barrier()
    yield (ctx.partitionId(), ctx.attemptNumber(), ctx.taskAttemptId())


sc.setLogLevel("OFF")
marker.unlink(missing_ok=True)
try:
    sc.parallelize(range(4), 4).barrier().mapPartitions(fail_once).collect()   # the barrier stage is the result stage
except Exception as e:
    msg = " ".join(str(e).split())
    i = msg.find("Could not recover")
    print("  barrier stage as the ResultStage:   job failed: " + msg[i:i + 62])

marker.unlink(missing_ok=True)
sc.setJobDescription("barrier retry")
rows = sorted(sc.parallelize(range(4), 4).barrier().mapPartitions(fail_once)
              .repartition(1).collect())                                       # now it is a ShuffleMapStage
print("  barrier stage as a ShuffleMapStage: (partition, attemptNumber, taskAttemptId)", rows)
job = [j for j in ui_api("jobs") if j.get("description") == "barrier retry"][0]
for st in sorted((s for s in ui_api("stages") if s["stageId"] in job["stageIds"]),
                 key=lambda s: (s["stageId"], s["attemptId"])):
    print(f"    stage {st['stageId']} attempt {st['attemptId']}: {st['status']:<8} "
          f"{st['numCompleteTasks']} complete, {st['numFailedTasks']} failed, {st['numKilledTasks']} killed")
spark.stop()
