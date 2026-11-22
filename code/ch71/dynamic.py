"""Chapter 71: dynamic allocation, watched through the event log.

local-cluster[3,2,1024] gives three workers with two cores each. The application starts with
zero executors. A job with a backlog of tasks makes ExecutorAllocationManager ask for more;
after the job, the idle executors go away. spark.dynamicAllocation.shuffleTracking.enabled
keeps an executor alive while it holds shuffle files that a later stage can still need.

Classic only. Run from code/:  uv run python ch71/dynamic.py
"""
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import zstandard
from common.book import session, work

w = work("ch71-dyn")
(w / "events").mkdir()
spark = session(
    "ch71-dynamic",
    spark_master="local-cluster[3,2,1024]",
    spark_eventLog_enabled="true",
    spark_eventLog_dir=str(w / "events"),
    spark_dynamicAllocation_enabled="true",
    spark_dynamicAllocation_shuffleTracking_enabled="true",
    spark_dynamicAllocation_minExecutors="0",
    spark_dynamicAllocation_initialExecutors="0",
    spark_dynamicAllocation_maxExecutors="3",
    spark_dynamicAllocation_schedulerBacklogTimeout="1s",
    spark_dynamicAllocation_executorIdleTimeout="5s",
    spark_dynamicAllocation_shuffleTracking_timeout="10s",
    spark_ui_showConsoleProgress="false",
)
sc = spark.sparkContext
t0 = time.time()


def busy(i):
    import time as t
    t.sleep(0.5)                  # half a second of "work" per element
    return i % 4, 1


print("t=0: application started, executors:", sc._jsc.sc().getExecutorMemoryStatus().size() - 1)
sc.setJobDescription("backlog of 48 tasks")
counts = sc.parallelize(range(48), 48).map(busy).reduceByKey(lambda a, b: a + b).collect()
print(f"t={time.time() - t0:.0f}s: job done, {sorted(counts)}")
time.sleep(25)                    # idle: watch the executors go
print(f"t={time.time() - t0:.0f}s: after idling, executors:",
      sc._jsc.sc().getExecutorMemoryStatus().size() - 1)
spark.stop()

logdir = next((w / "events").iterdir())
log = next(logdir.glob("events_*"))
text = zstandard.ZstdDecompressor().stream_reader(log.open("rb")).read().decode()
events = [json.loads(line) for line in text.splitlines()]
start = next(e["Timestamp"] for e in events if e["Event"] == "SparkListenerApplicationStart")
print("\nexecutor and job events from the event log (seconds after application start):")
for e in events:
    name = e["Event"].replace("SparkListener", "")
    if name in ("ExecutorAdded", "ExecutorRemoved"):
        why = f"  reason: {e['Removed Reason']}" if name == "ExecutorRemoved" else ""
        print(f"  {(e['Timestamp'] - start) / 1000:6.1f}  {name:<16} executor {e['Executor ID']}{why}")
    elif name in ("JobStart", "JobEnd"):
        ts = e["Submission Time"] if name == "JobStart" else e["Completion Time"]
        print(f"  {(ts - start) / 1000:6.1f}  {name:<16} job {e['Job ID']}")
