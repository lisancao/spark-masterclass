"""Chapter 33: delay scheduling and spark.locality.wait, on a real two-executor cluster (local-cluster mode).

Job 1 caches 8 partitions so that one executor holds 7 of them. Job 2 runs 8 tasks of 1 s on the cache.
The other executor can wait for a local task, or run a task at a lower locality level at once.
Each setting needs a new application, because the scheduler reads spark.locality.wait at start-up.
Run from code/:  uv run python ch33/locality.py   (timings under the bench lock)
"""
import collections, contextlib, fcntl, json, os, sys, time, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import DATA, ui_port


@contextlib.contextmanager
def bench_lock():
    if os.environ.get("BOOK_NO_LOCK"):
        yield
        return
    with open(DATA / ".bench.lock", "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def rest(path):
    base = f"http://localhost:{ui_port()}/api/v1"
    app = json.load(urllib.request.urlopen(f"{base}/applications"))[0]["id"]
    return json.load(urllib.request.urlopen(f"{base}/applications/{app}/{path}"))


def cache_job(i, it):
    import time
    if i == 1:
        time.sleep(6)            # partition 1 is slow, so the other executor computes and caches partitions 2 to 7
    return [i]


def work(x):
    import time
    time.sleep(1)
    return x


def run(label, conf):
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.master("local-cluster[2,1,1024]").appName(f"ch33-locality")
         .config("spark.ui.port", ui_port()).config("spark.ui.showConsoleProgress", "false"))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    while sc._jsc.sc().getExecutorMemoryStatus().size() < 3:      # the driver and two executors
        time.sleep(0.5)
    rdd = sc.parallelize(range(8), 8).mapPartitionsWithIndex(cache_job).cache()
    rdd.count()
    blocks = collections.Counter()
    for e in rest("executors"):
        if e["id"] != "driver":
            blocks[e["id"]] = e["rddBlocks"]
    with bench_lock():
        sc.setJobDescription("job 2: eight 1 s tasks on the cache")
        t0 = time.perf_counter()
        rdd.map(work).count()
        dt = time.perf_counter() - t0
    time.sleep(1)
    st = [s for s in rest("stages") if s.get("description") == "job 2: eight 1 s tasks on the cache"][0]
    tasks = rest(f"stages/{st['stageId']}/{st['attemptId']}/taskList?length=100")
    t_min = min(t["launchTime"] for t in tasks)
    lines = []
    for t in sorted(tasks, key=lambda t: t["launchTime"]):
        launched = (time.mktime(time.strptime(t["launchTime"][:19], "%Y-%m-%dT%H:%M:%S")) + int(t["launchTime"][20:23]) / 1000)
        lines.append((launched, t["executorId"], t["taskLocality"], t["index"]))
    t0l = lines[0][0]
    by_level = collections.Counter(l[2] for l in lines)
    print(f"\n{label}: job 2 took {dt:.1f} s; cached blocks per executor {dict(sorted(blocks.items()))}; "
          f"task locality {dict(by_level)}")
    for launched, ex, loc, idx in lines:
        print(f"    +{launched - t0l:4.1f} s  executor {ex}  partition {idx}  {loc}")
    spark.stop()
    time.sleep(2)
    return dt


print("local-cluster[2,1,1024]: two executor JVMs with one core each, on one host")
res = {}
for label, conf in [("spark.locality.wait=3s (default)", {}),
                    ("spark.locality.wait=0", {"spark.locality.wait": "0"}),
                    ("spark.locality.wait=10s", {"spark.locality.wait": "10s"}),
                    ("3s, legacyResetOnTaskLaunch=true", {"spark.locality.wait.legacyResetOnTaskLaunch": "true"})]:
    res[label] = run(label, conf)
