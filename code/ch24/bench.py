"""Chapter 24: time the taxi queries on one engine, and record its memory use.

Every engine, the CPU baseline too, runs with OFFLOAD_FRIENDLY settings, so all run the
same query. Each query runs WARMUP times untimed, then RUNS times timed; the result is the
median. The script also checks that the rows match the other engines' rows (a hash).
Run under the bench lock, with the Python of the engine's Spark version, for example:
  flock ~/spark-masterclass/data/.bench.lock \
      ~/spark-masterclass/data/ch24/venv-4.2.0/bin/python ch24/bench.py rapids
"""
import hashlib
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ui_api
from ch24.engines import start, queries, walk, native_share, OFFLOAD_FRIENDLY, YEAR

WARMUP, RUNS = int(os.environ.get("WARMUP", 2)), int(os.environ.get("RUNS", 7))
engine = sys.argv[1]


def gpu_used_mib():
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=memory.used",
                              "--format=csv,noheader,nounits"],
                             capture_output=True, text=True, timeout=5).stdout
        return int(out.split()[0])
    except Exception:
        return None


class GpuPeak(threading.Thread):
    """Samples GPU memory every 200 ms while the queries run."""
    def __init__(self):
        super().__init__(daemon=True)
        self.peak, self.stop = 0, threading.Event()

    def run(self):
        while not self.stop.is_set():
            self.peak = max(self.peak, gpu_used_mib() or 0)
            time.sleep(0.2)


gpu_idle = gpu_used_mib()
spark = start(engine, **OFFLOAD_FRIENDLY)
pid = spark._jvm.java.lang.ProcessHandle.current().pid()
sampler = GpuPeak()
sampler.start()

rows_total = spark.read.parquet(str(YEAR)).count()
result = {"engine": engine, "spark": spark.version, "rows": rows_total,
          "warmup": WARMUP, "runs": RUNS, "queries": {}}
for name, df in queries(spark).items():
    for _ in range(WARMUP):
        rows = df.collect()
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        rows = df.collect()
        times.append(time.perf_counter() - t0)
    fingerprint = hashlib.sha1(repr([tuple(r) for r in rows]).encode()).hexdigest()[:10]
    e, s, t = native_share(walk(df))
    result["queries"][name] = {
        "median_s": round(statistics.median(times), 3),
        "min_s": round(min(times), 3), "max_s": round(max(times), 3),
        "rows_hash": fingerprint, "engine_ops": e, "spark_ops": s, "transitions": t}

sampler.stop.set()
sampler.join()
with open(f"/proc/{pid}/status") as f:
    status = dict(l.split(":", 1) for l in f if l.startswith(("VmHWM", "VmRSS")))
time.sleep(11)                                   # one executor heartbeat: peak metrics arrive
peaks = {}
for ex in ui_api("executors"):
    peaks = ex.get("peakMemoryMetrics", {}) or peaks
result["memory"] = {
    "jvm_peak_rss_mib": int(status["VmHWM"].split()[0]) // 1024,
    "jvm_heap_peak_mib": peaks.get("JVMHeapMemory", 0) // 2**20,
    "offheap_execution_peak_mib": peaks.get("OffHeapExecutionMemory", 0) // 2**20,
    "onheap_execution_peak_mib": peaks.get("OnHeapExecutionMemory", 0) // 2**20,
    "gpu_idle_mib": gpu_idle, "gpu_peak_mib": sampler.peak or None,
}

out = Path(__file__).parent / "output" / f"bench-{engine}-{spark.version}.json"
out.write_text(json.dumps(result, indent=2) + "\n")
print(f"{engine} on Spark {spark.version}: {rows_total:,} rows, "
      f"median of {RUNS} runs after {WARMUP} warmup runs")
for name, q in result["queries"].items():
    print(f"  {name:<9} {q['median_s']:>6.2f} s  (min {q['min_s']:.2f}, max {q['max_s']:.2f})"
          f"  rows {q['rows_hash']}  engine/spark ops {q['engine_ops']}/{q['spark_ops']}")
print("  memory:", result["memory"])
