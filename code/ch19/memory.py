"""Chapter 19: the unified memory model, read live from the JVM for several heap sizes.

Classic only (Py4J). Each configuration runs in its own process, because driver memory is fixed at launch.
Run from code/:  uv run python ch19/memory.py
"""
import json, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

CONFIGS = [
    ("defaults", {}),
    ("driver 2g", {"spark.driver.memory": "2g"}),
    ("driver 2g, off-heap 256m", {"spark.driver.memory": "2g", "spark.memory.offHeap.enabled": "true",
                                  "spark.memory.offHeap.size": "256m"}),
    ("driver 4g, fraction 0.8", {"spark.driver.memory": "4g", "spark.memory.fraction": "0.8"}),
    ("driver 400m", {"spark.driver.memory": "400m"}),
]

if len(sys.argv) > 1:                       # child: start a session with these settings and report
    from common.book import session
    conf = json.loads(sys.argv[1])
    try:
        spark = session("ch19-memory", **{k.replace(".", "_"): v for k, v in conf.items()})
    except Exception as e:
        msg = str(e)
        print(json.dumps({"error": msg[msg.find("["):].splitlines()[0][:160] if "[" in msg else msg.splitlines()[0][:160]}))
        sys.exit(0)
    # SparkEnv.get().memoryManager() through Py4J fails on 4.3.0 (see the notes), so read the same
    # numbers from the executors endpoint of the REST API, which the Executors tab also shows.
    from common.book import ui_api
    ex = ui_api("executors")[0]
    m = ex["memoryMetrics"]
    print(json.dumps({
        "heap": spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
        "storage_on": m["totalOnHeapStorageMemory"],
        "storage_off": m["totalOffHeapStorageMemory"],
        "max": ex["maxMemory"],
        "fraction": float(spark.conf.get("spark.memory.fraction", "0.6")),
    }))
    sys.exit(0)

MiB = 2 ** 20
print(f"{'configuration':<28}{'heap':>8}{'(heap-300)*f':>14}{'on-heap pool':>14}{'off-heap':>10}{'maxMemory':>11}   (MiB)")
for label, conf in CONFIGS:
    out = subprocess.run([sys.executable, __file__, json.dumps(conf)], capture_output=True, text=True).stdout
    r = json.loads(out.strip().splitlines()[-1])
    if "error" in r:
        print(f"{label:<28}  fails: {r['error']}")
        continue
    expect = (r["heap"] / MiB - 300) * r["fraction"]
    print(f"{label:<28}{r['heap'] / MiB:>8.0f}{expect:>14.1f}{r['storage_on'] / MiB:>14.1f}"
          f"{r['storage_off'] / MiB:>10.0f}{r['max'] / MiB:>11.1f}")
