"""Chapter 24: where each engine keeps its data, shown from outside the JVM heap.

  gluten-no-offheap   Gluten without spark.memory.offHeap.enabled: what the plugin says
  rapids              GPU memory (nvidia-smi) before the session, after start, after a query
Run with the Python of the engine's Spark version, for example:
  ~/spark-masterclass/data/ch24/venv-4.2.0/bin/python ch24/memory.py rapids
"""
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ch24 import engines
from ch24.engines import start, queries, OFFLOAD_FRIENDLY

what = sys.argv[1]


def gpu_mib():
    out = subprocess.run(["nvidia-smi", "--query-gpu=memory.used,memory.total",
                          "--format=csv,noheader,nounits"], capture_output=True, text=True)
    used, total = (int(x) for x in out.stdout.split(","))
    return used, total


if what == "gluten-no-offheap":
    del engines.ENGINES["gluten"]["spark.memory.offHeap.enabled"]
    del engines.ENGINES["gluten"]["spark.memory.offHeap.size"]
    try:
        spark = start("gluten", **OFFLOAD_FRIENDLY)
        queries(spark)["hourly"].collect()
        print("Gluten without off-heap memory: the query ran")
    except Exception as e:
        java = [l for l in str(e).splitlines() if "Exception:" in l or "Error:" in l]
        print("Gluten without off-heap memory:")
        print("  " + (java or [str(e)])[0].lstrip(": ")[:300])

if what == "rapids":
    used0, total = gpu_mib()
    print(f"GPU memory, total {total} MiB")
    print(f"  before the session         {used0:>6} MiB used")
    spark = start("rapids", **OFFLOAD_FRIENDLY)
    spark.range(1).selectExpr("id + 1").collect()     # the first GPU task makes the pool
    time.sleep(1)
    print(f"  after the first GPU task   {gpu_mib()[0]:>6} MiB used")
    queries(spark)["routes"].collect()
    print(f"  after the routes query     {gpu_mib()[0]:>6} MiB used")
    spark.stop()
    time.sleep(2)
    print(f"  after spark.stop()         {gpu_mib()[0]:>6} MiB used")
