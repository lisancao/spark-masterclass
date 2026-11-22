"""Chapter 57, Step 11: what spark.executor.pyspark.memory limits (RLIMIT_AS, virtual address space).

local[1], so one worker gets the whole limit. Each limit runs in its own process (a core setting).
Linux only.  Run from code/:  uv run python ch57/memory.py
"""
import subprocess, sys

CHILD = r'''
import sys
from pyspark.sql import SparkSession, functions as F
limit, arrow = sys.argv[1], sys.argv[2] == "arrow"
sizes = [int(x) for x in sys.argv[3].split(",")]
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.executor.pyspark.memory", limit).getOrCreate())
spark.sparkContext.setLogLevel("OFF")

def probe(mib):
    import resource
    def vm(key):
        with open("/proc/self/status") as f:
            return int(next(l.split()[1] for l in f if l.startswith(key))) // 1024
    soft = resource.getrlimit(resource.RLIMIT_AS)[0]
    start = f"VmSize {vm('VmSize')} MiB, RSS {vm('VmRSS')} MiB, RLIMIT_AS {soft // 2**20 if soft > 0 else soft} MiB"
    try:
        block = bytearray(mib * 2**20)
        return f"{start} | {mib} MiB ok"
    except MemoryError:
        return f"{start} | {mib} MiB MemoryError"

u = F.udf(probe, "string", useArrow=arrow)
for mib in sizes:
    try:
        print(f"  {limit:<4} {'Arrow' if arrow else 'pickled':<8} {spark.range(1).select(u(F.lit(mib))).first()[0]}")
    except Exception as e:
        s = str(e)
        i = s.find("Python worker")
        cause = "Missing schema" if "Missing schema" in s else ""
        print(f"  {limit:<4} {'Arrow' if arrow else 'pickled':<8} query failed: {s[i:i+52]}... {cause}")
'''
# each list runs in order on one (reused) worker
for limit, kind, sizes in [("300m", "pickled", "100,400,900"), ("300m", "arrow", "100,400,900"),
                           ("1g", "pickled", "100,400,900"), ("1g", "arrow", "100,400,900"),
                           ("2g", "arrow", "900"), ("2g", "arrow", "900,900")]:
    print(f"-- {limit} {kind}, allocations {sizes} MiB")
    r = subprocess.run([sys.executable, "-c", CHILD, limit, kind, sizes], capture_output=True, text=True)
    print(r.stdout, end="")
    if r.returncode:
        print(r.stderr[-1500:])
