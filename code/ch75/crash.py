"""Chapter 75, Step 6: a Python worker that dies (segfault) and a worker that hangs.

faulthandler prints the Python stack of the moment of the crash into the error.
tracebackDumpIntervalSeconds prints the stack of a slow worker every N seconds.
Run from code/:  uv run python ch75/crash.py
"""
import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402
from common.book import session  # noqa: E402

spark = session("ch75-crash", spark_ui_showConsoleProgress="false")


@F.udf("long")
def parse_blob(n):
    import ctypes
    if n == 3:
        ctypes.string_at(0)           # read address 0: SIGSEGV in C code, no Python exception
    return n


df = spark.range(5).select(parse_blob("id").alias("x"))
for flag in ("false", "true"):
    spark.conf.set("spark.sql.execution.pyspark.udf.faulthandler.enabled", flag)
    try:
        df.collect()
    except Exception as e:
        lines = [line for line in str(e).splitlines() if line.strip()]
        cause = next(line for line in lines if line.startswith("Caused by") and "Python worker" in line)
        frames = [line.strip() for line in lines[lines.index(cause):] if line.strip().startswith('File "')]
        print(f"\n== faulthandler.enabled={flag}: {type(e).__name__}, {len(lines)} lines")
        import textwrap
        print("\n".join("   " + w for w in textwrap.wrap(cause.split(": ", 1)[1], 92)[:3]))
        if frames:
            print(f"   ... {len(frames)} Python frames after it, the first two:")
            for f in frames[:2]:
                print("     " + f.replace(str(Path(__file__).parent) + "/", "")[:92])

print("\n== a slow UDF, tracebackDumpIntervalSeconds=2 (the dump goes to the worker's stderr)")


@F.udf("long")
def slow(n):
    time.sleep(5)
    return n


spark.conf.set("spark.sql.execution.pyspark.udf.tracebackDumpIntervalSeconds", "2")
r = spark.range(1).repartition(1).select(slow("id")).collect()
print(f"   result {r}")
spark.stop()
