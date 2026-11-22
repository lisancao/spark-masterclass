"""Chapter 75, Step 10: how big a UDF's pickled command is, and when Spark broadcasts it for you.

Run from code/:  uv run python ch75/closures.py   (the threshold is a core setting, so the
"raised threshold" row runs in its own process)
"""
import subprocess, sys
from pathlib import Path

CHILD = r'''
import sys
from pyspark import cloudpickle
from pyspark.sql import SparkSession, functions as F
threshold, words, explicit = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "bc"
b = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
if threshold != "default":
    b = b.config("spark.broadcast.UDFCompressionThreshold", threshold)
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
stop = {f"word{i:06d}" for i in range(words)}
if explicit:
    bc = spark.sparkContext.broadcast(stop)
    f = lambda w: w in bc.value
else:
    f = lambda w: w in stop
u = F.udf(f, "boolean", useArrow=False)
pickled = len(cloudpickle.dumps(stop))
judf = u._unwrapped._judf if hasattr(u, "_unwrapped") else u._judf
cmd = judf.func().command().length()
nbc = judf.func().broadcastVars().size()
ok = spark.createDataFrame([("word000001",), ("hello",)], "w string").select(u("w")).collect()
print(f"{words:>7,} words{' (sc.broadcast)' if explicit else ''}, threshold {threshold:<8}"
      f"set pickles to {pickled:>9,} B   command {cmd:>9,} B   broadcast vars {nbc}   {[r[0] for r in ok]}")
'''
for args in [("default", "300000", "-"), ("100m", "300000", "-"), ("default", "60000", "-"),
             ("default", "300000", "bc")]:
    r = subprocess.run([sys.executable, "-c", CHILD, *args], capture_output=True, text=True)
    print(r.stdout, end="")
    for line in r.stderr.splitlines():
        if "large task binary" in line:
            print("   driver log:", line.split("WARN ")[-1])
    if r.returncode:
        print(r.stderr[-1500:])
