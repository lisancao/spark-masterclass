"""Chapter 60, Step 9: a segmentation fault without an idle timeout, with faulthandler off and on.

Run from code/:  uv run python ch60/segfault.py nofh; uv run python ch60/segfault.py fh
"""
import ctypes, sys, time, os
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
fh = sys.argv[1] == "fh"
spark.conf.set("spark.sql.execution.pyspark.udf.faulthandler.enabled", str(fh).lower())
def segfault(_):
    ctypes.string_at(0)
t = time.perf_counter()
try:
    spark.range(1).select(F.udf(segfault, "long", useArrow=False)("id")).collect()
except Exception as e:
    s = str(e)
    i = s.find("Python worker")
    print(fh, round(time.perf_counter()-t,1), s[i:i+200].replace("\n"," | "))
