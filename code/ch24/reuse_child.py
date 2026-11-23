"""Chapter 24, Step 7: one short Python process that asks for remote("local[2]"). Run by local_reuse.py."""
import os
import sys
import time

t = time.perf_counter()
from pyspark.sql import SparkSession  # noqa: E402

b = SparkSession.builder.remote("local[2]").config("spark.ui.port", os.environ.get("CONNECT_UI_PORT", "4128"))
if sys.argv[1] == "reuse":
    b = b.config("spark.local.connect.reuse", "true")
spark = b.getOrCreate()
n = spark.range(10).count()
print(f"{time.perf_counter() - t:.1f} {os.getpid()} {spark.client._session_id[:8]} {spark.client._builder.endpoint}")
spark.stop()
