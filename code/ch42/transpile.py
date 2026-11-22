"""Chapter 42, Step 3 (new in 4.3): a transpiled Python UDF needs no Python on the executors.

The workers get an interpreter path that does not exist. A plain UDF fails, and the same UDF
with spark.sql.experimental.optimizer.transpilePyUDFs=true and useArrow=False runs, because the
optimizer rewrote it as a Catalyst expression.

Run from code/:  uv run python ch42/transpile.py
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ui_port  # noqa: E402

os.environ["PYSPARK_PYTHON"] = "/nonexistent/bin/python3"     # no Python on the "executors"
from pyspark.sql import SparkSession, functions as F           # noqa: E402

spark = (SparkSession.builder.master("local[1]").config("spark.ui.port", ui_port())
         .config("spark.sql.ansi.enabled", "true").getOrCreate())
spark.sparkContext.setLogLevel("OFF")


def add_one(x):
    return x + 1


for flag in ("false", "true"):
    spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", flag)
    plain = F.udf(add_one, "long", useArrow=False)   # the transpiler runs here, when the UDF is made
    q = spark.range(3).select(plain("id").alias("y"))
    try:
        print(f"transpilePyUDFs={flag}: {[r.y for r in q.collect()]}")
        node = [l.strip() for l in q._jdf.queryExecution().executedPlan().toString().splitlines()][0]
        print("   top of the physical plan:", node)
    except Exception as e:
        line = next((l.strip() for l in str(e).splitlines() if "Cannot run program" in l), str(e)[:120])
        print(f"transpilePyUDFs={flag}: failed:", line[line.find("Cannot run"):][:90])
spark.stop()
