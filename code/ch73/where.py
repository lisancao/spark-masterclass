"""Chapter 73: what one Python worker sees. Prints the worker's interpreter, directory and sys.path.

Used by workers.py, which runs it from different folders and with different interpreters.
Run from code/:  uv run python ch73/where.py
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ui_port  # noqa: E402

b = SparkSession.builder.master("local[1]").config("spark.ui.port", ui_port())
for kv in os.environ.get("CH42_CONF", "").split():
    k, v = kv.split("=", 1)
    b = b.config(k, v)
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("OFF")


@F.udf("string")
def where_am_i(_):
    import os, sys
    from pyspark import SparkFiles
    try:
        import messytools
        found = f"messytools {messytools.VERSION} from {os.path.dirname(messytools.__file__)}"
    except ImportError as e:
        found = f"messytools: {type(e).__name__}"
    return "\n".join([f"executable  {sys.executable}",
                      f"version     {sys.version.split()[0]}",
                      f"cwd         {os.getcwd()}",
                      f"files root  {SparkFiles.getRootDirectory()}",
                      *[f"sys.path    {p}" for p in sys.path[:4]],
                      found])


print("driver pythonExec:", spark.sparkContext.pythonExec, " driver python:", sys.version.split()[0])
try:
    print(spark.range(1).select(where_am_i("id")).first()[0])
except Exception as e:
    msg = str(e)
    cls = next((c for c in ("PYTHON_VERSION_MISMATCH", "ModuleNotFoundError: No module named 'pyarrow'")
                if c in msg), type(e).__name__)
    line = next((l.strip() for l in msg.splitlines() if cls.split(":")[0] in l), "")
    print("task failed:", line[:200])
spark.stop()
