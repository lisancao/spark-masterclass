"""Chapter 29: why a folder of Parquet files is not a table. Runs on Spark 4.3.0 with no extra jars.

Run from code/:  uv run python ch29/parquet_folder.py
"""
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session
from common.lake import WORK

folder = WORK / "ch29-folder" / "events"
shutil.rmtree(folder.parent, ignore_errors=True)
spark = session("ch29-folder")

spark.createDataFrame([("e1", 12.5), ("e2", 3.0)], "event STRING, amount DOUBLE") \
    .coalesce(1).write.parquet(str(folder))
print("day 1 written:", spark.read.parquet(str(folder)).count(), "rows")

# Day 2: the producer now sends amount as a string. Nothing checks it on write.
spark.createDataFrame([("e3", "12.00")], "event STRING, amount STRING") \
    .coalesce(1).write.mode("append").parquet(str(folder))
print("day 2 appended with amount as a string: no error")
print("files:", sorted(p.name[:20] + "..." for p in folder.glob("*.parquet")))

for label, reader in [("read", lambda: spark.read.parquet(str(folder)).collect()),
                      ("read with mergeSchema", lambda: spark.read.option("mergeSchema", "true")
                       .parquet(str(folder)).collect())]:
    try:
        reader()
        print(f"{label}: OK")
    except Exception as e:
        # Classic PySpark raises a Py4JJavaError here; the condition is inside its message.
        conds = re.findall(r"\[([A-Z][A-Z_.]+)\]", str(e))
        print(f"{label}: {type(e).__name__}, conditions in the message: {conds}")

# An overwrite is a delete followed by a write. A reader in between sees an empty folder.
spark.range(3).coalesce(1).write.mode("overwrite").parquet(str(folder))
print("after overwrite:", sorted(p.name[:20] + "..." for p in folder.glob("*.parquet")),
      "\nno version history, so the two earlier days are gone for good")
