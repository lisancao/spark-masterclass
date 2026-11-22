"""Chapter 74: static and runtime configs, SQL and core configs, and what each one refuses.

Run from code/:  uv run python ch74/static_runtime.py > ch74/output/static_runtime.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from common.book import session, error_text

spark = session("ch74-static", **{"spark.sql.warehouse.dir": "/tmp/ch74-warehouse"})

KEYS = [
    ("spark.sql.shuffle.partitions", "runtime SQL"),
    ("spark.sql.ansi.enabled", "runtime SQL"),
    ("spark.sql.warehouse.dir", "static SQL"),
    ("spark.sql.extensions", "static SQL"),
    ("spark.executor.memory", "core"),
    ("spark.default.parallelism", "core (special case)"),
    ("spark.myteam.owner", "not registered"),
]

print("== 1. spark.conf.isModifiable")
for k, kind in KEYS:
    print(f"  {k:<32} {kind:<20} {spark.conf.isModifiable(k)}")

VALUES = {"spark.sql.shuffle.partitions": "8", "spark.sql.ansi.enabled": "false",
          "spark.default.parallelism": "8", "spark.executor.memory": "8g"}
print("\n== 2. spark.conf.set on each key")
for k, kind in KEYS:
    try:
        spark.conf.set(k, VALUES.get(k, "x"))
        print(f"  {k:<32} accepted")
    except Exception as e:
        print(f"  {k:<32} refused")
        print(error_text(e, width=86, lines=2))
spark.conf.unset("spark.myteam.owner")
spark.conf.unset("spark.default.parallelism")

print("\n== 3. SQL SET follows the same rules")
for stmt in ["SET spark.sql.extensions=x", "SET spark.executor.memory=8g"]:
    try:
        spark.sql(stmt)
        print(f"  {stmt:<40} accepted")
    except Exception as e:
        print(f"  {stmt:<40} refused: {type(e).__name__}")

print("\n== 4. reads are always allowed")
for k in ["spark.sql.warehouse.dir", "spark.executor.memory", "spark.app.name", "spark.master"]:
    print(f"  {k:<32} {spark.conf.get(k, '(not set)')}")

print("\n== 5. builder.config on a session that already exists")
again = (SparkSession.builder
         .config("spark.sql.shuffle.partitions", "9")
         .config("spark.sql.warehouse.dir", "/tmp/somewhere-else")
         .getOrCreate())
print(f"  same session object:        {again is spark}")
print(f"  spark.sql.shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  spark.sql.warehouse.dir      = {spark.conf.get('spark.sql.warehouse.dir')}")
spark.stop()
