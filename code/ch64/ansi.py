"""Chapter 64, Step 2: the pre-4.0 cast habit under ANSI mode (the OSA Con 2026 ansi_poison demo, on 4.3).

Run from code/:  uv run python ch64/ansi.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session  # noqa: E402
from pyspark.errors import PySparkException  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = session("ansi", spark_master="local[2]")
print("spark", spark.version, "| spark.sql.ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))
rows = [(str(i * 1.5),) for i in range(1000)] + [("POISON",)]          # 1000 valid amounts, one bad row
df = spark.createDataFrame(rows, "amount string")
try:
    df.select(F.col("amount").cast("double").alias("amount")).collect()
except PySparkException as e:
    print('F.col("amount").cast("double")     ->', e.getCondition(), "SQLSTATE", e.getSqlState())
out = df.select(F.col("amount").try_cast("double").alias("amount")).collect()
print('F.col("amount").try_cast("double") ->', len(out), "rows,", sum(r.amount is None for r in out), "null")
spark.stop()
