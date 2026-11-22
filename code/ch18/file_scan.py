"""Chapter 18: the scan node. Pruning, pushed filters, nested columns, and a count() that fails.

Run from code/:  uv run python ch18/file_scan.py
"""
import re, shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

spark = session("ch18-file-scan", spark_sql_session_timeZone="UTC")
OUT = Path("/tmp/masterclass-ch18")
shutil.rmtree(OUT, ignore_errors=True)


def scan_line(df):
    """The FileScan node of the physical plan, one field per line."""
    line = next(l for l in plan(df, "simple").splitlines() if "FileScan" in l or "Scan " in l)
    head, _, rest = line.strip().lstrip("+- ").partition("] ")
    print("  " + head + "]")
    for field in re.split(r", (?=[A-Z][A-Za-z]+: )", rest):
        print("    " + field.strip())


# 1. The same small nested table as Parquet and as JSON.
trips = spark.read.parquet(str(TRIPS)).limit(20000)
nested = trips.select(
    F.struct(F.col("PULocationID").alias("zone"), F.col("tpep_pickup_datetime").alias("ts")).alias("pickup"),
    F.struct(F.col("fare_amount").alias("amount"), F.col("tip_amount").alias("tip"),
             F.col("payment_type").alias("payment")).alias("fare"),
    "trip_distance")
nested.write.parquet(str(OUT / "nested_parquet"))
nested.write.json(str(OUT / "nested_json"))

for fmt in ("parquet", "json"):
    df = spark.read.format(fmt).load(str(OUT / f"nested_{fmt}"))
    q = df.where(F.col("fare.tip") > 10).select("fare.tip")
    print(f"\n{fmt}: where fare.tip > 10, select fare.tip")
    scan_line(q)

# 2. A pushed filter is still checked row by row.
print("\nThe Filter above a pushed filter:")
q = spark.read.parquet(str(OUT / "nested_parquet")).where(F.col("trip_distance") > 30)
print("\n".join("  " + l for l in plan(q, "simple").splitlines()[1:4]))

# 3. Pruning down to the corrupt-record column.
raw = OUT / "events.json"
raw.write_text('{"id": 1, "amount": 12.5}\n{"id": 2, "amount": 7.0}\n{"id": 3, "amou\n')
events = (spark.read.schema("id long, amount double, _corrupt_record string")
          .json(str(raw)))
good = events.where(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
print("\ngood rows:", [tuple(r) for r in good.collect()])
try:
    print("good.count():", good.count())
except Exception as e:
    print("good.count() ->", type(e).__name__ + ":", str(e).splitlines()[0][:150])
print("The plan count() runs:")
print("\n".join("  " + l[:120] for l in plan(good.groupBy().count(), "simple").splitlines()[1:8]))
print("good.where(id IS NOT NULL).count():", good.where(F.col("id").isNotNull()).count())
