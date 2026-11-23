"""The read side: what S3A asks MinIO for when Spark lists, plans and reads a table of files.

The taxi month written in day folders (pickup_day=2026-06-01/ ...). Then three traced reads:
open the table (listing and schema), count one day (partition pruning), and sum one column.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from s3 import Trace, s3_session  # noqa: E402
from common.book import TRIPS, MONTH_START, MONTH_END  # noqa: E402

spark = s3_session("ch38-listing")
from pyspark.sql import functions as F  # noqa: E402

path = "s3a://book/trips_by_day/"
(spark.read.parquet(str(TRIPS))
 .where(f"tpep_pickup_datetime >= '{MONTH_START}' AND tpep_pickup_datetime < '{MONTH_END}'")
 .withColumn("pickup_day", F.to_date("tpep_pickup_datetime"))
 .repartition("pickup_day").write.mode("overwrite").partitionBy("pickup_day").parquet(path))


def phase(title, fn):
    with Trace() as tr:
        result = fn()
    c = tr.counts()
    sent = sum(r.get("callStats", {}).get("tx", 0) for r in tr.calls if r["api"] == "s3.GetObject")
    print(f"== {title}")
    print(f"   result: {result}")
    print("   requests: " + ", ".join(f"{k} {v}" for k, v in sorted(c.items(), key=lambda kv: -kv[1])))
    print(f"   bytes sent by GetObject: {sent:,}")
    print()


holder = {}
phase("spark.read.parquet(path): list the folders, read one footer for the schema",
      lambda: (holder.__setitem__("df", spark.read.parquet(path)), f"{len(holder['df'].columns)} columns")[1])
df = holder["df"]
phase("count the trips of one day (partition pruning)",
      lambda: "{:,} rows".format(df.where("pickup_day = '2026-06-15'").count()))
phase("sum one column over the whole month (column pruning)",
      lambda: f"{df.agg(F.sum('fare_amount')).first()[0]:,.0f} dollars")
phase("count every row (footers only)", lambda: f"{df.count():,} rows")

files = spark._jvm.org.apache.hadoop.fs.Path(path).getFileSystem(spark._jsc.hadoopConfiguration())
summary = files.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(path))
print(f"the table: {summary.getDirectoryCount() - 1} folders, {summary.getFileCount()} files, "
      f"{summary.getLength():,} bytes")
