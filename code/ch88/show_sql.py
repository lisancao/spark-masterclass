"""One aggregate, two read paths: the SQL that PostgreSQL receives, and how many rows come back.

The jdbc format (DSv1) pushes only filters and columns. The JDBC catalog (DSv2) pushes the GROUP BY.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import PROPS, URL, Log, pg_session  # noqa: E402

spark = pg_session("ch88-show-sql")
from pyspark.sql import functions as F  # noqa: E402

for name, df in [("spark.read.jdbc(...)       (DSv1)", spark.read.jdbc(URL, "trips", properties=PROPS)),
                 ("spark.table('pg.public.trips') (DSv2)", spark.table("pg.public.trips"))]:
    q = df.groupBy("payment_type").agg(F.avg("fare_amount").alias("avg_fare"))
    with Log() as log:
        rows = q.collect()
    print(f"== {name}")
    for s in log.selects("trips"):
        print("   postgres> " + " ".join(s.split())[:84])
    print(f"   rows sent to Spark: about {log.fetches * 1000:,} ({log.fetches} fetches of 1000)"
          if log.fetches else f"   rows sent to Spark: {len(rows)}")
    print()
