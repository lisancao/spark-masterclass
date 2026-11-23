"""Chapter 7: how many June trips each cleaning rule removes, on its own."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, TRIPS, MONTH_START, MONTH_END

spark = session("ch07-cleaning")
t = spark.read.parquet(str(TRIPS))
rules = {
    "pickup outside June 2026": ~((F.col("tpep_pickup_datetime") >= MONTH_START) &
                                  (F.col("tpep_pickup_datetime") < MONTH_END)),
    "fare of zero or less": ~(F.col("fare_amount") > 0),
    "distance outside 0.1 to 100 miles": ~F.col("trip_distance").between(0.1, 100),
    "no passenger count": F.col("passenger_count").isNull(),
    "not paid by card (no tip recorded)": F.col("payment_type") != 1,
}
row = t.agg(F.count("*").alias("all"),
            *[F.sum(F.when(c, 1).otherwise(0)).alias(name) for name, c in rules.items()]).first()
for k, v in row.asDict().items():
    print(f"  {k:<36}{v:>10,}  {100 * v / row['all']:5.1f}%")
