"""A batch job for Chapter 77: June 2026 revenue by pickup borough, written as Parquet.

    spark-submit jobs/zone_revenue.py <taxi dir> <output dir | -> [run id]

The same file runs from a shell, from Airflow's SparkSubmitOperator and inside a Kubernetes driver pod.
With "-" as the output, the job writes nothing and only prints the result: on Kubernetes, the executor
pods have no shared disk to write to.
"""
import sys
import time

from pyspark.sql import SparkSession, functions as F

taxi, out = sys.argv[1], sys.argv[2]
run_id = sys.argv[3] if len(sys.argv) > 3 else "manual"

spark = SparkSession.builder.appName(f"zone-revenue-{run_id}").getOrCreate()
t0 = time.time()
trips = spark.read.parquet(f"{taxi}/yellow")
zones = spark.read.option("header", True).csv(f"{taxi}/taxi_zone_lookup.csv")

by_borough = (trips
              .where((F.col("tpep_pickup_datetime") >= "2026-06-01") &
                     (F.col("tpep_pickup_datetime") < "2026-07-01") &
                     (F.col("total_amount") > 0))
              .join(zones, trips.PULocationID == zones.LocationID)
              .groupBy("Borough")
              .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 0).alias("revenue"))
              .orderBy(F.desc("revenue")))

if out == "-":
    rows = by_borough.collect()
else:
    by_borough.write.mode("overwrite").parquet(out)
    rows = spark.read.parquet(out).orderBy(F.desc("revenue")).collect()
print(f"ZONE_REVENUE run={run_id} spark={spark.version} master={spark.sparkContext.master}")
for r in rows:
    print(f"ZONE_REVENUE {r.Borough:<15} {r.trips:>9,} trips  ${r.revenue:>14,.0f}")
print(f"ZONE_REVENUE {len(rows)} rows" + ("" if out == "-" else f" written to {out}") +
      f" in {time.time() - t0:.1f} s")
spark.stop()
