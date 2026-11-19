"""Chapter 3: one question about June 2026's taxi trips, asked in DataFrames and in SQL.

Which pickup zones are busiest, and how well do their riders tip?
Run from code/:  uv run python ch03/quickstart.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS, ZONES, MONTH_START, MONTH_END

spark = session("ch03-quickstart")

trips = spark.read.parquet(str(TRIPS))
zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))

# 1. Look before you aggregate: the raw month has trips from 2008 and negative fares.
trips.select(F.min("tpep_pickup_datetime").alias("first_pickup"),
             F.max("tpep_pickup_datetime").alias("last_pickup"),
             F.min("fare_amount").alias("lowest_fare"),
             F.max("trip_distance").alias("longest_trip_miles")).show(truncate=False)

# 2. The question, in the DataFrame API.
clean = trips.where(
    (F.col("tpep_pickup_datetime") >= MONTH_START) &
    (F.col("tpep_pickup_datetime") < MONTH_END) &
    (F.col("fare_amount") > 0) &
    F.col("trip_distance").between(0.1, 100))

card = F.col("payment_type") == 1          # tips are only recorded for card payments
by_zone = (clean
    .join(zones, clean.PULocationID == zones.LocationID)
    .groupBy("Borough", "Zone")
    .agg(F.count("*").alias("trips"),
         F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
         F.round(100 * F.sum(F.when(card, F.col("tip_amount"))) /
                 F.sum(F.when(card, F.col("fare_amount"))), 1).alias("tip_pct"))
    .orderBy(F.desc("trips"))
    .limit(10))
by_zone.show(truncate=False)

# 3. The same question in SQL, over the same two DataFrames registered as views.
trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")
by_zone_sql = spark.sql(f"""
    SELECT z.Borough, z.Zone,
           count(*)                                            AS trips,
           round(avg(t.fare_amount), 2)                        AS avg_fare,
           round(100 * sum(CASE WHEN t.payment_type = 1 THEN t.tip_amount END) /
                       sum(CASE WHEN t.payment_type = 1 THEN t.fare_amount END), 1) AS tip_pct
    FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
    WHERE t.tpep_pickup_datetime >= '{MONTH_START}' AND t.tpep_pickup_datetime < '{MONTH_END}'
      AND t.fare_amount > 0 AND t.trip_distance BETWEEN 0.1 AND 100
    GROUP BY z.Borough, z.Zone
    ORDER BY trips DESC
    LIMIT 10""")

print("Same answer:", by_zone.collect() == by_zone_sql.collect())

# sameSemantics compares the *analyzed* plans: the query as written, before optimization.
print("Same analyzed plan (sameSemantics):", by_zone.sameSemantics(by_zone_sql))

# After the optimizer, the two plans are the same query. (Classic only: this reaches into the JVM.)
qa, qb = by_zone._jdf.queryExecution(), by_zone_sql._jdf.queryExecution()
print("Same optimized plan (sameResult):  ", qa.optimizedPlan().sameResult(qb.optimizedPlan()))

# 4. Where the analyzed plans differ: each tree's operators, without their arguments.
import re
def skeleton(tree):
    out = []
    for line in tree.splitlines():
        m = re.match(r"^([\s:+\-']*)(\w+)", line)
        if m:
            out.append(m.group(1).replace("'", "") + m.group(2))
    return out
for name, q in (("dataframe", qa), ("sql", qb)):
    print(f"\n{name}, analyzed:")
    print("\n".join("  " + l for l in skeleton(q.analyzed().toString())))

# 5. What actually runs: the physical plan, after adaptive execution.
print()
print(plan(by_zone_sql, "formatted").split("\n\n\n")[0])
