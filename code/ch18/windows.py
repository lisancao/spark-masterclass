"""Chapter 18: window functions, taught from the frame up, and how Spark runs them.

Run from code/:  uv run python ch18/windows.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.errors import PySparkException
from pyspark.sql import functions as F, Window
from common.book import session, plan, TRIPS, ZONES, MONTH_START, MONTH_END

spark = session("ch18-windows", spark_sql_session_timeZone="UTC")


def err(e):
    first = next(l for l in str(e).splitlines() if l.strip())
    return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:80]


def ops(df):
    """The physical operators of df's plan, top to bottom, names only."""
    lines = plan(df, "simple").splitlines()[1:]
    out = []
    for l in lines:
        s = l.lstrip(" +-:*()0123456789")
        if s and not s.startswith("=="):
            out.append(s.split(" ")[0].split("[")[0])
    return out


# Small data first, so that every value can be checked by eye.
sales = spark.createDataFrame([
    ("Queens", "2026-06-01", 10), ("Queens", "2026-06-02", 30), ("Queens", "2026-06-03", 30),
    ("Queens", "2026-06-04", 20), ("Queens", "2026-06-06", 50),
    ("Bronx", "2026-06-01", 5), ("Bronx", "2026-06-02", 15), ("Bronx", "2026-06-05", 15),
], "borough STRING, day STRING, trips INT").withColumn("day", F.to_date("day"))
sales.createOrReplaceTempView("sales")

# 1. A window function keeps every row. groupBy keeps one row per group.
print("== 1. groupBy against a window")
print("groupBy rows:", sales.groupBy("borough").agg(F.sum("trips")).count(), "| input rows:", sales.count())
w_all = Window.partitionBy("borough")
sales.select("borough", "day", "trips",
             F.sum("trips").over(w_all).alias("borough_total"),
             F.round(F.col("trips") / F.sum("trips").over(w_all), 2).alias("share")).show()

# 2. Ranking functions on ties.
print("== 2. ranking functions")
w_rank = Window.partitionBy("borough").orderBy(F.desc("trips"))
sales.select("borough", "day", "trips",
             F.row_number().over(w_rank).alias("row_number"),
             F.rank().over(w_rank).alias("rank"),
             F.dense_rank().over(w_rank).alias("dense_rank"),
             F.round(F.percent_rank().over(w_rank), 2).alias("pct_rank"),
             F.round(F.cume_dist().over(w_rank), 2).alias("cume_dist"),
             F.ntile(2).over(w_rank).alias("ntile2")).orderBy("borough", "row_number").show()

# 3. Offsets: lag and lead look at other rows of the same partition.
print("== 3. lag and lead")
w_day = Window.partitionBy("borough").orderBy("day")
sales.select("borough", "day", "trips",
             F.lag("trips").over(w_day).alias("prev"),
             (F.col("trips") - F.lag("trips").over(w_day)).alias("change"),
             F.lead("day").over(w_day).alias("next_day"),
             F.lag("trips", 1, 0).over(w_day).alias("prev_or_0")).orderBy("borough", "day").show()

# 4. Frames. With an ORDER BY and no frame, the frame is RANGE UNBOUNDED PRECEDING to CURRENT ROW.
print("== 4. frames")
q = spark.sql("""
SELECT borough, day, trips,
  sum(trips)        OVER (PARTITION BY borough ORDER BY trips)                    AS default_frame,
  sum(trips)        OVER (PARTITION BY borough ORDER BY trips
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS rows_frame,
  last_value(trips) OVER (PARTITION BY borough ORDER BY day)                      AS last_default,
  last_value(trips) OVER (PARTITION BY borough ORDER BY day
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_whole
FROM sales WHERE borough = 'Queens' ORDER BY trips, day""")
q.show()
print([l.strip() for l in plan(q, "simple").splitlines() if "windowspecdefinition" in l][0]
      .split("windowspecdefinition")[1][:96])

# 5. ROWS counts rows. RANGE measures the ORDER BY value. Day 5 is missing for Queens.
print("\n== 5. moving windows: ROWS against RANGE")
spark.sql("""
SELECT borough, day, trips,
  sum(trips) OVER (PARTITION BY borough ORDER BY day
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)                      AS last_3_rows,
  sum(trips) OVER (PARTITION BY borough ORDER BY day
                   RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW)       AS last_3_days
FROM sales ORDER BY borough, day""").show()
w_days = Window.partitionBy("borough").orderBy(F.col("day").cast("timestamp").cast("long")) \
    .rangeBetween(-2 * 86400, 0)
print("DataFrame rangeBetween on seconds, same answer:",
      [r.s for r in sales.select(F.sum("trips").over(w_days).alias("s"))
       .orderBy("borough", "day").collect()])

# 6. Named windows in SQL.
print("\n== 6. the WINDOW clause")
spark.sql("""
SELECT borough, day, trips, sum(trips) OVER w AS running, avg(trips) OVER w AS running_avg,
       row_number() OVER (w2) AS n
FROM sales
WINDOW w AS (PARTITION BY borough ORDER BY day ROWS UNBOUNDED PRECEDING),
       w2 AS (PARTITION BY borough ORDER BY trips DESC)
ORDER BY borough, day""").show()

# 7. What the analyzer refuses.
print("== 7. errors")
for label, sql in [
    ("rank() with no ORDER BY", "SELECT rank() OVER (PARTITION BY borough) FROM sales"),
    ("window in WHERE", "SELECT * FROM sales WHERE row_number() OVER (ORDER BY day) = 1"),
    ("RANGE with two ORDER BY keys", "SELECT sum(trips) OVER (ORDER BY borough, day "
                                     "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales"),
    ("row_number() with a frame", "SELECT row_number() OVER (ORDER BY day "
                                  "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales"),
]:
    try:
        spark.sql(sql).collect()
        print(f"  {label:<30} ok")
    except PySparkException as e:
        print(f"  {label:<30} {err(e)}")

# 8. The plan: a Window operator needs its rows clustered by partition key and sorted.
print("\n== 8. the physical plan of a window, on the June trips")
trips = spark.read.parquet(str(TRIPS)).where(
    (F.col("tpep_pickup_datetime") >= MONTH_START) & (F.col("tpep_pickup_datetime") < MONTH_END))
w_zone = Window.partitionBy("PULocationID").orderBy("tpep_pickup_datetime")
gaps = trips.select("PULocationID", "tpep_pickup_datetime",
                    F.timestamp_diff("SECOND", F.lag("tpep_pickup_datetime").over(w_zone),
                                     "tpep_pickup_datetime").alias("gap_s"))
print("\n".join(l[:104] for l in plan(gaps, "simple").splitlines()[:9]))

# Two windows with the same PARTITION BY and ORDER BY share one Window operator.
two = trips.select("PULocationID", "DOLocationID", "fare_amount",
                   F.row_number().over(w_zone).alias("n"),
                   F.lag("fare_amount").over(w_zone).alias("prev_fare"))
print("same spec, two functions ->", ops(two).count("Window"), "Window operator(s)")
# A different ORDER BY needs a second sort, and a different PARTITION BY a second shuffle.
three = two.withColumn("fare_rank", F.rank().over(Window.partitionBy("PULocationID").orderBy("fare_amount"))) \
           .withColumn("do_n", F.row_number().over(Window.partitionBy("DOLocationID").orderBy("fare_amount")))
o = ops(three)
print("three specs ->", o.count("Window"), "Window,", o.count("Sort"), "Sort,", o.count("Exchange"), "Exchange")

# 9. A window with no PARTITION BY moves every row to one partition.
print("\n== 9. no PARTITION BY")
one = trips.select(F.row_number().over(Window.orderBy("tpep_pickup_datetime")).alias("n"))
print([l.strip()[:90] for l in plan(one, "simple").splitlines() if "Exchange" in l])
print("partitions of the result:", one.rdd.getNumPartitions())

# 10. A real question: the gap between pickups in each zone, and the busiest minute.
print("\n== 10. median gap between pickups, five busiest zones")
zones = spark.read.option("header", True).csv(str(ZONES))
res = (gaps.where(F.col("gap_s").isNotNull())
       .groupBy("PULocationID")
       .agg(F.count("*").alias("pickups"), F.percentile_approx("gap_s", 0.5).alias("median_gap_s"))
       .join(zones, F.col("PULocationID") == zones.LocationID)
       .orderBy(F.desc("pickups")).select("Zone", "pickups", "median_gap_s").limit(5))
res.show(truncate=False)
