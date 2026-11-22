"""Chapter 15, part 1: time series on the taxi trips. Buckets, origins, time zones, gaps, sessions.

Run from code/:  uv run python ch15/timeseries.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.errors import PySparkException
from pyspark.sql import functions as F, Window
from common.book import session, plan, TRIPS, MONTH_START, MONTH_END

spark = session("ch15-timeseries", spark_sql_session_timeZone="UTC")
trips = spark.read.parquet(str(TRIPS)).where(
    (F.col("tpep_pickup_datetime") >= MONTH_START) & (F.col("tpep_pickup_datetime") < MONTH_END))
trips.createOrReplaceTempView("trips")


def err(e):
    first = next(l for l in str(e).splitlines() if l.strip())
    return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:72]


# 1. Three ways to put a timestamp into a bucket.
print("== 1. date_trunc, window and time_bucket")
spark.sql("""
SELECT ts,
       date_trunc('HOUR', ts)                          AS trunc_hour,
       window(ts, '15 minutes').start                  AS window_15m,
       time_bucket(INTERVAL 15 MINUTES, ts)            AS bucket_15m,
       time_bucket(INTERVAL 15 MINUTES, ts, TIMESTAMP_NTZ'2026-06-01 00:05:00') AS bucket_15m_at_05
FROM VALUES (TIMESTAMP_NTZ'2026-06-03 08:07:30'), (TIMESTAMP_NTZ'2026-06-03 08:21:00') AS v(ts)""").show()
q = spark.sql("SELECT window(tpep_pickup_datetime, '15 minutes') AS w, "
              "time_bucket(INTERVAL 15 MINUTES, tpep_pickup_datetime) AS b FROM trips")
print("types:", [(f.name, f.dataType.simpleString()) for f in q.schema.fields])

# 2. Sizes that date_trunc cannot give, and the origin.
print("\n== 2. bucket sizes and origins")
spark.sql("""
SELECT d,
       date_trunc('WEEK', d)                                       AS trunc_week,
       time_bucket(INTERVAL 7 DAYS, d)                             AS bucket_7d_epoch,
       time_bucket(INTERVAL 7 DAYS, d, TIMESTAMP_NTZ'2026-06-07')   AS bucket_7d_sunday,
       time_bucket(INTERVAL 1 MONTH, d, TIMESTAMP_NTZ'2026-01-15') AS month_from_15th
FROM VALUES (TIMESTAMP_NTZ'2026-06-10 12:00'), (TIMESTAMP_NTZ'2026-06-14 09:00') AS v(d)""").show()
print("1970-01-01 was a", spark.sql("SELECT date_format(DATE'1970-01-01', 'EEEE')").first()[0])
for label, sql in [
    ("window with a month", "SELECT window(TIMESTAMP_NTZ'2026-06-10', '1 month')"),
    ("time_bucket with 0 minutes", "SELECT time_bucket(INTERVAL 0 MINUTES, TIMESTAMP_NTZ'2026-06-10')"),
    ("time_bucket with a column size", "SELECT time_bucket(make_dt_interval(0, 0, id), "
                                       "TIMESTAMP_NTZ'2026-06-10') FROM range(1, 2)"),
    ("time_bucket on a DATE", "SELECT time_bucket(INTERVAL 1 DAY, DATE'2026-06-10')"),
]:
    try:
        print(f"  {label:<31} {spark.sql(sql).first()[0]}")
    except PySparkException as e:
        print(f"  {label:<31} {err(e)}")

# 3. TIMESTAMP against TIMESTAMP_NTZ: a daily bucket over a DST change.
print("\n== 3. daily buckets over the March DST change, session zone America/New_York")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
spark.sql("""
SELECT CAST(ts AS STRING) AS local_time,
       time_bucket(INTERVAL 1 DAY, ts)                          AS ltz_bucket,
       time_bucket(INTERVAL 1 DAY, CAST(ts AS TIMESTAMP_NTZ))   AS ntz_bucket,
       time_bucket(INTERVAL 6 HOURS, ts)                        AS ltz_6h
FROM VALUES (TIMESTAMP'2026-03-08 01:30:00'), (TIMESTAMP'2026-03-08 03:30:00'),
            (TIMESTAMP'2026-03-09 05:00:00') AS v(ts)""").show(truncate=False)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# 4. The plan: time_bucket is one expression in the projection, window() adds an Expand or a filter.
print("== 4. what each one adds to the plan")
for label, df in [
    ("time_bucket 15 min", spark.sql("SELECT time_bucket(INTERVAL 15 MINUTES, tpep_pickup_datetime) AS b, "
                                     "count(*) FROM trips GROUP BY 1")),
    ("window 15 min", trips.groupBy(F.window("tpep_pickup_datetime", "15 minutes")).count()),
    ("window 1 h, slide 15 min", trips.groupBy(F.window("tpep_pickup_datetime", "1 hour", "15 minutes")).count()),
]:
    ops = re.findall(r"\b(Expand|Filter|Project|HashAggregate)\b", plan(df, "simple"))
    print(f"  {label:<25} {ops}  rows: {df.count():,}")

# 5. Trips per 15 minutes in one zone. Empty buckets have no rows.
print("\n== 5. buckets with no trips, zone 108 (Gravesend), first day")
DAY_START, DAY_END = "2026-06-01 00:00:00", "2026-06-02 00:00:00"
counts = spark.sql(f"""
SELECT time_bucket(INTERVAL 1 HOUR, tpep_pickup_datetime) AS hour, count(*) AS trips
FROM trips WHERE PULocationID = 108
  AND tpep_pickup_datetime >= '{DAY_START}' AND tpep_pickup_datetime < '{DAY_END}'
GROUP BY 1""")
print("hours with a row:", counts.count(), "of 24")

# 6. Fill the gaps: a series of all buckets, a left join, and 0 for no rows.
grid = spark.sql(f"""SELECT explode(sequence(TIMESTAMP_NTZ'{DAY_START}', TIMESTAMP_NTZ'{DAY_END}' - INTERVAL 1 HOUR,
                                    INTERVAL 1 HOUR)) AS hour""")
filled = grid.join(counts, "hour", "left").select("hour", F.coalesce("trips", F.lit(0)).alias("trips"))
w = Window.orderBy("hour")
res = (filled.withColumn("avg_3h", F.round(F.avg("trips").over(w.rowsBetween(-2, 0)), 2))
             .withColumn("change", F.col("trips") - F.lag("trips").over(w)))
print("after the fill:", res.count(), "rows")
res.where(F.hour("hour").between(0, 8)).orderBy("hour").show()

# 7. The fill in the wrong place: a moving average over rows with gaps.
nofill = counts.withColumn("avg_3rows", F.round(F.avg("trips").over(w.rowsBetween(-2, 0)), 2))
cmp = res.join(nofill.select("hour", "avg_3rows"), "hour").where(F.col("avg_3h") != F.col("avg_3rows"))
print("hours where the average over 3 rows is not the average over 3 hours:", cmp.count())

# 8. Sessions: group pickups in a zone into bursts with gaps of more than 10 minutes.
print("\n== 6. sessions: bursts of pickups at Newark Airport (zone 1), gaps over 10 minutes")
sess = (trips.where(F.col("PULocationID") == 1)
        .groupBy(F.session_window("tpep_pickup_datetime", "10 minutes"))
        .agg(F.count("*").alias("pickups")))
s = sess.select(F.timestamp_diff("MINUTE", "session_window.start", "session_window.end").alias("minutes"),
                "pickups")
s.agg(F.count("*").alias("sessions"), F.max("pickups").alias("largest"),
      F.round(F.avg("minutes"), 1).alias("avg_minutes"),
      F.count_if(F.col("pickups") == 1).alias("single_pickup")).show()

# 9. The same sessions with windows: a new session starts after a gap.
gap = Window.orderBy("tpep_pickup_datetime")
manual = (trips.where(F.col("PULocationID") == 1).select("tpep_pickup_datetime")
          .withColumn("new", F.coalesce((F.timestamp_diff("MINUTE", F.lag("tpep_pickup_datetime").over(gap),
                                                          "tpep_pickup_datetime") >= 10).cast("int"), F.lit(1)))
          .withColumn("session_id", F.sum("new").over(gap.rowsBetween(Window.unboundedPreceding, 0))))
print("sessions with lag + running sum:", manual.select("session_id").distinct().count())

# 10. Percentiles per bucket: trip duration by hour of the day, the whole month.
print("\n== 7. trip minutes by hour of day (all June)")
spark.sql("""
SELECT hour(tpep_pickup_datetime) AS hour, count(*) AS trips,
       percentile_approx(timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) / 60, 0.5) AS p50_min,
       percentile_approx(timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) / 60, 0.9) AS p90_min
FROM trips WHERE tpep_dropoff_datetime > tpep_pickup_datetime
GROUP BY 1 HAVING hour IN (4, 8, 13, 17, 22) ORDER BY 1""").show()

# 11. Nanosecond timestamps and time_bucket.
print("== 8. time_bucket on a nanosecond timestamp (4.3 preview type)")
spark.conf.set("spark.sql.timestampNanosTypes.enabled", "true")
try:
    print(spark.sql("SELECT CAST(time_bucket(INTERVAL 1 SECOND, "
                    "CAST('2026-06-01 08:00:00.123456789' AS TIMESTAMP_NTZ(9))) AS STRING)").first()[0])
except PySparkException as e:
    print(err(e))
