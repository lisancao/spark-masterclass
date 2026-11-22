"""Chapter 9: the time-aware joins. ASOF JOIN (4.3), NEAREST BY (4.2) and LATERAL, with their plans.

Timings: run under the bench lock.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch09/asof.py
"""
import contextlib
import fcntl
import os
import re
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.errors import PySparkException
from pyspark.sql import functions as F, Window
from common.book import session, plan, TRIPS, ZONES, MONTH_START, MONTH_END, DATA

spark = session("ch09-asof", spark_sql_session_timeZone="UTC")
spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES)).createOrReplaceTempView("zones")
trips = spark.read.parquet(str(TRIPS)).where(
    (F.col("tpep_pickup_datetime") >= MONTH_START) & (F.col("tpep_pickup_datetime") < MONTH_END)
    & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")))
trips.createOrReplaceTempView("trips")


def streams(days, zone=None):
    """Two event streams from the same trips, from 1 June for `days` days: pickups and drop-offs per zone."""
    end = f"TIMESTAMP_NTZ'2026-06-01' + INTERVAL {days} DAYS"
    z = f" AND {{c}} = {zone}" if zone else ""
    spark.sql(f"CREATE OR REPLACE TEMP VIEW pickups AS SELECT PULocationID AS zone, tpep_pickup_datetime AS t "
              f"FROM trips WHERE tpep_pickup_datetime < {end}" + z.format(c="PULocationID"))
    spark.sql(f"CREATE OR REPLACE TEMP VIEW dropoffs AS SELECT DOLocationID AS zone, tpep_dropoff_datetime AS t "
              f"FROM trips WHERE tpep_dropoff_datetime < {end}" + z.format(c="DOLocationID"))


streams(1)   # one day, all zones


def err(e):
    first = next(l for l in str(e).splitlines() if l.strip())
    return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:72]


def ops(df):
    return [o for o in re.findall(r"\b([A-Z][A-Za-z]+(?:Join|Exec|Product|Aggregate|Generate|Window))\b",
                                  plan(df, "simple"))]


@contextlib.contextmanager
def bench_lock():
    """Hold the shared bench lock only while a timing runs (the book quotes these timings).
    BOOK_NO_LOCK=1 skips the lock, for films and test runs whose timings the book does not quote."""
    if os.environ.get("BOOK_NO_LOCK"):
        yield
        return
    with open(DATA / ".bench.lock", "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def median_s(build, runs=3):
    """Median wall time of build().collect() over `runs` runs after one warmup, a new DataFrame each run."""
    with bench_lock():
        build().collect()
        ts = []
        for _ in range(runs):
            t0 = time.perf_counter()
            build().collect()
            ts.append(time.perf_counter() - t0)
    return statistics.median(ts)


# 1. ASOF JOIN is off by default in 4.3.0.
print("== 1. ASOF JOIN needs a flag")
ASOF = """
SELECT p.zone, p.t AS pickup, d.t AS last_dropoff
FROM pickups p
ASOF JOIN dropoffs d
  MATCH_CONDITION (p.t >= d.t)
  ON p.zone = d.zone"""
try:
    spark.sql(ASOF)
except PySparkException as e:
    print("default config:", err(e))
spark.conf.set("spark.sql.join.asofJoin.enabled", "true")

# 2. For each pickup, the last drop-off in the same zone before it: how long had the taxi waited?
print("\n== 2. the last drop-off before each pickup, in the same zone (1 June)")
spark.sql(ASOF + " WHERE p.zone = 132 ORDER BY pickup LIMIT 5").show()
waits = spark.sql(f"""
SELECT z.Zone, count(*) AS pickups,
       percentile(timestampdiff(SECOND, last_dropoff, pickup), 0.5) AS median_wait_s
FROM ({ASOF}) a JOIN zones z ON a.zone = z.LocationID
GROUP BY z.Zone ORDER BY pickups DESC LIMIT 5""")
waits.show(truncate=False)

# 3. The plan: one sort-merge pass per zone, not a search per row.
print("== 3. the plan of ASOF JOIN")
p = plan(spark.sql(ASOF), "simple")
print("\n".join(l[:104] for l in p.splitlines()[1:9]))

# 4. Direction, LEFT and ties.
print("\n== 4. direction, LEFT ASOF and NULL")
spark.sql("""CREATE OR REPLACE TEMP VIEW readings(t, zone, level) AS VALUES
  (TIMESTAMP_NTZ'2026-06-01 08:00', 1, 'low'), (TIMESTAMP_NTZ'2026-06-01 09:00', 1, 'high'),
  (TIMESTAMP_NTZ'2026-06-01 10:00', 1, 'low')""")
spark.sql("""CREATE OR REPLACE TEMP VIEW events(t, zone) AS VALUES
  (TIMESTAMP_NTZ'2026-06-01 07:30', 1), (TIMESTAMP_NTZ'2026-06-01 09:00', 1),
  (TIMESTAMP_NTZ'2026-06-01 09:40', 1), (CAST(NULL AS TIMESTAMP_NTZ), 1)""")
for op, label in ((">=", "last at or before"), (">", "last strictly before"), ("<=", "first at or after")):
    rows = spark.sql(f"""SELECT date_format(e.t, 'HH:mm') AS e, r.level, date_format(r.t, 'HH:mm') AS r
        FROM events e LEFT ASOF JOIN readings r MATCH_CONDITION (e.t {op} r.t) USING (zone)
        ORDER BY e.t NULLS LAST""").collect()
    print(f"  e.t {op:<2} r.t ({label:<20}):", [(x.e, x.r) for x in rows])
print("  INNER drops unmatched rows:", spark.sql("""SELECT count(*) FROM events e ASOF JOIN readings r
      MATCH_CONDITION (e.t >= r.t) USING (zone)""").first()[0], "of 4")
for label, sql in [
    ("= in MATCH_CONDITION", "SELECT * FROM events e ASOF JOIN readings r MATCH_CONDITION (e.t = r.t)"),
    ("RIGHT ASOF JOIN", "SELECT * FROM events e RIGHT ASOF JOIN readings r MATCH_CONDITION (e.t >= r.t)"),
]:
    try:
        spark.sql(sql).collect()
        print(f"  {label:<22} ok")
    except PySparkException as e:
        print(f"  {label:<22} {err(e)}")

# 5. The ways to get the same answer before ASOF JOIN, and what each costs on the full month.
print("\n== 5. the same answer three ways, 1 June (median of 3 after a warmup)")


def asof_sql():
    return spark.sql(f"SELECT count(*) AS n, sum(timestampdiff(SECOND, last_dropoff, pickup)) AS s FROM ({ASOF})")


def window_way():
    # Union both streams, sort by zone and time, and carry the last drop-off time forward.
    ev = spark.sql("""SELECT zone, t, 1 AS kind, t AS dt FROM dropoffs
                      UNION ALL SELECT zone, t, 2 AS kind, NULL AS dt FROM pickups""")
    w = Window.partitionBy("zone").orderBy("t", "kind").rowsBetween(Window.unboundedPreceding, -1)
    return (ev.withColumn("last_dropoff", F.last("dt", ignorenulls=True).over(w))
              .where((F.col("kind") == 2) & F.col("last_dropoff").isNotNull())
              .agg(F.count("*").alias("n"),
                   F.sum(F.timestamp_diff("SECOND", "last_dropoff", "t")).alias("s")))


def range_join_way():
    # A plain join on zone and a time range, then keep the latest match: the rows explode first.
    return spark.sql("""
        SELECT count(*) AS n, sum(timestampdiff(SECOND, last_dropoff, pickup)) AS s FROM (
          SELECT p.zone, p.t AS pickup, max(d.t) AS last_dropoff
          FROM pickups p JOIN dropoffs d
            ON p.zone = d.zone AND d.t <= p.t AND d.t > p.t - INTERVAL 10 MINUTES
          GROUP BY p.zone, p.t)""")


for label, build in (("ASOF JOIN", asof_sql), ("window over a union", window_way),
                     ("range join + max (10 min)", range_join_way)):
    r = build().first()
    print(f"  {label:<26} n={r.n:>9,}  s={r.s:>14,}  {median_s(build):6.2f} s  {sorted(set(ops(build())))[:4]}")
print("  (pickups may share a timestamp, so the range join groups fewer rows;"
      " and it misses waits over 10 minutes)")

# 5b. How the time grows with the size of one zone's group.
print("\n== 5b. one busy zone (237), more days: ASOF JOIN against the window (median of 3)")
for days in (1, 2, 4):
    streams(days, zone=237)
    n = spark.sql("SELECT count(*) FROM pickups").first()[0]
    print(f"  {days} day(s), {n:>6,} pickups:  ASOF {median_s(asof_sql, 3):6.2f} s   window {median_s(window_way, 3):5.2f} s")
streams(1)

# 6. NEAREST BY: the closest rows in either direction, top-K, by any expression.
print("\n== 6. NEAREST BY")
spark.sql("""CREATE OR REPLACE TEMP VIEW fare_rules(rule, fare) AS VALUES
  ('minimum', 3.0), ('short', 10.0), ('crosstown', 25.0), ('airport', 70.0), ('long', 120.0)""")
spark.sql("""
SELECT t.fare_amount, r.rule, r.fare AS rule_fare
FROM (SELECT DISTINCT fare_amount FROM trips WHERE fare_amount IN (4.4, 19.8, 52.0, 69.9)) t
JOIN fare_rules r EXACT NEAREST 2 BY DISTANCE abs(t.fare_amount - r.fare)
ORDER BY t.fare_amount, abs(t.fare_amount - r.fare)""").show()
nb = spark.sql("""SELECT * FROM pickups p JOIN dropoffs d
                  APPROX NEAREST 1 BY DISTANCE abs(timestampdiff(SECOND, p.t, d.t))""")
print("plan of APPROX NEAREST BY:", sorted(set(ops(nb))))
ex = spark.sql("""SELECT * FROM pickups p JOIN dropoffs d
                  EXACT NEAREST 1 BY DISTANCE abs(timestampdiff(SECOND, p.t, d.t))""")
print("plan of EXACT NEAREST BY: ", sorted(set(ops(ex))))
q = spark.createDataFrame([(30.0,), (95.0,)], "fare_amount DOUBLE").alias("t")
print("DataFrame API:", [tuple(r) for r in q.nearestByJoin(
    spark.table("fare_rules").alias("r"), F.abs(F.col("t.fare_amount") - F.col("r.fare")), 1, "exact", "distance")
    .orderBy("fare_amount").collect()])
for label, sql in [
    ("k = 0", "SELECT * FROM trips t JOIN fare_rules r EXACT NEAREST 0 BY DISTANCE abs(t.fare_amount - r.fare)"),
    ("FULL join", "SELECT * FROM trips t FULL JOIN fare_rules r EXACT NEAREST 1 BY DISTANCE abs(t.fare_amount - r.fare)"),
]:
    try:
        spark.sql(sql).collect()
        print(f"  {label:<10} ok")
    except PySparkException as e:
        print(f"  {label:<10} {err(e)}")

# 7. LATERAL: a subquery that can read the columns of the row on its left.
print("\n== 7. LATERAL: the three longest trips from each of three zones")
spark.sql("""
SELECT z.Zone, l.trip_distance, l.fare_amount
FROM zones z,
LATERAL (SELECT trip_distance, fare_amount FROM trips t
         WHERE t.PULocationID = z.LocationID AND fare_amount > 0 AND trip_distance < 100
         ORDER BY trip_distance DESC LIMIT 3) l
WHERE z.LocationID IN (132, 138, 161)
ORDER BY z.Zone, l.trip_distance DESC""").show(truncate=False)
