"""Chapter 14: SQL UDFs (scalar and table), how Catalyst inlines them, the SQL path, and recursive CTEs.

Run from code/:  uv run python ch14/functions.py
"""
import contextlib
import fcntl
import os
import re
import sys
import tempfile
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import functions as F
from common.book import session, plan, TRIPS, ZONES, MONTH_START, MONTH_END, DATA

spark = session("ch14-functions", spark_sql_session_timeZone="UTC",
                spark_sql_warehouse_dir=tempfile.mkdtemp(prefix="ch14-"))
spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips")
spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES)).createOrReplaceTempView("zones")


def err(e):
    if isinstance(e, PySparkException):
        first = next(l for l in str(e).splitlines() if l.strip())
        return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:72]
    m = re.search(r"\[([A-Z_.]+)\] ([^\n]*)", str(e))
    return f"{type(e).__name__} {m.group(1)}: {m.group(2)[:60]}"


def run(label, sql, **kw):
    try:
        rows = spark.sql(sql, **kw).collect()
        print(f"{label}: {rows if len(rows) != 1 else rows[0]}")
    except (PySparkException, Py4JJavaError) as e:
        print(f"{label}: {err(e)}")


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


# 1. A scalar SQL UDF. Its body is an expression, and Catalyst puts that expression into the plan.
print("== 1. a scalar SQL UDF")
spark.sql("""
CREATE OR REPLACE TEMPORARY FUNCTION fare_band(fare DOUBLE)
RETURNS STRING
COMMENT 'Fare band of a trip'
RETURN CASE WHEN fare IS NULL OR fare <= 0 THEN 'invalid'
            WHEN fare < 15 THEN 'short'
            WHEN fare < 40 THEN 'medium'
            ELSE 'long' END""")
q = spark.sql("SELECT fare_band(fare_amount) AS band, count(*) AS n FROM trips GROUP BY 1 ORDER BY n DESC")
q.show()
ana = plan(q, "extended")
print("analyzed plan mentions the function:",
      [l.strip()[:90] for l in ana.split("== Optimized Logical Plan ==")[0].splitlines() if "fare_band" in l][:1])
opt = ana.split("== Optimized Logical Plan ==")[1].split("== Physical Plan ==")[0]
print("optimized plan lines with the name:", [l.strip()[:70] for l in opt.splitlines() if "fare_band" in l])
print("optimized plan:", [l.strip()[:96] for l in opt.splitlines() if "CASE WHEN" in l][:1])

# 2. The same logic as a Python UDF: an opaque call, a Python worker, and no filter pushdown.
print("\n== 2. the same logic as a Python UDF")


@F.udf("string")
def fare_band_py(fare):
    if fare is None or fare <= 0:
        return "invalid"
    return "short" if fare < 15 else "medium" if fare < 40 else "long"


spark.udf.register("fare_band_py", fare_band_py)
for name in ("fare_band", "fare_band_py"):
    df = spark.sql(f"SELECT count(*) FROM trips WHERE {name}(fare_amount) = 'long'")
    p = plan(df, "simple")
    ops = sorted({w for w in re.findall(r"\b(ArrowEvalPython|BatchEvalPython|PushedFilters: \[[^\]]*\])", p)})
    t = []
    with bench_lock():
        for _ in range(4):
            t0 = time.perf_counter()
            n = df.first()[0]
            t.append(time.perf_counter() - t0)
    print(f"  {name:<13} {n:>7,} rows  median {sorted(t[1:])[1]:.2f} s  {ops}")

# 3. A SQL table function: a parameterized view.
print("\n== 3. a SQL table function")
spark.sql(f"""
CREATE OR REPLACE TEMPORARY FUNCTION zone_trips(zone_name STRING, min_fare DOUBLE DEFAULT 0)
RETURNS TABLE (pickup TIMESTAMP_NTZ, dropoff_zone STRING, fare DOUBLE)
RETURN SELECT t.tpep_pickup_datetime, d.Zone, t.fare_amount
       FROM trips t
       JOIN zones p ON t.PULocationID = p.LocationID
       JOIN zones d ON t.DOLocationID = d.LocationID
       WHERE p.Zone = zone_name AND t.fare_amount > min_fare
         AND t.tpep_pickup_datetime >= '{MONTH_START}' AND t.tpep_pickup_datetime < '{MONTH_END}'""")
spark.sql("""SELECT dropoff_zone, count(*) AS n, round(avg(fare), 2) AS avg_fare
             FROM zone_trips('JFK Airport', min_fare => 20) GROUP BY dropoff_zone ORDER BY n DESC LIMIT 4""").show()
print("plan of the table function:",
      [l.strip()[:80] for l in plan(spark.sql("SELECT * FROM zone_trips('JFK Airport')"), "simple").splitlines()
       if "FileScan" in l or "SQLFunctionNode" in l or "BroadcastHashJoin" in l][:3])

# 4. Limits of SQL UDFs.
print("\n== 4. what a SQL UDF refuses")
run("a SQL UDF named like a built-in", "CREATE OR REPLACE TEMPORARY FUNCTION upper(s STRING) RETURNS STRING RETURN s")
run("a non-constant LIMIT", "CREATE OR REPLACE TEMPORARY FUNCTION top_fares(n INT) RETURNS TABLE (f DOUBLE) "
    "RETURN SELECT fare_amount FROM trips ORDER BY fare_amount DESC LIMIT n")
run("a parameter used as a table name", "CREATE OR REPLACE TEMPORARY FUNCTION cnt(t STRING) RETURNS BIGINT "
    "RETURN (SELECT count(*) FROM IDENTIFIER(t))")
spark.sql("CREATE OR REPLACE TEMPORARY FUNCTION doubled(x INT) RETURNS INT RETURN x * 2")
run("a wrong argument type", "SELECT doubled('ten')")
run("a scalar UDF with a subquery", "CREATE OR REPLACE TEMPORARY FUNCTION zone_name(id INT) RETURNS STRING "
    "RETURN (SELECT Zone FROM zones WHERE LocationID = id)")
run("  ... and its use", "SELECT zone_name(132) AS z, zone_name(999) AS missing")

# 5. Persistent functions keep their definition in the catalog.
print("\n== 5. a persistent SQL function")
spark.sql("CREATE OR REPLACE FUNCTION default.tip_pct(tip DOUBLE, fare DOUBLE) RETURNS DOUBLE "
          "DETERMINISTIC RETURN round(100 * tip / nullif(fare, 0), 1)")
desc = spark.sql("DESCRIBE FUNCTION EXTENDED default.tip_pct").collect()
for r in desc:
    if r[0].split(":")[0].strip() in ("Function", "Type", "Input", "Returns", "Deterministic", "Body"):
        print("  ", r[0][:90])
run("tip_pct(5, 20)", "SELECT default.tip_pct(5, 20) AS pct, default.tip_pct(5, 0) AS div0")

# 6. The SQL path: where Spark looks for an unqualified function name.
print("\n== 6. the SQL path")
run("current_path() by default", "SELECT current_path()")
run("SET PATH, default config", "SET PATH = spark_catalog.default, system.builtin")
spark.conf.set("spark.sql.path.enabled", "true")
run("current_path() with spark.sql.path.enabled", "SELECT current_path()")
run("unqualified tip_pct, default path", "SELECT tip_pct(5, 20)")
spark.sql("CREATE SCHEMA IF NOT EXISTS finance")
spark.sql("CREATE OR REPLACE FUNCTION finance.tip_pct(tip DOUBLE, fare DOUBLE) RETURNS DOUBLE "
          "RETURN round(tip / nullif(fare, 0), 3)")
spark.sql("SET PATH = spark_catalog.finance, system.builtin, system.session")
run("after SET PATH = spark_catalog.finance, ...", "SELECT current_path() AS path, tip_pct(5, 20) AS tip")
spark.sql("SET PATH = DEFAULT_PATH")
run("after SET PATH = DEFAULT_PATH", "SELECT current_path() AS path, tip_pct(5, 20) AS tip")
spark.conf.unset("spark.sql.path.enabled")

# 7. A recursive CTE: follow the most frequent destination from a zone, step by step.
print("\n== 7. a recursive CTE")
spark.sql(f"""CREATE OR REPLACE TEMP VIEW next_zone AS
SELECT PULocationID AS src, max_by(DOLocationID, n) AS dst FROM (
  SELECT PULocationID, DOLocationID, count(*) AS n FROM trips
  WHERE tpep_pickup_datetime >= '{MONTH_START}' AND tpep_pickup_datetime < '{MONTH_END}'
    AND PULocationID <> DOLocationID
  GROUP BY 1, 2) GROUP BY 1""")
CHAIN = """
WITH RECURSIVE chain(step, zone_id, path) AS (
  SELECT 0, 154, array(154)
  UNION ALL
  SELECT c.step + 1, n.dst, array_append(c.path, n.dst)
  FROM chain c JOIN next_zone n ON n.src = c.zone_id
  WHERE NOT array_contains(c.path, n.dst)
)
SELECT c.step, z.Zone FROM chain c JOIN zones z ON z.LocationID = c.zone_id ORDER BY step"""
spark.sql(CHAIN).show(truncate=False)

# 8. Without the cycle check, the recursion stops at the level limit.
NOCHECK = """
WITH RECURSIVE chain(step, zone_id) AS (
  SELECT 0, 154
  UNION ALL
  SELECT c.step + 1, n.dst FROM chain c JOIN next_zone n ON n.src = c.zone_id
)
SELECT max(step) FROM chain"""
run("no cycle check", NOCHECK)
run("no cycle check, MAX RECURSION LEVEL 20", NOCHECK.replace("WITH RECURSIVE chain(step, zone_id)",
                                                              "WITH RECURSIVE chain(step, zone_id) MAX RECURSION LEVEL 20"))
run("a LIMIT stops the recursion", NOCHECK.replace("SELECT max(step) FROM chain", "SELECT * FROM chain LIMIT 12")
    .replace("SELECT *", "SELECT count(*) FROM (SELECT *") + ")")
print("plan nodes:", sorted(set(re.findall(r"\b(UnionLoop\w*|UnionLoopRef)\b", plan(spark.sql(CHAIN), "extended")))))

# 9. A calendar: the classic use, with no table to read.
print("\n== 8. a calendar from a recursive CTE")
run("days in June with no trip from zone 108", f"""
WITH RECURSIVE days(d) AS (
  SELECT DATE'{MONTH_START}' UNION ALL SELECT d + 1 FROM days WHERE d + 1 < DATE'{MONTH_END}')
SELECT count(*) AS days, count_if(n IS NULL) AS days_with_no_trip FROM days
LEFT JOIN (SELECT to_date(tpep_pickup_datetime) AS d, count(*) AS n FROM trips
           WHERE PULocationID = 108 GROUP BY 1) USING (d)""")
