"""Chapter 18: top-N per group with QUALIFY, pipe syntax and aggregates, and what each one plans.

Run from code/:  uv run python ch18/qualify.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.errors import PySparkException
from common.book import session, plan, TRIPS, ZONES, MONTH_START, MONTH_END

spark = session("ch18-qualify", spark_sql_session_timeZone="UTC")
spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips")
spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES)).createOrReplaceTempView("zones")
spark.sql(f"""CREATE OR REPLACE TEMP VIEW june AS
SELECT t.*, z.Borough, z.Zone FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
WHERE tpep_pickup_datetime >= '{MONTH_START}' AND tpep_pickup_datetime < '{MONTH_END}'
  AND fare_amount > 0 AND trip_distance BETWEEN 0.1 AND 100""")


def err(e):
    first = next(l for l in str(e).splitlines() if l.strip())
    return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:72]


def nodes(df):
    """Operator lines of the physical plan, without arguments."""
    out = []
    for l in plan(df, "simple").splitlines()[1:]:
        s = l.lstrip(" +-:*()0123456789")
        if s:
            out.append(s.split(" ")[0])
    return out


# 1. The longest trip from each borough, the old way: a subquery and a filter on row_number.
OLD = """
SELECT Borough, Zone, trip_distance FROM (
  SELECT Borough, Zone, trip_distance,
         row_number() OVER (PARTITION BY Borough ORDER BY trip_distance DESC) AS rn
  FROM june) WHERE rn = 1 ORDER BY Borough"""
# 2. The same with QUALIFY: a WHERE for window functions.
NEW = """
SELECT Borough, Zone, trip_distance FROM june
QUALIFY row_number() OVER (PARTITION BY Borough ORDER BY trip_distance DESC) = 1
ORDER BY Borough"""
# 3. The same as a pipe: EXTEND the rank, then WHERE on it.
PIPE = """
FROM june
|> EXTEND row_number() OVER (PARTITION BY Borough ORDER BY trip_distance DESC) AS rn
|> WHERE rn = 1
|> SELECT Borough, Zone, trip_distance
|> ORDER BY Borough"""
print("== 1. longest trip per borough, three ways")
old, new, pipe = (spark.sql(s) for s in (OLD, NEW, PIPE))
new.show(truncate=False)
print("same rows:", old.collect() == new.collect() == pipe.collect())
o = [q._jdf.queryExecution().optimizedPlan() for q in (old, new, pipe)]
print("same optimized plan, subquery vs QUALIFY:", o[0].sameResult(o[1]))
print("same optimized plan, QUALIFY vs pipe:    ", o[1].sameResult(o[2]))


def names(p):
    return [p.nodeName()] + [n for i in range(p.children().size()) for n in names(p.children().apply(i))]


print("same operators, QUALIFY vs pipe:         ", names(o[1]) == names(o[2]))
print("the one difference:", [l.strip()[:60] for l in o[2].toString().splitlines() if "Project [trip_distance" in l])

# 4. The plan: Spark keeps at most one row per borough before the shuffle.
print("\n== 2. the physical plan of the QUALIFY query")
print("\n".join(l[:100] for l in plan(new, "simple").splitlines()[1:14]))
spark.conf.set("spark.sql.optimizer.windowGroupLimitThreshold", -1)
print("with windowGroupLimitThreshold = -1:", [n for n in nodes(spark.sql(NEW)) if "Window" in n])
spark.conf.unset("spark.sql.optimizer.windowGroupLimitThreshold")

# 5. Which filters get the limit.
print("\n== 3. which rank filters get a WindowGroupLimit")
for label, q in [
    ("row_number() = 1", "QUALIFY row_number() OVER w = 1"),
    ("rank() <= 3", "QUALIFY rank() OVER w <= 3"),
    ("dense_rank() < 4", "QUALIFY dense_rank() OVER w < 4"),
    ("row_number() > 1", "QUALIFY row_number() OVER w > 1"),
    ("ntile(4) = 1", "QUALIFY ntile(4) OVER w = 1"),
]:
    df = spark.sql(f"SELECT Borough, trip_distance FROM june WINDOW w AS "
                   f"(PARTITION BY Borough ORDER BY trip_distance DESC) {q}")
    n = nodes(df)
    print(f"  {label:<18} WindowGroupLimit x{n.count('WindowGroupLimit')}")

# 6. QUALIFY rules.
print("\n== 4. QUALIFY rules")
for label, sql in [
    ("alias of a window", "SELECT Borough, Zone, count(*) AS n, rank() OVER (PARTITION BY Borough "
                          "ORDER BY count(*) DESC) AS r FROM june GROUP BY Borough, Zone QUALIFY r <= 2"),
    ("no window function", "SELECT Borough FROM june QUALIFY trip_distance > 50"),
    ("aggregate in QUALIFY", "SELECT Borough, Zone FROM june GROUP BY Borough, Zone "
                             "QUALIFY count(*) > 10"),
]:
    try:
        rows = spark.sql(sql).orderBy("Borough").collect()
        print(f"  {label:<22} ok, {len(rows)} rows, first {tuple(rows[0])}")
    except PySparkException as e:
        print(f"  {label:<22} {err(e)}")

# 7. Ties: row_number picks one row, rank keeps all.
print("\n== 5. ties")
for f in ("row_number", "rank"):
    n = spark.sql(f"SELECT Borough FROM june QUALIFY {f}() OVER "
                  f"(PARTITION BY Borough ORDER BY passenger_count DESC) = 1").count()
    print(f"  {f}() = 1 on passenger_count: {n:,} rows")

# 8. Without a window: max_by and min_by with a count (4.2), and the pipe aggregate.
print("\n== 6. top-N without a window")
spark.sql("""SELECT Borough, max_by(Zone, trip_distance) AS zone_of_longest,
                    max_by(round(trip_distance, 1), trip_distance, 3) AS top3
             FROM june GROUP BY Borough ORDER BY Borough""").show(truncate=False)
q = spark.sql("SELECT Borough, max_by(trip_distance, trip_distance, 3) AS top3 FROM june GROUP BY Borough")
print("plan nodes:", [n for n in nodes(q) if "Aggregate" in n or "Window" in n or "Exchange" in n])

# 9. Pipe syntax: every operator, one question.
print("\n== 7. pipe operators")
spark.sql("""
FROM june
|> WHERE Borough IN ('Manhattan', 'Queens', 'Brooklyn')
|> EXTEND tip_amount / fare_amount AS tip_share
|> SET tip_share = round(tip_share, 3)
|> AGGREGATE count(*) AS trips, avg(tip_share) AS avg_tip_share GROUP BY Borough, payment_type
|> WHERE trips > 1000
|> EXTEND rank() OVER (PARTITION BY Borough ORDER BY trips DESC) AS r
|> WHERE r <= 2
|> DROP r
|> AS t
|> SELECT t.Borough, t.payment_type, t.trips, round(t.avg_tip_share, 3) AS avg_tip_share
|> ORDER BY Borough, trips DESC""").show()

# 10. Aggregates in a pipe SELECT, and the column order of AGGREGATE.
print("== 8. AGGREGATE against SELECT with an aggregate")
a = spark.sql("FROM june |> AGGREGATE count(*) AS trips GROUP BY Borough")
s = spark.sql("FROM june |> SELECT Borough, count(*) AS trips GROUP BY Borough")
print("AGGREGATE columns:", a.columns, "| SELECT columns:", s.columns)
try:
    print("pipe SELECT count(*), no GROUP BY:", spark.sql("FROM june |> SELECT count(*) AS n").first().n)
except PySparkException as e:
    print("pipe SELECT count(*), no GROUP BY:", err(e))
try:
    spark.sql("FROM june |> SELECT Borough, count(*) AS n")
    print("pipe SELECT with a bare column and an aggregate: ok")
except PySparkException as e:
    print("pipe SELECT with a bare column and an aggregate:", err(e))

# 11. The one-character pipe.
print("\n== 9. | as a pipe")
for sql in ("FROM zones | WHERE Borough = 'Queens' | AGGREGATE count(*) AS n", "SELECT 1 | 2 AS bits"):
    try:
        print(f"  {sql:<66} -> {spark.sql(sql).first()[0]}")
    except PySparkException as e:
        print(f"  {sql:<66} -> {err(e)}")

# 12. Pipes append to any query, and live inside views and CTEs.
print("\n== 10. pipes after a query, in a CTE")
print(spark.sql("""
WITH busy AS (SELECT Zone, count(*) AS n FROM june GROUP BY Zone |> WHERE n > 100000)
SELECT * FROM busy |> ORDER BY n DESC |> LIMIT 3""").collect())

# 13. Pivot and unpivot as pipe operators.
print("\n== 11. PIVOT and UNPIVOT")
pv = spark.sql("""
FROM june
|> SELECT Borough, payment_type
|> PIVOT (count(*) FOR payment_type IN (1 AS card, 2 AS cash))
|> WHERE Borough IN ('Manhattan', 'Queens')
|> ORDER BY Borough""")
pv.show()
pv.createOrReplaceTempView("pv")
spark.sql("FROM pv |> UNPIVOT (trips FOR method IN (card, cash)) |> ORDER BY Borough, method").show()

# 14. Other SQL aggregates from 4.x that replace windows or subqueries.
print("== 12. listagg and approx_top_k")
spark.sql("""SELECT Borough, listagg(DISTINCT service_zone, ', ') WITHIN GROUP (ORDER BY service_zone) AS zones
             FROM zones GROUP BY Borough ORDER BY Borough""").show(truncate=False)
print(spark.sql("SELECT approx_top_k(Zone, 3) AS top FROM june").first().top)
