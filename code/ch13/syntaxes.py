"""Chapter 13: one question in three syntaxes (DataFrame, SQL, pipe SQL), the plans they make,
constant folding, and parameters.

Classic only (it reads plans through _jdf to compare them).
Run from code/:  uv run python ch13/syntaxes.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.errors import PySparkException
from pyspark.sql import functions as F
from common.book import session, plan, TRIPS, ZONES, MONTH_START, MONTH_END

spark = session("ch13-syntaxes", spark_sql_session_timeZone="UTC")
trips = spark.read.parquet(str(TRIPS))
zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")


def err(e):
    first = next(l for l in str(e).splitlines() if l.strip())
    return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:70]


# 1. The question: per borough, trips and average fare of clean June trips, busy boroughs only.
df_q = (trips
        .where((F.col("tpep_pickup_datetime") >= MONTH_START) & (F.col("tpep_pickup_datetime") < MONTH_END))
        .where((F.col("fare_amount") > 0) & F.col("trip_distance").between(0.1, 100))
        .join(zones, trips.PULocationID == zones.LocationID)
        .groupBy("Borough")
        .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
        .where(F.col("trips") > 10000)
        .orderBy(F.desc("trips")))

sql_q = spark.sql(f"""
SELECT z.Borough, count(*) AS trips, round(avg(t.fare_amount), 2) AS avg_fare
FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
WHERE t.tpep_pickup_datetime >= '{MONTH_START}' AND t.tpep_pickup_datetime < '{MONTH_END}'
  AND t.fare_amount > 0 AND t.trip_distance BETWEEN 0.1 AND 100
GROUP BY z.Borough
HAVING count(*) > 10000
ORDER BY trips DESC""")

PIPE = f"""
FROM trips AS t
|> WHERE t.tpep_pickup_datetime >= '{MONTH_START}' AND t.tpep_pickup_datetime < '{MONTH_END}'
|> WHERE t.fare_amount > 0 AND t.trip_distance BETWEEN 0.1 AND 100
|> JOIN zones AS z ON t.PULocationID = z.LocationID
|> AGGREGATE count(*) AS trips, round(avg(t.fare_amount), 2) AS avg_fare GROUP BY z.Borough
|> WHERE trips > 10000
|> ORDER BY trips DESC"""
pipe_q = spark.sql(PIPE)

print("== 1. one question, three syntaxes")
pipe_q.show()
rows = [q.collect() for q in (df_q, sql_q, pipe_q)]
print("same rows:", rows[0] == rows[1] == rows[2])
opt = [q._jdf.queryExecution().optimizedPlan() for q in (df_q, sql_q, pipe_q)]
print("same optimized plan, DataFrame vs SQL:", opt[0].sameResult(opt[1]))
print("same optimized plan, SQL vs pipe:     ", opt[1].sameResult(opt[2]))


# 2. The plans before the optimizer: what the parser and the analyzer made of each one.
def shape(p, depth=8):
    """Node names of a plan tree, one per line, indented (no arguments)."""
    out = []

    def walk(n, d):
        if len(out) < 40:
            out.append("  " * d + n.nodeName())
            for i in range(n.children().size()):
                walk(n.children().apply(i), d + 1)
    walk(p, 0)
    return out


print("\n== 2. parsed plans (node names only)")
for name, q in (("sql", sql_q), ("pipe", pipe_q)):
    print(f"--- {name}")
    print("\n".join(shape(q._jdf.queryExecution().logical())))

# 3. Every prefix of a pipe query is a query.
print("\n== 3. each prefix of the pipe query")
lines = [l for l in PIPE.strip().splitlines()]
for n in range(1, len(lines) + 1):
    q = spark.sql("\n".join(lines[:n]))
    print(f"  {n} operators: {len(q.columns):>2} columns, {q.count():>9,} rows   {lines[n - 1][:46]}")

# 4. explain modes.
print("\n== 4. explain modes")
for mode in ("simple", "extended", "formatted", "cost", "codegen"):
    text = plan(sql_q, mode)
    heads = [l for l in text.splitlines() if l.startswith("==")]
    print(f"  {mode:<10} {len(text.splitlines()):>4} lines  {' '.join(heads)[:70]}")
cost = plan(sql_q, "cost")
print("  cost mode, first operator lines:")
for l in cost.splitlines()[1:4]:
    print("   ", l.strip()[:100])

# 5. The optimizer evaluates constants once, at plan time. Under ANSI that includes errors.
print("\n== 5. constant folding")
print([l.strip() for l in plan(spark.sql("SELECT 1 + 2 * 3 AS x, upper('nyc') AS y FROM range(1)"),
                                "extended").split("== Optimized Logical Plan ==")[1].splitlines()[1:2]])
zero = spark.sql("SELECT 1/0 AS x FROM range(10) WHERE false")
print("defined a query with 1/0 and WHERE false:", zero.columns)
try:
    zero.collect()
except PySparkException as e:
    print("collect():", err(e))
    frames = [l.strip() for l in e._stackTrace.splitlines() if "org.apache.spark.sql.catalyst" in l]
    print("  first Spark frames:", *[f.removeprefix("at ").split("(")[0] for f in frames[:4]], sep="\n    ")

# 6. Parameters: values are bound as literals, never pasted into the SQL text.
print("\n== 6. parameters")
named = spark.sql("SELECT count(*) AS n FROM trips WHERE fare_amount > :min_fare AND PULocationID = :zone",
                  args={"min_fare": 100, "zone": 132})
print("named   :min_fare, :zone  ->", named.first().n)
pos = spark.sql("SELECT count(*) AS n FROM trips WHERE fare_amount > ? AND PULocationID = ?", args=[100, 132])
print("positional ?, ?            ->", pos.first().n)
print("IDENTIFIER(:tbl)           ->",
      spark.sql("SELECT count(*) AS n FROM IDENTIFIER(:tbl)", args={"tbl": "zones"}).first().n)
print("DataFrame as {t}           ->",
      spark.sql("SELECT count(*) AS n FROM {t} WHERE Borough = {b}", t=zones, b="Queens").first().n)
user_input = "0 OR 1 = 1"
print("f-string with user input   ->",
      spark.sql(f"SELECT count(*) AS n FROM trips WHERE fare_amount > {user_input}").first().n)
try:
    spark.sql("SELECT count(*) AS n FROM trips WHERE fare_amount > :v", args={"v": user_input}).first()
except PySparkException as e:
    print("the same input as :v       ->", err(e))
try:
    spark.sql("SELECT count(*) FROM IDENTIFIER(:tbl)", args={"tbl": "zones; DROP TABLE trips"}).first()
except PySparkException as e:
    print("IDENTIFIER with a bad name ->", err(e))
