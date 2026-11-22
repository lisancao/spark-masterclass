"""Chapter 83: PIVOT and UNPIVOT in SQL and in the DataFrame API, and the plans that they make.
Run from code/:  uv run python ch83/pivots.py
"""
import re
from aggbook import start, nodes, short, row_counts
from pyspark.sql import functions as F

spark = start("ch83-pivots")
spark.sql("""CREATE OR REPLACE TEMP VIEW small AS
SELECT Borough, CASE payment_type WHEN 1 THEN 'card' WHEN 2 THEN 'cash' END AS pay, fare_amount,
       tip_amount, Zone, hour(tpep_pickup_datetime) AS hour
FROM june WHERE Borough IN ('Manhattan', 'Queens', 'Brooklyn') AND payment_type IN (1, 2)""")


def logical(df):
    """Node names of the optimized logical plan, one per line, indented."""
    out = []
    for line in df._jdf.queryExecution().optimizedPlan().toString().splitlines():
        s = re.sub(r"#\d+L?", "", line)
        out.append(short(s, 100))
    return out


# 1. SQL PIVOT: values become columns.
print("== 1. PIVOT in SQL")
P = """SELECT * FROM (SELECT Borough, pay, fare_amount FROM small)
       PIVOT (count(*) AS trips, round(avg(fare_amount), 2) AS fare FOR pay IN ('card' AS card, 'cash' AS cash))
       ORDER BY Borough"""
spark.sql(P).show()

# 2. The plan: group by (Borough, pay), then a second aggregate with PivotFirst.
print("== 2. the optimized plan of the PIVOT")
for l in logical(spark.sql(P)):
    print(l)

# 3. Every column that the PIVOT does not name becomes a grouping column.
print("\n== 3. the implicit GROUP BY of a SQL PIVOT")
for label, src in (("subquery with 3 columns", "(SELECT Borough, pay, fare_amount FROM small)"),
                   ("the whole view (6 columns)", "small")):
    df = spark.sql(f"SELECT * FROM {src} PIVOT (count(*) FOR pay IN ('card', 'cash'))")
    print(f"  {label:<28} columns: {df.columns}  rows: {df.count():,}")

# 4. An aggregate type that PivotFirst cannot hold: one Aggregate with an IF for each value.
print("\n== 4. PIVOT of a string aggregate: no PivotFirst")
S = """SELECT * FROM (SELECT Borough, pay, Zone, fare_amount FROM small)
       PIVOT (max_by(Zone, fare_amount) FOR pay IN ('card' AS card, 'cash' AS cash)) ORDER BY Borough"""
spark.sql(S).show(truncate=False)
for l in logical(spark.sql(S)):
    print(l)

# 5. The DataFrame API: pivot with values, and pivot without values (an extra job, at once).
print("\n== 5. pivot in the DataFrame API")
small = spark.table("small")
tracker = spark.sparkContext.statusTracker()


def jobs():
    return len(tracker.getJobIdsForGroup())


j0 = jobs()
with_values = small.groupBy("Borough").pivot("pay", ["card", "cash"]).agg(F.count(F.lit(1)))
j1 = jobs()
without_values = small.groupBy("Borough").pivot("pay").agg(F.count(F.lit(1)))
j2 = jobs()
print(f"  jobs run by pivot('pay', ['card', 'cash']): {j1 - j0}")
print(f"  jobs run by pivot('pay'):                   {j2 - j1}   (before any action)")
print("  columns:", without_values.columns)
print("  same rows as SQL:", sorted(without_values.collect(), key=str) == sorted(
    spark.sql("SELECT * FROM (SELECT Borough, pay FROM small) PIVOT (count(*) FOR pay IN ('card', 'cash'))")
    .collect(), key=str))
try:
    small.groupBy("Borough").pivot("pay", ["card", "cash"]).agg(F.count("*"))
except Exception as e:
    print("  pivot(...).agg(count('*')):", str(e).splitlines()[0][:70])
spark.conf.set("spark.sql.pivotMaxValues", "100")
try:
    spark.table("june").groupBy("Borough").pivot("Zone").agg(F.count("*"))
except Exception as e:
    msg = " ".join(str(e).split())
    print("  pivot('Zone') with spark.sql.pivotMaxValues=100:", short(msg, 140))
spark.conf.unset("spark.sql.pivotMaxValues")

# 6. UNPIVOT: columns become rows again.
print("\n== 6. UNPIVOT in SQL")
spark.sql("""CREATE OR REPLACE TEMP VIEW wide AS
SELECT * FROM (SELECT Borough, hour, tip_amount FROM small WHERE hour IN (3, 8, 18))
PIVOT (round(avg(tip_amount), 2) FOR hour IN (3 AS h03, 8 AS h08, 18 AS h18))""")
spark.sql("""CREATE OR REPLACE TEMP VIEW wide2 AS
SELECT * FROM wide UNION ALL SELECT 'Test', CAST(NULL AS DOUBLE), 1.0D, CAST(NULL AS DOUBLE)""")
spark.sql("SELECT * FROM wide2 ORDER BY Borough").show()
U = "SELECT * FROM wide2 UNPIVOT {nulls} (avg_tip FOR hour IN (h03, h08, h18)) ORDER BY Borough, hour"
for nulls in ("", "INCLUDE NULLS"):
    df = spark.sql(U.format(nulls=nulls))
    print(f"-- UNPIVOT {nulls or '(EXCLUDE NULLS is the default)'}: {df.count()} rows")
df = spark.sql(U.format(nulls=""))
df.show()
for l in logical(df):
    print(l)

# 7. UNPIVOT in the DataFrame API, and a multi-value UNPIVOT.
print("\n== 7. unpivot and melt in the DataFrame API")
wide2 = spark.table("wide2")
api = wide2.unpivot("Borough", ["h03", "h08", "h18"], "hour", "avg_tip")
print("  unpivot rows:", api.count(), "| melt rows:", wide2.melt("Borough", None, "hour", "avg_tip").count(),
      "| same as SQL INCLUDE NULLS:",
      sorted(api.collect(), key=str) == sorted(spark.sql(U.format(nulls="INCLUDE NULLS")).collect(), key=str))
spark.sql("""SELECT * FROM (SELECT Borough, pay, count(*) AS n, round(avg(fare_amount), 2) AS fare
                            FROM small GROUP BY Borough, pay)
             PIVOT (first(n) AS n, first(fare) AS fare FOR pay IN ('card' AS card, 'cash' AS cash))
             UNPIVOT ((n, fare) FOR pay IN ((card_n, card_fare) AS card, (cash_n, cash_fare) AS cash))
             ORDER BY Borough, pay""").show()
