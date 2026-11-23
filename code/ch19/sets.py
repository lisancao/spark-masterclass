"""Chapter 19: GROUPING SETS, ROLLUP and CUBE, grouping() and grouping_id(), and the Expand operator.
Run from code/:  uv run python ch19/sets.py
"""
from aggbook import start, nodes, short, row_counts

spark = start("ch19-sets")
spark.sql("""CREATE OR REPLACE TEMP VIEW small AS
SELECT Borough, CASE payment_type WHEN 1 THEN 'card' WHEN 2 THEN 'cash' END AS pay, fare_amount,
       passenger_count, tpep_pickup_datetime
FROM june WHERE Borough IN ('Manhattan', 'Queens', 'Brooklyn') AND payment_type IN (1, 2)""")

# 1. ROLLUP: totals for each level, in one query.
print("== 1. ROLLUP(Borough, pay)")
R = """SELECT Borough, pay, count(*) AS trips, grouping(Borough) AS gb, grouping(pay) AS gp,
              grouping_id() AS gid
       FROM small GROUP BY ROLLUP(Borough, pay) ORDER BY gid, Borough, pay"""
spark.sql(R).show()

# 2. The plan: one Expand that copies each row once per grouping set.
print("== 2. the plan of the ROLLUP")
df = spark.sql(R)
df.write.format("noop").mode("overwrite").save()
for line in nodes(df):
    if any(k in line for k in ("Aggregate", "Expand", "Exchange")):
        print(short(line, 120))
for name, desc, n in row_counts(spark):
    if name in ("Filter", "Expand") or "Aggregate" in name:
        print(f"  rows out: {name:<15} {n:>10,}")

# 3. CUBE and grouping_id bits.
print("\n== 3. CUBE(Borough, pay): every combination")
C = """SELECT grouping_id() AS gid, bin(grouping_id()) AS bits,
              CASE WHEN grouping(Borough) = 1 THEN '(all)' ELSE Borough END AS Borough,
              CASE WHEN grouping(pay) = 1 THEN '(all)' ELSE pay END AS pay,
              count(*) AS trips
       FROM small GROUP BY CUBE(Borough, pay) ORDER BY gid, Borough, pay"""
spark.sql(C).show(20)
df = spark.sql(C)
print([l.strip()[:200] for l in nodes(df) if "Expand" in l][0].replace("+- ", ""))

# 4. GROUPING SETS is the general form, and it equals a UNION ALL that reads the data N times.
print("\n== 4. GROUPING SETS against UNION ALL")
G = """SELECT Borough, pay, count(*) AS trips FROM small
       GROUP BY GROUPING SETS ((Borough), (pay), ())"""
U = """SELECT Borough, NULL AS pay, count(*) AS trips FROM small GROUP BY Borough
       UNION ALL SELECT NULL, pay, count(*) FROM small GROUP BY pay
       UNION ALL SELECT NULL, NULL, count(*) FROM small"""
g, u = spark.sql(G), spark.sql(U)
print("same rows:", sorted(g.collect(), key=str) == sorted(u.collect(), key=str))
for label, df in (("GROUPING SETS", g), ("UNION ALL", u)):
    df.write.format("noop").mode("overwrite").save()
    scans = [n for name, _, n in row_counts(spark) if name == "Scan parquet"]
    print(f"  {label:<14} parquet scans: {len(scans)}, rows read: {sum(scans):,}")

# 5. A NULL key and a subtotal both print NULL. grouping() tells them apart.
print("\n== 5. NULL keys against subtotal rows")
spark.sql("""SELECT passenger_count, count(*) AS trips, grouping(passenger_count) AS g
             FROM june WHERE passenger_count IS NULL OR passenger_count = 1
             GROUP BY ROLLUP(passenger_count) ORDER BY g, passenger_count""").show()

# 6. Mixed forms: a plain column with ROLLUP gives the cross product of the sets.
print("== 6. GROUP BY Borough, ROLLUP(pay, hour)")
M = """SELECT Borough, pay, hour(tpep_pickup_datetime) AS hour, count(*) AS trips, grouping_id() AS gid
       FROM small WHERE hour(tpep_pickup_datetime) IN (8, 18)
       GROUP BY Borough, ROLLUP(pay, hour(tpep_pickup_datetime))"""
spark.sql(M).groupBy("gid").count().orderBy("gid").show()

# 7. The DataFrame API: rollup, cube and groupingSets.
print("== 7. the same in the DataFrame API")
from pyspark.sql import functions as F
small = spark.table("small")
api = {
    "rollup": small.rollup("Borough", "pay").agg(F.count("*").alias("trips")),
    "cube": small.cube("Borough", "pay").agg(F.count("*").alias("trips")),
    "groupingSets": small.groupingSets([["Borough"], ["pay"], []], "Borough", "pay").agg(
        F.count("*").alias("trips")),
}
sqls = {"rollup": "ROLLUP(Borough, pay)", "cube": "CUBE(Borough, pay)",
        "groupingSets": "GROUPING SETS ((Borough), (pay), ())"}
for k, df in api.items():
    s = spark.sql(f"SELECT Borough, pay, count(*) AS trips FROM small GROUP BY {sqls[k]}")
    print(f"  {k:<13} rows: {df.count():>2}   same rows as SQL: "
          f"{sorted(df.collect(), key=str) == sorted(s.collect(), key=str)}")

# 8. An empty grouping set is a grand total, also over an empty input.
print("\n== 8. GROUPING SETS (()) over an empty input")
for s in ("SELECT count(*) AS n FROM small WHERE false",
          "SELECT count(*) AS n FROM small WHERE false GROUP BY GROUPING SETS (())",
          "SELECT count(*) AS n FROM small WHERE false GROUP BY Borough"):
    print(f"  {len(spark.sql(s).collect())} row(s)   {s.split('WHERE false')[1].strip() or '(no GROUP BY)'}")
