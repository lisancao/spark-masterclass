"""Chapter 83: GROUP BY and its plans: two phases, the three aggregate operators, 4.3 plan rules,
FILTER clauses and DISTINCT aggregates. Run from code/:  uv run python ch83/groupby.py
"""
from aggbook import start, nodes, short, row_counts, sql_metrics

spark = start("ch83-groupby")


def show_nodes(df, final=False, keep=None):
    for line in nodes(df, final):
        if keep is None or any(k in line for k in keep):
            print(short(line))


# 1. Two phases: a partial aggregate before the shuffle, a final aggregate after it.
print("== 1. GROUP BY payment_type: partial and final aggregates")
q = spark.sql("""SELECT payment_type, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare
                 FROM june GROUP BY payment_type ORDER BY payment_type""")
q.show()
show_nodes(q, final=True)
print("rows out of each node:")
for name, desc, n in row_counts(spark):
    print(f"  {name:<28} {n:>10,}   {short(desc, 50)}")
ex = [m for nm, _, m in sql_metrics(spark, ("shuffle records written", "shuffle bytes written"))
      if nm == "Exchange" and m]
print("  shuffle:", ex[0])
print("tasks in the scan stage:", spark.read.parquet(str(__import__("aggbook").TRIPS)).rdd.getNumPartitions())

# 2. Which operator: it depends on the type of each aggregation buffer.
print("\n== 2. which aggregate operator")
CASES = [
    ("count(*), sum(fare_amount)", "numbers"),
    ("max(fare_amount)", "a number"),
    ("max(Zone)", "a string"),
    ("first(Zone)", "a string"),
    ("percentile_approx(fare_amount, 0.5)", "an object"),
    ("collect_list(VendorID)", "an object"),
    ("approx_count_distinct(DOLocationID)", "fixed words"),
]
for agg, buf in CASES:
    df = spark.sql(f"SELECT Borough, {agg} FROM june GROUP BY Borough")
    ops = [l.strip().split("(")[0].lstrip("+- ") for l in nodes(df) if "Aggregate(" in l]
    print(f"  {agg:<38} buffer: {buf:<11} -> {ops[0]}")
spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "false")
df = spark.sql("SELECT Borough, percentile_approx(fare_amount, 0.5) FROM june GROUP BY Borough")
ops = [l.strip().split("(")[0].lstrip("+- ") for l in nodes(df) if "Aggregate(" in l]
print(f"  percentile_approx, useObjectHashAggregateExec=false        -> {ops[0]}")
spark.conf.unset("spark.sql.execution.useObjectHashAggregateExec")

# 2b. ObjectHashAggregate falls back to sorting after 128 keys in one task.
print("\n== 2b. ObjectHashAggregate: the sort fallback")
for key, label in (("Borough", "8 boroughs"), ("PULocationID", "zones")):
    spark.sql(f"SELECT {key}, percentile_approx(fare_amount, 0.5) FROM june GROUP BY {key}").collect()
    fb = [(d, m) for nm, d, m in sql_metrics(spark, ("number of sort fallback tasks", "number of output rows"))
          if nm == "ObjectHashAggregate" and "partial_" in d]
    print(f"  GROUP BY {key:<13} partial rows out {fb[0][1]['number of output rows']:>6}, "
          f"sort fallback tasks: {fb[0][1]['number of sort fallback tasks']}")

# 3. New in 4.3: adjacent partial and final aggregates become one, and a sorted child gets a SortAggregate.
print("\n== 3. GROUP BY after a sort-merge join on the same key")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
J = """SELECT t.PULocationID, count(*) AS trips FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
       GROUP BY t.PULocationID"""
df = spark.sql(J)
df.collect()
print("-- 4.3.0 defaults")
show_nodes(df, final=True, keep=("Aggregate", "Exchange", "Sort", "Join"))
spark.conf.set("spark.sql.execution.combineAdjacentAggregation", "false")
spark.conf.set("spark.sql.execution.replaceHashWithSortAgg", "false")
df = spark.sql(J)
df.collect()
print("-- combineAdjacentAggregation=false, replaceHashWithSortAgg=false")
show_nodes(df, final=True, keep=("Aggregate", "Exchange", "Sort", "Join"))
spark.conf.unset("spark.sql.execution.combineAdjacentAggregation")
spark.conf.unset("spark.sql.execution.replaceHashWithSortAgg")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# 3b. New in 4.3: whole-stage code for SortAggregate with grouping keys.
print("\n== 3b. SortAggregate with keys in whole-stage code")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
for on in ("true", "false"):
    spark.conf.set("spark.sql.codegen.aggregate.sortAggregate.withKeys.enabled", on)
    for label, s in (("count(*) after the join", J),
                     ("max(Zone), a string buffer", "SELECT Borough, max(Zone) FROM june GROUP BY Borough")):
        df = spark.sql(s)
        df.collect()
        lines = [l.strip().lstrip("+- ") for l in nodes(df, final=True) if "SortAggregate" in l]
        print(f"  withKeys.enabled={on:<5} {label:<27} " + " | ".join(l.split("(key")[0] for l in lines))
spark.conf.unset("spark.sql.codegen.aggregate.sortAggregate.withKeys.enabled")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# 4. New in 4.3: bypassPartialAggregation, for keys that hardly repeat.
print("\n== 4. partial aggregation on a key that hardly repeats")
H = "SELECT tpep_pickup_datetime, count(*) AS n FROM june GROUP BY tpep_pickup_datetime"
for on in ("false", "true"):
    spark.conf.set("spark.sql.execution.bypassPartialAggregation", on)
    df = spark.sql(H)
    df.write.format("noop").mode("overwrite").save()
    print(f"-- bypassPartialAggregation={on}")
    show_nodes(df, final=True, keep=("Aggregate", "Exchange"))
    for name, desc, n in row_counts(spark):
        if "Aggregate" in name or "Scan" in name:
            print(f"     rows out: {name:<22} {n:>10,}  {short(desc, 44)}")
spark.conf.unset("spark.sql.execution.bypassPartialAggregation")

# 5. FILTER: each aggregate sees only its own rows.
print("\n== 5. aggregate FILTER clauses")
F = """SELECT Borough,
          count(*)                                   AS trips,
          count(*) FILTER (WHERE tip_amount > 0)     AS tipped,
          round(avg(tip_amount) FILTER (WHERE tip_amount > 0), 2) AS avg_tip_if_tipped,
          round(avg(tip_amount), 2)                  AS avg_tip_all
       FROM june WHERE payment_type = 1 GROUP BY Borough ORDER BY trips DESC"""
df = spark.sql(F)
df.show()
same = spark.sql("""SELECT Borough, count(*) AS trips, count_if(tip_amount > 0) AS tipped,
          round(avg(CASE WHEN tip_amount > 0 THEN tip_amount END), 2) AS avg_tip_if_tipped,
          round(avg(tip_amount), 2) AS avg_tip_all
       FROM june WHERE payment_type = 1 GROUP BY Borough ORDER BY trips DESC""")
print("same rows as count_if and CASE WHEN:", df.collect() == same.collect())
import re
for line in nodes(df, final=True):
    if "HashAggregate(" in line:
        fns = re.search(r"functions=\[(.*)\]\)", line).group(1)
        print("partial:" if "partial_" in fns else "final:  ")
        for f in re.split(r", (?=(?:partial_)?(?:count|avg)\()", fns):
            print("    " + f)

# 6. DISTINCT aggregates.
print("\n== 6. DISTINCT aggregates")
D1 = "SELECT payment_type, count(DISTINCT PULocationID) AS zones FROM june GROUP BY payment_type"
D2 = """SELECT payment_type, count(DISTINCT PULocationID) AS pu_zones,
              count(DISTINCT DOLocationID) AS do_zones FROM june GROUP BY payment_type"""
D3 = """SELECT payment_type, count(DISTINCT PULocationID) FILTER (WHERE tip_amount > 0) AS tipped_zones
        FROM june GROUP BY payment_type"""
for label, s in (("one DISTINCT", D1), ("two DISTINCT", D2), ("DISTINCT with FILTER", D3)):
    df = spark.sql(s)
    df.write.format("noop").mode("overwrite").save()
    print(f"-- {label}")
    show_nodes(df, final=True, keep=("Aggregate", "Exchange", "Expand"))
    for name, desc, n in row_counts(spark):
        if name.startswith(("Scan", "Expand")):
            print(f"     rows out: {name:<22} {n:>10,}")
spark.sql(D2 + " ORDER BY payment_type").show()
