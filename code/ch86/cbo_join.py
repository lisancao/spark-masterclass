"""Chapter 86: CBO estimates change the join strategy and the join order. Timings under the bench lock.

Needs the tables from stats.py.  Run from code/:  uv run python ch86/cbo_join.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from c86 import start, plan, median_s, noop, last_sql_metrics

spark = start("ch86-cbo-join")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
# Column statistics without histograms for the join keys, and a histogram for fares.total_amount.
spark.conf.set("spark.sql.statistics.histogram.enabled", "false")
spark.sql("ANALYZE TABLE trips COMPUTE STATISTICS FOR COLUMNS trip_id, PULocationID")
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS FOR COLUMNS trip_id")
spark.sql("ANALYZE TABLE zones COMPUTE STATISTICS FOR COLUMNS LocationID, Borough, Zone")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS FOR COLUMNS total_amount")


def joins(df):
    p = plan(df, "simple").split("== Physical Plan ==")[-1]
    return re.findall(r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin)", p)


def exchanges(df):
    return plan(df, "simple").split("== Physical Plan ==")[-1].count("Exchange hashpartitioning")


print("=== 1. one filter on the small side: the strategy follows the estimate")
SQL1 = """SELECT t.PULocationID, f.total_amount FROM trips t JOIN fares f ON t.trip_id = f.trip_id
          WHERE f.total_amount > 500"""
res = {}
for cbo in ("false", "true"):
    spark.conf.set("spark.sql.cbo.enabled", cbo)
    q = spark.sql(SQL1)
    est = [re.sub(r".*Statistics\(", "Statistics(", l).strip() for l in plan(q, "cost").splitlines()
           if l.lstrip(":- +").startswith("Filter") and "total_amount" in l and "Statistics(" in l]
    print(f"  spark.sql.cbo.enabled={cbo}")
    print(f"    estimate of the filtered fares: {est[0] if est else '?'}")
    print(f"    planned join: {joins(q)}, hash exchanges planned: {exchanges(q)}")
    res[cbo] = median_s(lambda: noop(spark.sql(SQL1)))
    q2 = spark.sql(SQL1); q2.collect()          # run this DataFrame, so that explain() shows its final plan
    final = plan(q2, "simple").split("== Final Plan ==")[1].split("== Initial Plan ==")[0]
    j = re.findall(r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin)", final)
    ex = final.count("Exchange hashpartitioning")
    print(f"    final plan after AQE: {j}, hash exchanges that ran: {ex}; median {res[cbo]:.2f} s")
print(f"  rows: {spark.sql(SQL1).count()}")

print("\n=== 2. join reorder: three tables, broadcast off so that order decides the shuffle size")
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
SQL2 = """SELECT z.Zone, count(*) AS trips, sum(f.total_amount) AS amount
          FROM trips t JOIN fares f ON t.trip_id = f.trip_id
                       JOIN zones z ON t.PULocationID = z.LocationID
          WHERE z.Borough = 'Staten Island'
          GROUP BY z.Zone"""


def join_tree(df):
    """The Join and relation lines of the optimized logical plan, indented as Spark prints them."""
    opt = plan(df, "extended").split("== Optimized Logical Plan ==")[1].split("== Physical Plan ==")[0]
    out = []
    for l in opt.splitlines():
        s = l.lstrip(":- +")
        if s.startswith("Join") or s.startswith("Relation"):
            ind = len(l) - len(l.lstrip(":- +"))
            name = re.search(r"Relation spark_catalog\.default\.(\w+)", s)
            out.append(" " * ind + (s.split(",")[0].split(" (")[0] if s.startswith("Join") else f"Relation {name.group(1)}"))
    pad = min(len(l) - len(l.lstrip()) for l in out)
    return [l[pad:] for l in out]


timing = {}
for reorder in ("false", "true"):
    spark.conf.set("spark.sql.cbo.joinReorder.enabled", reorder)
    q = spark.sql(SQL2)
    print(f"  spark.sql.cbo.joinReorder.enabled={reorder}: optimized join tree")
    for l in join_tree(q):
        print(f"    {l}")
    timing[reorder] = median_s(lambda: spark.sql(SQL2).collect())
    spark.sql(SQL2).collect()
    smj = last_sql_metrics(spark, "SortMergeJoin")
    rows = sorted((int(str(m.get("number of output rows", "0")).replace(",", "")) for _, m in smj), reverse=True)
    print(f"    rows out of the two SortMergeJoin nodes: {' and '.join(f'{r:,}' for r in rows)}")
    print(f"    median {timing[reorder]:.2f} s")
print(f"  result rows: {len(spark.sql(SQL2).collect())}")
print(f"  speed-up from reorder: {timing['false'] / timing['true']:.1f}x")

print("\n=== 3. the same query after histograms on the join keys")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
spark.sql("ANALYZE TABLE trips COMPUTE STATISTICS FOR COLUMNS trip_id, PULocationID")
spark.sql("ANALYZE TABLE zones COMPUTE STATISTICS FOR COLUMNS LocationID, Borough, Zone")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
q = spark.sql(SQL2)
for l in join_tree(q):
    print(f"    {l}")
for l in plan(q, "cost").split("== Physical Plan ==")[0].splitlines():
    if l.lstrip(":- +").startswith("Join") and "LocationID" in l:
        print("    estimate of the zones join: " + re.sub(r".*Statistics\(", "Statistics(", l).strip())
real = spark.sql("SELECT count(*) FROM trips t JOIN zones z ON t.PULocationID = z.LocationID WHERE z.Borough = 'Staten Island'").first()[0]
print(f"    real rows of trips JOIN zones (Staten Island): {real:,}")
# put the join keys back to statistics without histograms for the other scripts
spark.conf.set("spark.sql.statistics.histogram.enabled", "false")
spark.sql("ANALYZE TABLE trips COMPUTE STATISTICS FOR COLUMNS trip_id, PULocationID")
spark.sql("ANALYZE TABLE zones COMPUTE STATISTICS FOR COLUMNS LocationID, Borough, Zone")
