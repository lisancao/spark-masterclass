"""Chapter 93, Step 5: frequent patterns. Trips as baskets (FPGrowth), days as sequences (PrefixSpan).

Run from code/:  uv run python ch93/patterns.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml.fpm import FPGrowth, PrefixSpan  # noqa: E402
from pyspark.sql import functions as F, Window  # noqa: E402
from common.book import session, plan  # noqa: E402
from ch93.taxi93 import trips, zones  # noqa: E402

spark = session("ch93-patterns", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="16")
z = zones(spark)
t = (trips(spark).sample(fraction=0.1, seed=7)
     .join(z.select(F.col("zone").alias("PULocationID"), F.col("zone_name").alias("pu_name")), "PULocationID")
     .join(z.select(F.col("zone").alias("DOLocationID"), F.col("zone_name").alias("do_name")), "DOLocationID"))

# one basket per trip: the items that describe it
items = F.filter(F.array(
    F.concat(F.lit("from "), F.col("pu_name")),
    F.concat(F.lit("to "), F.col("do_name")),
    F.when(F.col("RatecodeID") == 2, "JFK flat fare"),
    F.when(F.col("payment_type") == 1, "card").when(F.col("payment_type") == 2, "cash"),
    F.when(F.col("hour").isin(22, 23, 0, 1, 2, 3, 4, 5), "night"),
    F.when(F.col("Airport_fee") > 0, "airport fee"),
    F.when(F.col("tolls_amount") > 0, "toll"),
    F.when(F.col("tip_pct") >= 20, "tip >= 20%"),
    F.when(F.col("trip_distance") >= 10, "10+ miles")), lambda x: x.isNotNull())
baskets = t.select(F.array_distinct(items).alias("items")).cache()
print(f"== FPGrowth: {baskets.count():,} baskets (a 10% sample of the trips), one per trip")
baskets.show(3, truncate=False)

fp = FPGrowth(itemsCol="items", minSupport=0.01, minConfidence=0.5)
m = fp.fit(baskets)
print(f"frequent itemsets with support >= 1%: {m.freqItemsets.count()}")
m.freqItemsets.orderBy(F.desc("freq")).show(8, truncate=False)
print(f"association rules with confidence >= 0.5: {m.associationRules.count()}; with at most 2 antecedent items, by lift:")
(m.associationRules.where(F.size("antecedent") <= 2).orderBy(F.desc("lift"))
  .select("antecedent", "consequent", F.round("confidence", 3).alias("confidence"),
          F.round("lift", 2).alias("lift"), F.round("support", 4).alias("support"))
  .show(10, truncate=False))

print("transform(): the consequents of each rule whose antecedent is in the basket")
new = spark.createDataFrame([(["from JFK Airport", "toll"],), (["from Midtown Center", "night"],)], "items array<string>")
m.transform(new).show(truncate=False)
print("the plan of transform():")
print("\n".join(plan(m.transform(new), "simple").splitlines()[:4]))

print("\nduplicates in a basket:")
spark.sparkContext.setLogLevel("OFF")
try:
    fp.fit(spark.createDataFrame([(["card", "card", "night"],)], "items array<string>"))
except Exception as e:
    msg = " ".join(str(e).split())
    print("  " + type(e).__name__ + ": " + msg[msg.index("Items in a transaction"):msg.index(").", msg.index("Items in a transaction")) + 2])
    print("  the job started first:", "Job aborted due to stage failure" in msg)

# ---- PrefixSpan: one sequence per day; each element is the set of the 3 busiest pickup zones of an hour
print("\n== PrefixSpan: one sequence per day, an itemset per hour (the 3 busiest pickup zones)")
full = trips(spark).join(z.select(F.col("zone").alias("PULocationID"), F.col("zone_name").alias("pu")), "PULocationID")
by_hour = full.groupBy(F.to_date("tpep_pickup_datetime").alias("day"), "hour", "pu").count()
w = Window.partitionBy("day", "hour").orderBy(F.desc("count"), "pu")
top3 = (by_hour.withColumn("r", F.row_number().over(w)).where("r <= 3")
        .groupBy("day", "hour").agg(F.sort_array(F.collect_list("pu")).alias("zones")))
seqs = (top3.groupBy("day").agg(F.sort_array(F.collect_list(F.struct("hour", "zones"))).alias("s"))
        .select("day", F.col("s.zones").alias("sequence")).cache())
print(f"{seqs.count()} sequences (days), {seqs.select(F.size('sequence')).first()[0]} itemsets each")
first = seqs.orderBy("day").first()
print(f"{first.day}: hour 0 {first.sequence[0]}, hour 8 {first.sequence[8]}, hour 18 {first.sequence[18]}")
ps = PrefixSpan(minSupport=1.0, maxPatternLength=3, sequenceCol="sequence")
pats = ps.findFrequentSequentialPatterns(seqs).cache()
print(f"patterns on all {seqs.count()} days (minSupport=1.0, maxPatternLength=3): {pats.count()}")
multi = pats.where(F.exists("sequence", lambda e: F.size(e) >= 2))
print(f"patterns with an itemset of two zones in the same hour: {multi.count()}")
multi.orderBy(F.size("sequence").desc(), F.col("sequence").cast("string")).show(4, truncate=False)
print("three-step patterns with JFK Airport:")
(pats.where((F.size("sequence") == 3) & F.exists("sequence", lambda e: F.array_contains(e, "JFK Airport")))
     .orderBy(F.col("sequence").cast("string")).show(4, truncate=False))
spark.stop()
