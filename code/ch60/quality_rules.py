"""Chapter 60: look at the data before you write rules for it, then gate a load on the rules.

Profiles, rules as data, the NULL trap, Observation metrics, and a gate that the optimizer removes.
Run from code/:  uv run python ch60/quality_rules.py
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Observation
from pyspark.sql import functions as F
from common.book import plan, session

Q = Path(__file__).resolve().parents[1] / "data" / "quality"
spark = session("ch60-profile", **{"spark.sql.shuffle.partitions": "4"})
COLS = ["order_id", "customer_id", "order_ts", "quantity", "unit_price", "status", "currency"]
orders = (spark.read.option("header", True).schema(", ".join(f"{c} STRING" for c in COLS))
          .csv(str(Q / "orders")).select("*", F.col("_metadata.file_name").alias("_source_file")))
customers = spark.read.option("header", True).csv(str(Q / "customers.csv"))

print("== 1. summary() on string columns")
orders.select("quantity", "unit_price").summary("count", "min", "max").show()

print("== 2. a profile in one aggregation: missing, distinct (approximate), most frequent")
aggs = [F.count(F.lit(1)).alias("rows")]
for c in COLS:
    aggs += [F.count_if(F.col(c).isNull() | (F.trim(c) == "")).alias(f"{c}__missing"),
             F.approx_count_distinct(c, 0.01).alias(f"{c}__distinct"),
             F.mode(c, True).alias(f"{c}__top")]
one_row = orders.agg(*aggs)
one_row.select(F.inline(F.array(*[
    F.struct(F.lit(c).alias("column"), "rows", F.col(f"{c}__missing").alias("missing"),
             F.col(f"{c}__distinct").alias("distinct"), F.col(f"{c}__top").alias("top"))
    for c in COLS]))).show(truncate=False)

print("== 3. approximate means approximate")
ids = customers.select("customer_id")
print("  exact distinct customer_id:      ", ids.distinct().count())
for rsd in (0.05, 0.01):
    print(f"  approx_count_distinct, rsd {rsd}: ",
          ids.agg(F.approx_count_distinct("customer_id", rsd)).first()[0])
sk = ids.agg(F.hll_sketch_agg("customer_id").alias("s")).select(F.hll_sketch_estimate("s")).first()[0]
print("  hll_sketch_estimate:             ", sk)
print("  percentile_approx(quantity, 0.5):",
      orders.agg(F.percentile_approx(F.try_to_number("quantity", F.lit("999")), 0.5)).first()[0])

print("\n== 4. rules as data")
rules = json.load(open(Q / "rules.json"))
for r in rules[:3]:
    print("  ", r)
print("   ...", len(rules), "rules")
known = customers.select("customer_id", F.lit(True).alias("_in_dim")).dropDuplicates(["customer_id"])
checked = (orders.join(known, "customer_id", "left")
           .select("*", F.coalesce("_in_dim", F.lit(False)).alias("_customer_known"),
                   F.to_date(F.regexp_extract("_source_file", r"(\d{4}-\d{2}-\d{2})", 1)).alias("_file_date")))


def failed(severity, coalesce=True):
    def test(r):
        e = F.expr(r["check"])
        return ~F.coalesce(e, F.lit(False)) if coalesce else ~e
    return F.array_compact(F.array(*[F.when(test(r), F.lit(r["name"]))
                                     for r in rules if r["severity"] == severity]))


checked = checked.select("*", failed("error").alias("failed_rules"), failed("warn").alias("warnings"),
                         failed("error", coalesce=False).alias("failed_without_coalesce"))
checked.where("size(failed_rules) > 0").select("order_id", "quantity", "unit_price", "failed_rules") \
    .orderBy("order_id").show(6, truncate=False)

print("== 5. the NULL trap: a check that is NULL is not a failure unless you say so")
spark.sql("""SELECT q AS quantity, try_cast(q AS INT) > 0 AS `check`, NOT (try_cast(q AS INT) > 0) AS `not check`,
                    NOT coalesce(try_cast(q AS INT) > 0, false) AS `failed`
             FROM VALUES ('3'), ('-1'), (''), (NULL) AS t(q)""").show()
print("  rows that fail quantity_positive, with coalesce:   ",
      checked.where("array_contains(failed_rules, 'quantity_positive')").count())
print("  rows that fail quantity_positive, without coalesce:",
      checked.where("array_contains(failed_without_coalesce, 'quantity_positive')").count())

print("\n== 6. metrics while you write: DataFrame.observe and Observation")
obs = Observation("quality")
out = Path(spark.conf.get("spark.sql.warehouse.dir").replace("file:", "")) / "ch60_quarantine"
(checked.observe(obs, F.count(F.lit(1)).alias("rows"),
                 F.count_if(F.size("failed_rules") > 0).alias("quarantined"),
                 F.count_if(F.size("warnings") > 0).alias("warned"))
 .where("size(failed_rules) > 0").select("order_id", "failed_rules")
 .write.mode("overwrite").parquet(str(out)))
print("  observed in the same job as the write:", obs.get)
try:
    Observation("unused").get
except Exception as e:
    print("  get before an action ->", type(e).__name__, str(e).split(chr(10))[0][:80])

print("\n== 7. a gate that never ran")
report = checked.groupBy("_source_file").agg(
    F.round(F.count_if(F.size("failed_rules") > 0) / F.count("*"), 4).alias("error_ratio"))
gate = report.agg(F.assert_true(F.max("error_ratio") <= 0.05, "quality gate failed").alias("_gate"))
silent = checked.crossJoin(gate).where("size(failed_rules) = 0").select("order_id")
print("  gate never used:", silent.count(), "rows, no error")
print("  the gate in the plan:", [l.strip(" :+-") for l in plan(silent, "simple").splitlines()
                                  if "keys=[]" in l or "assert_true" in l])
enforced = checked.crossJoin(gate).where(F.col("_gate").isNull()).where("size(failed_rules) = 0").select("order_id")
try:
    enforced.count()
except Exception as e:
    print("  gate in a filter ->", e.getCondition() if hasattr(e, "getCondition") else type(e).__name__,
          str(e).split("\n")[0][:90])
