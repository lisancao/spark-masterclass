"""Chapter 37: which queries single-task execution applies to, read from their plans and their tasks.

For each query shape: the Exchange nodes in the physical plan and the tasks the query ran, with
spark.sql.optimizer.singleTaskExecution.enabled off and then on. Classic only (job labels, df.rdd).
Run from code/:  uv run python ch37/eligibility.py
"""
import shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F, Window
from common.book import session, plan, ui_api, TRIPS, TAXI, ZONES

spark = session("ch37-eligibility", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"
TMP = Path("/tmp/masterclass-ch37")
shutil.rmtree(TMP, ignore_errors=True)

# Inputs: the zone lookup CSV (one 12 KB file), the June trips (one 62 MB Parquet file),
# six months of trips (six files), and the zones split into two files.
zones = lambda: spark.read.option("header", True).csv(str(ZONES))
june = lambda: spark.read.parquet(str(TRIPS))
spark.read.option("header", True).csv(str(ZONES)).repartition(2).write.parquet(str(TMP / "zones2"))
two_files = lambda: spark.read.parquet(str(TMP / "zones2"))
pdf = zones().toPandas()
from_pandas = lambda n: spark.createDataFrame(pd.concat([pdf] * 4).head(n))
rows = [tuple(r) for r in pdf.itertuples(index=False)]
spark.udf.register("upper_py", lambda s: s.upper() if s else s, "string")
zones().createOrReplaceTempView("zones")

by_borough = Window.partitionBy("Borough").orderBy("LocationID")
SHAPES = [
    ("CSV: groupBy, orderBy", lambda: zones().groupBy("Borough").count().orderBy("Borough")),
    ("CSV: distinct", lambda: zones().select("Borough").distinct()),
    ("CSV: window rank", lambda: zones().withColumn("r", F.rank().over(by_borough))),
    ("CSV: rollup", lambda: zones().rollup("Borough", "service_zone").count()),
    ("CSV: orderBy, limit 5", lambda: zones().orderBy("Zone").limit(5)),
    ("CSV: offset 5", lambda: zones().orderBy("Zone").offset(5)),
    ("Parquet, 62 MB: groupBy", lambda: june().groupBy("PULocationID").count()),
    ("SQL VALUES: groupBy", lambda: spark.sql("SELECT k, count(*) FROM VALUES (1), (1), (2) AS t(k) GROUP BY k")),
    ("pandas, 1,000 rows: groupBy", lambda: from_pandas(1000).groupBy("Borough").count()),
    ("pandas, 1,001 rows: groupBy", lambda: from_pandas(1001).groupBy("Borough").count()),
    ("Python list: groupBy", lambda: spark.createDataFrame(rows, pdf.columns.tolist()).groupBy("Borough").count()),
    ("CSV: top 3 per group", lambda: zones().withColumn("r", F.row_number().over(by_borough)).where("r <= 3")),
    ("CSV: self join", lambda: zones().alias("a").join(zones().alias("b"), "LocationID")),
    ("CSV: union of two reads", lambda: zones().union(zones()).groupBy("Borough").count()),
    ("CSV: Python UDF", lambda: zones().groupBy(F.expr("upper_py(Borough)")).count()),
    ("CSV: Python UDF, pruned", lambda: zones().select(F.expr("upper_py(Zone)")).groupBy().count()),
    ("CSV: scalar subquery", lambda: spark.sql(
        "SELECT * FROM zones WHERE LocationID > (SELECT avg(LocationID) FROM zones) ORDER BY Zone")),
    ("Parquet, 2 files: groupBy", lambda: two_files().groupBy("Borough").count()),
    ("CSV: groupBy, write Parquet", lambda: "parquet"),
    ("CSV: groupBy, write noop", lambda: "noop"),
    ("Parquet, 6 files: groupBy", lambda: spark.read.parquet(str(TAXI / "yellow-h1")).groupBy("PULocationID").count()),
]

WRITES = {
    "parquet": lambda: zones().groupBy("Borough").count().write.mode("overwrite").parquet(str(TMP / "out")),
    "noop": lambda: zones().groupBy("Borough").count().write.mode("overwrite").format("noop").save(),
}


def measure(build, enabled, label):
    spark.conf.set(FLAG, enabled)
    df = build()
    if isinstance(df, str):                      # a write: no DataFrame plan to print
        exchanges = "-"
        spark.sparkContext.setJobDescription(label)
        WRITES[df]()
    else:
        exchanges = plan(df, "simple").count("Exchange")
        spark.sparkContext.setJobDescription(label)
        df.collect()
    spark.sparkContext.setJobDescription(None)
    jobs = [j for j in ui_api("jobs") if j.get("description") == label]
    tasks = sum(j["numTasks"] - j["numSkippedTasks"] for j in jobs)
    return exchanges, len(jobs), tasks


print(f"{'query':<30}{'off: exch jobs tasks':>22}{'on: exch jobs tasks':>22}")
for i, (name, build) in enumerate(SHAPES):
    off = measure(build, "false", f"q{i} off")
    on = measure(build, "true", f"q{i} on")
    print(f"{name:<30}{off[0]:>10}{off[1]:>6}{off[2]:>6}{on[0]:>10}{on[1]:>6}{on[2]:>6}")

# Two settings that turn it off for a query that qualifies.
print()
spark.conf.set(FLAG, "true")
for key, value in [("spark.sql.files.maxPartitionBytes", "32m"), ("spark.sql.leafNodeDefaultParallelism", "4")]:
    spark.conf.set(key, value)
    print(f"{key}={value}: June scan partitions {june().rdd.getNumPartitions()}, "
          f"exchanges in groupBy {plan(june().groupBy('PULocationID').count(), 'simple').count('Exchange')}")
    spark.conf.unset(key)
print(f"defaults: June scan partitions {june().rdd.getNumPartitions()}")
spark.conf.set(FLAG, "false")
print(f"flag off: June scan partitions {june().rdd.getNumPartitions()}")

# What the leaf of each DataFrame is, after optimization.
leaf = lambda df: df._jdf.queryExecution().optimizedPlan().collectLeaves().head().nodeName()
print("\nleaf nodes: Python list ->", leaf(spark.createDataFrame(rows, pdf.columns.tolist())),
      "| pandas ->", leaf(from_pandas(10)), "| VALUES ->", leaf(spark.sql("SELECT * FROM VALUES (1) AS t(k)")))
