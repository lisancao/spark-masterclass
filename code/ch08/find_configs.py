"""Chapter 8: four ways to find a config and its value: SET, SET -v, spark.conf and the REST API.

Run from code/:  uv run python ch08/find_configs.py > ch08/output/find_configs.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, ui_api

spark = session("ch08-find", **{"spark.sql.shuffle.partitions": "16",
                                "spark.myteam.db.password": "hunter2"})

print("== 1. SET -v: every public SQL config, with its meaning")
allv = spark.sql("SET -v")
print("  columns:", allv.columns)
print("  rows:   ", allv.count())
(allv.where(F.col("key").startswith("spark.sql.adaptive.coalescePartitions"))
     .select("key", "value", "Since version").show(truncate=False))

print("== 1b. SET -v rows by the version that added the config")
(allv.withColumn("since", F.regexp_extract("Since version", r"^(\d+\.\d+)", 1))
     .where(F.col("since") >= "4.0").groupBy("since").count().orderBy("since").show())
(allv.where(F.col("Since version") == "4.3.0").select("key").orderBy("key")
     .show(40, truncate=False))

print("== 2. SET -v, searched for a word in the meaning")
(allv.where(F.lower("meaning").contains("broadcast"))
     .select("key", "value").orderBy("key").show(truncate=False))

print("== 3. SET with no argument: only the keys that something set")
setonly = spark.sql("SET")
print("  rows:", setonly.count())
setonly.where(F.col("key").rlike("shuffle.partitions|password|app.name|warehouse")).show(truncate=False)

print("== 4. SET for one key, also for a key that nothing set")
spark.sql("SET spark.sql.ansi.enabled").show(truncate=False)
spark.sql("SET spark.sql.files.maxPartitionBytes").show(truncate=False)

print("== 5. spark.conf.get with and without a default")
print("  spark.conf.get('spark.sql.files.maxPartitionBytes') =",
      spark.conf.get("spark.sql.files.maxPartitionBytes"))
print("  spark.conf.get('spark.myteam.none', 'fallback')      =",
      spark.conf.get("spark.myteam.none", "fallback"))
try:
    spark.conf.get("spark.myteam.none")
except Exception as e:
    print("  spark.conf.get('spark.myteam.none')                  ->", str(e).split("]")[0] + "]")

print("  spark.conf.get('spark.myteam.db.password')           =",
      spark.conf.get("spark.myteam.db.password"))

print("\n== 6. the REST API: /environment (the Environment tab, as JSON)")
env = ui_api("environment")
print("  sections:", list(env))
props = dict(env["sparkProperties"])
print("  sparkProperties:", len(props), "entries")
for k in ["spark.sql.shuffle.partitions", "spark.myteam.db.password", "spark.master", "spark.app.id"]:
    print(f"    {k:<30} {props.get(k, '(absent)')}")
print("  a SQL config set only at runtime is absent here:")
spark.conf.set("spark.sql.ansi.enabled", "false")
props = dict(ui_api("environment")["sparkProperties"])
print(f"    spark.sql.ansi.enabled in sparkProperties: {'spark.sql.ansi.enabled' in props}")
spark.stop()
