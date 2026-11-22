"""Chapter 56, Step 3: watch two rules rewrite a plan, then time every rule in the session.

The plan change log is written by the JVM's log4j to stderr, so run this with stderr kept:
  uv run python ch56/rules.py 2>&1 | grep -v NativeCodeLoader
Classic only (spark._jvm).
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session
from logs import access_parquet, _area

spark = session("ch56-rules")
df = access_parquet(spark)
area = F.udf(_area, "string")

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.planChangeLog.level", "WARN")
spark.conf.set("spark.sql.planChangeLog.rules",
               "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates,"
               "org.apache.spark.sql.execution.python.ExtractPythonUDFs")
spark.conf.set("spark.sql.planChangeLog.batches", "Operator Optimization before Inferring Filters,"
               "Extract Python UDFs")
q = df.withColumn("area", area("path")).filter(F.col("area") == "api")
sys.stdout.flush()
q.explain()
sys.stdout.flush()
time.sleep(0.5)                      # let log4j finish writing to stderr before stdout continues
spark.conf.unset("spark.sql.planChangeLog.level")
spark.sparkContext.setLogLevel("ERROR")

table = spark._jvm.org.apache.spark.sql.catalyst.rules.RuleExecutor.dumpTimeSpent()
lines = [l for l in table.splitlines() if l.strip()]
print(lines[0]); print(lines[1]); print(lines[2])
print(f"{'rule (package org.apache.spark.sql. removed)':<58}{'effective ns':>13}{'total ns':>13}  runs")
for l in lines[4:13]:
    name, eff, _, tot, runs, _, total_runs = l.split()
    print(f"{name.replace('org.apache.spark.sql.', ''):<58}{int(eff):>13,}{int(tot):>13,}  {runs}/{total_runs}")
