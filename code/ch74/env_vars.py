"""Chapter 74: environment variables that change a config, and where they stop.

SPARK_ANSI_SQL_MODE sets the default of spark.sql.ansi.enabled (SQLConf.scala in 4.3.0:
createWithDefault(!sys.env.get("SPARK_ANSI_SQL_MODE").contains("false"))). It is a default,
so any explicit setting wins over it.

Run from code/:  uv run python ch74/env_vars.py > ch74/output/env_vars.txt
"""
import os
import subprocess
import sys

CHILD = r'''
import os
from pyspark.sql import SparkSession
b = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
if os.environ.get("BUILDER_ANSI"):
    b = b.config("spark.sql.ansi.enabled", os.environ["BUILDER_ANSI"])
spark = b.getOrCreate()
print(spark.conf.get("spark.sql.ansi.enabled"), spark.sql("SELECT try_divide(1, 0) AS a").first()[0])
spark.stop()
'''


def run(label, **env):
    e = {k: v for k, v in os.environ.items() if k not in ("SPARK_ANSI_SQL_MODE", "BUILDER_ANSI")}
    e.update(env)
    out = subprocess.run([sys.executable, "-c", CHILD], env=e, capture_output=True,
                         text=True, check=True).stdout.split()
    print(f"  {label:<52} spark.sql.ansi.enabled = {out[0]}")


print("== the environment sets a default, not a value")
run("no variable")
run("SPARK_ANSI_SQL_MODE=false", SPARK_ANSI_SQL_MODE="false")
run("SPARK_ANSI_SQL_MODE=false + builder.config(..., true)",
    SPARK_ANSI_SQL_MODE="false", BUILDER_ANSI="true")
