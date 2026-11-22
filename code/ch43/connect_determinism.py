"""Chapter 43, Step 9: Classic keeps a DataFrame's optimized plan, Connect analyzes every action again.

Start the server first:  ch43/server.sh start
Run from code/:          uv run python ch43/connect_determinism.py
"""
import os
import time

from pyspark.sql import SparkSession, functions as F

from chaos import fresh

PORT = os.environ.get("CONNECT_PORT", "10280")


def compare(spark, mode):
    sql_df = spark.sql("SELECT rand() AS r, uuid() AS u, current_timestamp() AS t")
    seeded = spark.sql("SELECT rand(42) AS r, uuid() AS u, current_timestamp() AS t")
    api_df = spark.range(1).select(F.rand().alias("r"), F.uuid().alias("u"),
                                   F.current_timestamp().alias("t"))
    for name, df in [("spark.sql", sql_df), ("spark.sql rand(42)", seeded), ("DataFrame API", api_df)]:
        a = df.collect()[0]
        time.sleep(1.1)
        b = df.collect()[0]
        print(f"{mode:8} {name:19} same rand: {a.r == b.r!s:5}  same uuid: {a.u == b.u!s:5}  "
              f"same timestamp: {a.t == b.t!s:5}")


compare(fresh("local[2]"), "classic")
SparkSession.getActiveSession().stop()
with SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate() as spark:
    compare(spark, "connect")
    # Where the DataFrame API's stable seed comes from: the client wrote it into the plan.
    lit = spark.range(1).select(F.rand())._plan.to_proto(spark.client)
    print("\nthe client-side seed in the plan sent for F.rand():",
          str(lit).split("long: ")[1].split()[0])
