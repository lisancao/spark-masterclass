"""Chapter 5: the everyday DataFrame operations on a small table with an explicit schema.

The table is the "blogs" example from Learning Spark (Example 3-6), the one SparkTutor's lesson uses.
Run from code/:  uv run python ch05/basics.py
Part 6 runs again on Spark Connect when SPARK_REMOTE is set (server: ch06/connect_server.sh start).
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from common.book import session, plan

spark = session("ch05-basics", spark_sql_session_timeZone="UTC")
CONNECT = bool(os.environ.get("SPARK_REMOTE"))


def err(f):
    try:
        return f()
    except Exception as e:
        return "ERROR " + str(e).splitlines()[0][:84]


if not CONNECT:
    # 1. An explicit schema, with an array column.
    print("== 1. the blogs table, with an explicit schema")
    schema = StructType([
        StructField("Id", IntegerType(), False),
        StructField("First", StringType()),
        StructField("Last", StringType()),
        StructField("Url", StringType()),
        StructField("Published", StringType()),
        StructField("Hits", IntegerType()),
        StructField("Campaigns", ArrayType(StringType())),
    ])
    data = [
        (1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]),
        (2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]),
        (3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]),
        (4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]),
        (5, "Matei", "Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]),
        (6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]),
    ]
    blogs = spark.createDataFrame(data, schema)
    blogs.printSchema()

    # 2. The everyday operations. Each returns a new DataFrame.
    print("== 2. select, where, withColumns, drop, rename, sort")
    report = (blogs
              .where(F.col("Hits") > 5000)
              .withColumns({"Big_Hitter": F.col("Hits") > 10000,
                            "Published_Date": F.to_date("Published", "M/d/yyyy"),
                            "Channels": F.size("Campaigns")})
              .drop("Url", "Published", "Campaigns")
              .withColumnRenamed("First", "first_name")
              .orderBy(F.desc("Hits")))
    report.show()
    print("columns:", report.columns)
    print("drop a name that is not there:", blogs.drop("no_such_column").columns == blogs.columns)

    # 3. SQL strings inside the DataFrame API: expr, selectExpr and the :: cast.
    print("\n== 3. expr, selectExpr and SQL expressions")
    tiers = blogs.selectExpr(
        "Id",
        "CASE WHEN Hits > 20000 THEN 'star' WHEN Hits > 8000 THEN 'regular' ELSE 'new' END AS tier",
        "Hits::STRING || ' hits' AS label",
        "array_contains(Campaigns, 'FB') AS on_fb")
    tiers.show()
    same = blogs.select(F.when(F.col("Hits") > 20000, "star").when(F.col("Hits") > 8000, "regular")
                        .otherwise("new").alias("tier"))
    print("expr vs when/otherwise, same optimized plan:",
          tiers.select("tier")._jdf.queryExecution().optimizedPlan().sameResult(
              same._jdf.queryExecution().optimizedPlan()))

    # 4. Two columns with one name: a self join.
    print("\n== 4. the same column name on two sides")
    a, b = blogs.alias("a"), blogs.alias("b")
    pairs = a.join(b, F.col("a.Hits") < F.col("b.Hits"))
    print("pairs.columns has", pairs.columns.count("Hits"), "columns named Hits")
    print("select('Hits'):", err(lambda: pairs.select("Hits").collect()))
    print("select('a.Hits', 'b.Hits'):", pairs.select("a.Hits", "b.Hits").count(), "rows")

    # 5. Python data in: NumPy, and the JSON prefersDecimal option.
    print("\n== 5. other inputs")
    arr = np.array([[1, 2.5], [2, 3.5]])
    print("from a NumPy array:", spark.createDataFrame(arr).schema.simpleString())
    js = spark.createDataFrame([('{"amount": 0.1}',), ('{"amount": 12.25}',)], "j STRING")
    print("JSON default:      ", spark.read.json(js.rdd.map(lambda r: r.j)).schema.simpleString())
    print("JSON prefersDecimal:", spark.read.option("prefersDecimal", True)
          .json(js.rdd.map(lambda r: r.j)).schema.simpleString())
    for v in ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"]:
        spark.conf.set("spark.sql.timestampType", v)
        print(f"spark.sql.timestampType={v}: typeof(TIMESTAMP'2026-06-01 10:00') =",
              spark.sql("SELECT typeof(TIMESTAMP'2026-06-01 10:00')").first()[0])
    spark.conf.unset("spark.sql.timestampType")

    # 6. VARIANT, a first look: a JSON value kept as one typed column.
    print("\n== 6. VARIANT")
    v = spark.sql("""SELECT parse_json('{"device": {"os": "ios", "ver": 17}, "tags": ["a", "b"]}') AS payload""")
    v.selectExpr("typeof(payload)", "variant_get(payload, '$.device.os', 'string') AS os",
                 "payload:device.ver::int AS ver", "payload:tags[1]::string AS tag2").show()

# 7. A bad column name: Classic fails at once; Connect fails later.
print("== 7. a bad column name, mode:", "Connect" if CONNECT else "Classic")
df = spark.range(3)
print("df['typo']:", err(lambda: repr(df["typo"])))
print("df.select(df['typo']).collect():", err(lambda: df.select(df["typo"]).collect()))
