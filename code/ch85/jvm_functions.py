"""Chapter 85, Step 12: the JVM function options next to a native expression.

A Java UDF1 (ScalaUDF), a Hive GenericUDF, a typed Aggregator registered with functions.udaf,
the legacy UserDefinedAggregateFunction, and Hive's own GenericUDAFAverage: how each one is
registered, what it does with NULL, and which plan node it becomes.
Run from code/:  ch85/jfun/build.sh && ch85/build.sh && uv run python ch85/jvm_functions.py
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import BooleanType  # noqa: E402
from common.book import session, plan, TRIPS, ROOT  # noqa: E402

HERE = Path(__file__).resolve().parent
JARS = f"{HERE / 'jfun' / 'target' / 'jfun.jar'},{HERE / 'jext' / 'target' / 'jext-0.1.0.jar'}"
META = ROOT / "data" / "work" / "ch85-metastore"
SIG = b"\x89PNG\r\n\x1a\n"


def nodes(df, keys=("HiveGenericUDF", "UDF(", "is_png", "static_invoke", "HashAggregate", "ObjectHashAggregate",
                    "SortAggregate", "mean_fare", "hive_avg", "avg(")):
    p = plan(df, "simple").split("== Physical Plan ==")[-1]
    out = []
    for line in p.splitlines():
        s = line.strip(" +-*()0123456789")
        if any(k in line for k in keys):
            out.append(s[:100])
    return out


# ---- 1. a Hive UDF needs a session with Hive support
plain = session("ch85-jvm-plain", spark_jars=JARS, spark_ui_showConsoleProgress="false")
print("== 1. CREATE TEMPORARY FUNCTION ... AS 'jfun.IsPngHive', without Hive support")
print(f"   spark.sql.catalogImplementation = {plain.conf.get('spark.sql.catalogImplementation')}")
try:
    plain.sql("CREATE TEMPORARY FUNCTION is_png_hive AS 'jfun.IsPngHive'")
    plain.sql("SELECT is_png_hive(X'89504E470D0A1A0A')").collect()
except Exception as e:
    print(f"   {type(e).__name__}: {' '.join(str(e).split())[:150]}")
plain.stop()

spark = session("ch85-jvm", spark_jars=JARS, spark_sql_extensions="jext.Extensions",
                spark_ui_showConsoleProgress="false",
                **{"spark.sql.catalogImplementation": "hive",
                   "spark.sql.warehouse.dir": str(META / "warehouse"),
                   "spark.hadoop.javax.jdo.option.ConnectionURL":
                       f"jdbc:derby:;databaseName={META / 'metastore_db'};create=true"})
print(f"\n   with spark.sql.catalogImplementation = {spark.conf.get('spark.sql.catalogImplementation')}:")
spark.sql("CREATE TEMPORARY FUNCTION is_png_hive AS 'jfun.IsPngHive'")
spark.sql("CREATE TEMPORARY FUNCTION hive_avg AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage'")
print("   " + str(spark.sql("SELECT is_png_hive(X'89504E470D0A1A0A') AS sig, is_png_hive(X'00') AS other").first()))

print("\n== 2. register the Java functions from Python")
spark.udf.registerJavaFunction("is_png_udf", "jfun.IsPngUdf", BooleanType())
print("   spark.udf.registerJavaFunction('is_png_udf', 'jfun.IsPngUdf', BooleanType()): ok")
spark.udf.registerJavaUDAF("mean_fare_udaf", "jfun.MeanFareUdaf")
print("   spark.udf.registerJavaUDAF('mean_fare_udaf', 'jfun.MeanFareUdaf'): ok (a UserDefinedAggregateFunction)")
try:
    spark.udf.registerJavaUDAF("mean_fare", "jfun.MeanFare")
except Exception as e:
    print(f"   spark.udf.registerJavaUDAF('mean_fare', 'jfun.MeanFare') -> {type(e).__name__}: "
          f"{' '.join(str(e).split())[:100]}")
spark._jvm.jfun.Register.all(spark._jsparkSession)
print("   spark._jvm.jfun.Register.all(spark._jsparkSession): mean_fare = functions.udaf(new MeanFare(), "
      "Encoders.DOUBLE())")

print("\n== 3. the same answer, NULL included")
vals = spark.createDataFrame([(SIG + b"rest",), (b"GIF89a",), (b"\x89P",), (None,)], "b binary")
vals.selectExpr("is_png(b) AS native", "is_png_fast(b) AS static_invoke", "is_png_udf(b) AS java_udf",
                "is_png_hive(b) AS hive_udf").show()
fares = spark.createDataFrame([(1, 10.0), (1, 20.0), (1, None), (2, None)], "zone int, fare double")
(fares.groupBy("zone").agg(F.avg("fare").alias("avg"), F.expr("mean_fare(fare)").alias("aggregator"),
                           F.expr("mean_fare_udaf(fare)").alias("udaf"), F.expr("hive_avg(fare)").alias("hive_avg"))
      .orderBy("zone").show())

print("== 4. the plan node of each one")
blobs = spark.range(10).select(F.when(F.col("id") % 3 == 0, F.lit(SIG)).otherwise(F.lit(b"GIF89a")).alias("b"))
for name, col in (("native is_png_fast", "is_png_fast(b)"), ("Java UDF1", "is_png_udf(b)"),
                  ("Hive GenericUDF", "is_png_hive(b)")):
    first = plan(blobs.selectExpr(f"{col} AS p"), "simple").split("== Physical Plan ==")[-1].strip().splitlines()[0]
    fn = first.split("Project [")[1].split("(")[0]
    print(f"   {name:<20} {first[:4]}  expression {fn:<28} "
          f"{'in whole-stage codegen' if first.startswith('*') else 'not in whole-stage codegen'}")
trips = spark.read.parquet(str(TRIPS))
for name, col in (("built-in avg", "avg(fare_amount)"), ("Aggregator (udaf)", "mean_fare(fare_amount)"),
                  ("legacy UDAF", "mean_fare_udaf(fare_amount)"), ("Hive GenericUDAF", "hive_avg(fare_amount)")):
    df = trips.groupBy("PULocationID").agg(F.expr(col).alias("m"))
    ops = [n.split(" ")[0].split("(")[0] for n in nodes(df) if "Aggregate" in n]
    print(f"   {name:<20} {' -> '.join(reversed(ops))}")

print("\n== 5. a Hive UDF checks its argument types in initialize()")
try:
    spark.sql("SELECT is_png_hive('not binary')").collect()
except Exception as e:
    print(f"   {type(e).__name__}: {' '.join(str(e).split())[:300]}")
spark.stop()
