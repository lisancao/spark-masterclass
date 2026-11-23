"""Chapter 94, Step 2: what assertDataFrameEqual and assertSchemaEqual check, and what a failure shows.

Run from code/:  uv run python ch94/asserts.py        (SPARK_REMOTE=sc://... for Connect)
"""
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import Row  # noqa: E402
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType  # noqa: E402
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual  # noqa: E402
from common.book import session  # noqa: E402

spark = session("ch94-asserts", spark_ui_showConsoleProgress="false")
print(f"session: {type(spark).__module__}.{type(spark).__name__}")
actual = spark.createDataFrame([("JFK", 142192, 70.31), ("LGA", 98619, 49.12), ("Midtown", 120511, 17.40)],
                               "zone string, trips long, avg_fare double")


def attempt(title, f):
    print(f"\n== {title}")
    try:
        f()
        print("passed")
    except AssertionError as e:
        cls = type(e).__name__
        cond = getattr(e, "getCondition", lambda: None)()
        lines = str(e).splitlines()
        print(f"{cls} [{cond}]: {len(lines)} lines")
        for line in lines[:14]:
            print("  " + re.sub(r"\x1b\[[0-9;]*m", "", line)[:96])
    except Exception as e:
        print(f"{type(e).__name__} [{e.getCondition()}]: {str(e).splitlines()[0][:150]}")


exp_rows = [Row(zone="LGA", trips=98619, avg_fare=49.12), Row(zone="JFK", trips=142192, avg_fare=70.31),
            Row(zone="Midtown", trips=120511, avg_fare=17.40)]
attempt("rows in another order (checkRowOrder=False, the default)", lambda: assertDataFrameEqual(actual, exp_rows))
attempt("rows in another order, checkRowOrder=True", lambda: assertDataFrameEqual(actual, exp_rows, checkRowOrder=True))
attempt("a float that differs in the 7th digit (rtol=1e-5)",
        lambda: assertDataFrameEqual(actual, [Row(zone="JFK", trips=142192, avg_fare=70.3100004),
                                              Row(zone="LGA", trips=98619, avg_fare=49.12),
                                              Row(zone="Midtown", trips=120511, avg_fare=17.40)]))
wrong = spark.createDataFrame([("JFK", 142192, 70.31), ("LGA", 98620, 49.12), ("Midtown", 120511, 17.41)],
                              "zone string, trips long, avg_fare double")
attempt("two wrong values", lambda: assertDataFrameEqual(actual, wrong))
attempt("two wrong values, showOnlyDiff=True", lambda: assertDataFrameEqual(actual, wrong, showOnlyDiff=True))
attempt("a column of another type (trips as int)",
        lambda: assertDataFrameEqual(actual, actual.selectExpr("zone", "CAST(trips AS INT) AS trips", "avg_fare")))
attempt("the same, ignoreColumnType=True",
        lambda: assertDataFrameEqual(actual, actual.selectExpr("zone", "CAST(trips AS INT) AS trips", "avg_fare"),
                                     ignoreColumnType=True))
spark.conf.set("spark.sql.ansi.enabled", "false")
attempt("the same, ignoreColumnType=True, spark.sql.ansi.enabled=false",
        lambda: assertDataFrameEqual(actual, actual.selectExpr("zone", "CAST(trips AS INT) AS trips", "avg_fare"),
                                     ignoreColumnType=True))
spark.conf.unset("spark.sql.ansi.enabled")
attempt("columns in another order, ignoreColumnOrder=True",
        lambda: assertDataFrameEqual(actual, actual.select("avg_fare", "zone", "trips"), ignoreColumnOrder=True))
expected_schema = StructType([StructField("zone", StringType(), False), StructField("trips", LongType()),
                              StructField("avg_fare", DoubleType())])
attempt("assertSchemaEqual, nullable differs (ignoreNullable=True, the default)",
        lambda: assertSchemaEqual(actual.schema, expected_schema))
attempt("assertSchemaEqual, ignoreNullable=False",
        lambda: assertSchemaEqual(actual.schema, expected_schema, ignoreNullable=False))
attempt("pandas DataFrames (the pandas API path)",
        lambda: assertDataFrameEqual(actual.toPandas(), wrong.toPandas()))
spark.stop()
