"""Chapter 43: how each way of running Python shows up in a physical plan.

Classic. Run from code/:  uv run python ch43/plan_nodes.py
"""
import sys, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

warnings.filterwarnings("ignore")
spark = session("ch43-plan-nodes", spark_sql_session_timeZone="UTC")
trips = spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount", "tip_amount")
print("spark.sql.ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))
print("spark.sql.execution.pythonUDF.arrow.enabled =", spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled"))


def band_builtin(c):
    return (F.when(c.isNull() | (c <= 0), "invalid").when(c < 10, "under $10")
            .when(c < 30, "$10 to $30").otherwise("over $30"))


@F.udf("string")
def band_udf(fare):
    if fare is None or fare <= 0:
        return "invalid"
    return "under $10" if fare < 10 else "$10 to $30" if fare < 30 else "over $30"


band_pickled = F.udf(band_udf.func, "string", useArrow=False)


@F.pandas_udf("string")
def band_pandas(fare: pd.Series) -> pd.Series:
    out = pd.Series("over $30", index=fare.index)
    out[fare < 30] = "$10 to $30"
    out[fare < 10] = "under $10"
    out[fare.isna() | (fare <= 0)] = "invalid"
    return out


@F.arrow_udf("double")
def tip_share(tip: pa.Array, fare: pa.Array) -> pa.Array:
    return pc.divide(tip, fare)


def per_zone(pdf: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"PULocationID": [pdf.PULocationID.iloc[0]], "p90": [pdf.fare_amount.quantile(0.9)]})


CASES = [
    ("built-in when()", trips.select(band_builtin(F.col("fare_amount")))),
    ("@udf (Arrow by default)", trips.select(band_udf("fare_amount"))),
    ("@udf(useArrow=False)", trips.select(band_pickled("fare_amount"))),
    ("@pandas_udf, scalar", trips.select(band_pandas("fare_amount"))),
    ("@arrow_udf, scalar", trips.select(tip_share("tip_amount", "fare_amount"))),
    ("mapInPandas", trips.mapInPandas(lambda it: (b[b.fare_amount > 100] for b in it), trips.schema)),
    ("mapInArrow", trips.mapInArrow(lambda it: (b.filter(pc.greater(b["fare_amount"], 100)) for b in it),
                                    trips.schema)),
    ("groupBy().applyInPandas", trips.groupBy("PULocationID").applyInPandas(per_zone, "PULocationID int, p90 double")),
    ("two @udf, nested", trips.select(band_udf(band_udf("fare_amount")))),
    ("@udf inside @pandas_udf", trips.select(band_pandas(F.length(band_udf("fare_amount"))))),
]
NODES = ("ArrowEvalPython", "BatchEvalPython", "MapInPandas", "PythonMapInArrow", "MapInArrow",
         "FlatMapGroupsInPandas", "Exchange", "Sort ")
for name, df in CASES:
    lines = [l.strip().lstrip("+- ").lstrip(":- ") for l in plan(df, "simple").splitlines()
             if any(n in l for n in NODES)]
    print(f"\n{name}:")
    for l in lines or ["(no Python node: the JVM evaluates it)"]:
        print("   " + l[:100])
