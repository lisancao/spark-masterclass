"""Chapter 56, Steps 4-5: the eval type each Python API gets, and when ExtractPythonUDFs fuses UDFs.

Run from code/:  uv run python ch56/eval_types.py
"""
import re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from typing import Iterator
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import functions as F
from common.book import session, plan
from logs import access_parquet, _area

spark = session("ch56-eval-types")
df = access_parquet(spark)

def python_nodes(q):
    """The lines of the physical plan that are Python evaluation or map nodes."""
    return [l.strip().lstrip("+- *()0123456789").strip() for l in plan(q, "simple").splitlines()
            if re.search(r"EvalPython|MapIn|FlatMapGroups", l)]

area = F.udf(_area, "string")                                   # Arrow-optimized by default
area_pickled = F.udf(_area, "string", useArrow=False)
@F.pandas_udf("string")
def strip_q(s: pd.Series) -> pd.Series:
    return s.str.split("?").str[0]
@F.pandas_udf("string")
def strip_q_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for s in it:
        yield s.str.split("?").str[0]
@F.arrow_udf("string")
def strip_q_arrow(a: pa.Array) -> pa.Array:
    return pc.utf8_slice_codeunits(a, 0, 8)
upper = F.udf(lambda s: s.upper(), "string")

print("spark.sql.execution.pythonUDF.arrow.enabled =",
      spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled"), "\n")
cases = {
    "udf(useArrow=False)":       df.select(area_pickled("path")),
    "udf (default)":             df.select(area("path")),
    "pandas_udf, Series":        df.select(strip_q("path")),
    "pandas_udf, iterator":      df.select(strip_q_iter("path")),
    "arrow_udf":                 df.select(strip_q_arrow("path")),
    "mapInPandas":               df.mapInPandas(lambda it: it, df.schema),
    "mapInArrow":                df.mapInArrow(lambda it: it, df.schema),
}
for name, q in cases.items():
    node = python_nodes(q)[0]
    print(f"{name:<22} {node[:64]}")

print("\nFusion:")
fusion = {
    "nested, same type    upper(area(path))":   df.select(upper(area("path"))),
    "siblings, same type  area(path), upper(ip)": df.select(area("path"), upper("ip")),
    "nested, 101 over 200 area(strip_q(path))": df.select(area(strip_q("path"))),
    "JVM expr between     upper(concat(area(path), '!'))":
        df.select(upper(F.concat(area("path"), F.lit("!")))),
    "iterator siblings    strip_q_iter(path), strip_q_iter(ip)":
        df.select(strip_q_iter("path"), strip_q_iter("ip")),
    "same call twice      area(path), area(path)": df.select(area("path").alias("a"), area("path").alias("b")),
}
for name, q in fusion.items():
    print(f"\n{name}")
    for n in python_nodes(q):
        print(f"  {n}")
