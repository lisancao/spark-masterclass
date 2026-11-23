"""Chapter 88, Step 6: which filters reach the Parquet scan when a query has Python in it.

Run from code/:  uv run python ch88/filters.py
"""
import re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan
from logs import access_parquet, _area

spark = session("ch88-filters")
df = access_parquet(spark)
area = F.udf(_area, "string")
ok = F.udf(lambda s: s < 400, "boolean")

def show(title, q):
    print(f"--- {title}")
    for line in [l for l in plan(q, "simple").splitlines()[1:] if l.strip()]:
        line = re.sub(r"Batched: true, DataFilters: .*?(PushedFilters: \[.*?\]).*", r"... \1", line)
        print(line)
    print()

show("1. a UDF next to a native condition",
     df.filter((F.col("status") == 200) & (area("path") == "api")))
show("2. a native filter written after a UDF column",
     df.withColumn("a", area("path")).filter("status = 200"))
show("3. the same, with the UDF nondeterministic",
     df.withColumn("a", area.asNondeterministic()("path")).filter("status = 200"))
show("4a. a UDF as the predicate", df.filter(ok("status")))
show("4b. the native predicate", df.filter(F.col("status") < 400))
