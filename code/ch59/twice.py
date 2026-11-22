"""Chapter 59, Step 7: count how many times a Python UDF runs, and three rewrites that run it once.

An accumulator, added to inside the UDF, counts the calls. Classic only (sparkContext).
Run from code/:  uv run python ch59/twice.py
"""
import re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan
from logs import access_parquet, _area

spark = session("ch59-twice")
df = access_parquet(spark)
calls = spark.sparkContext.accumulator(0)

def counted_area(path):
    calls.add(1)
    return _area(path)

area = F.udf(counted_area, "string")

def run(label, action):
    calls.value = 0
    rows = action()
    print(f"  {label:<52} rows {rows:>4}   UDF calls {calls.value:>4}")
    return rows

def evals(q):
    return sum("ArrowEvalPython" in l for l in plan(q, "simple").splitlines())

u = df.withColumn("area", area("path"))
slow = u.filter(F.col("area") == "api").filter(F.col("status") == 200)
print(f"spark.sql.optimizer.avoidDoubleFilterEval = {spark.conf.get('spark.sql.optimizer.avoidDoubleFilterEval')}")
print(f"slow query: {evals(slow)} ArrowEvalPython nodes\n")
print("200 rows; 98 with status 200; 31 API calls among them; 70 API calls in all\n")
run("withColumn(area).collect()", lambda: len(u.collect()))
run("withColumn(area).count()", lambda: u.count())
run("slow: filter on area and status, collect()", lambda: len(slow.collect()))
run("filter on area only, collect()", lambda: len(u.filter("area = 'api'").collect()))
run("u.union(u).collect()", lambda: len(u.union(u).collect()))
nd = df.withColumn("area", area.asNondeterministic()("path"))
run("filter on area only, UDF nondeterministic", lambda: len(nd.filter("area = 'api'").collect()))
run("count(), UDF nondeterministic", lambda: nd.count())

print("\nRewrites of the slow query:")
once = (df.filter(F.col("status") == 200)
          .withColumn("area", area.asNondeterministic()("path"))
          .filter(F.col("area") == "api"))
print(f"  filter first, then nondeterministic UDF: {evals(once)} ArrowEvalPython node, pushed: "
      + re.search(r"PushedFilters: (\[.*?\])", plan(once, "simple")).group(1))
run("  ... collect()", lambda: len(once.collect()))
native = (df.withColumn("area", F.coalesce(F.nullif(F.split(F.split("path", r"\?")[0], "/")[1], F.lit("")),
                                           F.lit("home")))
            .filter(F.col("area") == "api").filter(F.col("status") == 200))
print(f"  native functions: {evals(native)} ArrowEvalPython nodes")
run("  ... collect()", lambda: len(native.collect()))

print("\nMaterialize, and the cache trap:")
c = df.withColumn("area", area("path")).cache()
run("c = u.cache(); c.count() builds the cache", lambda: c.count())
run("c.filter(area = 'api').collect()", lambda: len(c.filter("area = 'api'").collect()))
c.unpersist()
t = df.withColumn("area", area("path"))
run("t.collect() before any cache", lambda: len(t.collect()))
t.cache()
run("t.cache(); t.collect() on the same object", lambda: len(t.collect()))
run("t.filter(area = 'api').collect()", lambda: len(t.filter("area = 'api'").collect()))
run("t.collect() again", lambda: len(t.collect()))
run("t.filter(area = 'api').collect() again", lambda: len(t.filter("area = 'api'").collect()))
run("t.select('*').collect(), a derived DataFrame", lambda: len(t.select("*").collect()))
