"""Chapter 5: a DataFrame is a schema and a plan. What withColumn, withColumns and select add to it.

Classic only for the plan internals (it reads queryExecution through _jdf).
Run from code/:  uv run python ch05/plans.py
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS

spark = session("ch05-plans")
jobs_before = len(ui_api("jobs"))

# 1. Building DataFrames runs nothing (the Parquet footer gives the schema).
trips = spark.read.parquet(str(TRIPS))
fares = trips.select("fare_amount", "tip_amount", "tolls_amount")
print("== 1. a DataFrame is a schema and a plan")
print(fares.schema.simpleString())
new_jobs = ui_api("jobs")[: len(ui_api("jobs")) - jobs_before]
print("jobs run by read + select:", [(j["name"], j["numTasks"]) for j in new_jobs])
print("same object after withColumn?", fares is fares.withColumn("x", F.lit(1)))

# 2. Three ways to name a column, and what each one builds.
print("\n== 2. column references")
print(repr(F.col("fare_amount")), repr(fares["fare_amount"]), repr(F.expr("fare_amount * 2")))
print("F.col('no_such_column') ->", repr(F.col("no_such_column")))       # a name, not checked yet
try:
    fares["no_such_column"]                                               # Classic checks it now
except Exception as e:
    print("fares['no_such_column'] ->", str(e).splitlines()[0][:70])

# 3. A withColumn chain: one Project per call in the analyzed plan, one after optimization.
chain = (fares
         .withColumn("tip_pct", F.col("tip_amount") / F.col("fare_amount"))
         .withColumn("has_toll", F.col("tolls_amount") > 0)
         .withColumn("fare_band", F.when(F.col("fare_amount") < 10, "short").otherwise("long")))
qe = chain._jdf.queryExecution()
print("\n== 3. withColumn x3: parsed (unresolved) plan")
print(qe.logical().toString())
print("== analyzed plan")
print(qe.analyzed().toString())
print("== optimized plan")
print(qe.optimizedPlan().toString())

one = fares.withColumns({
    "tip_pct": F.col("tip_amount") / F.col("fare_amount"),
    "has_toll": F.col("tolls_amount") > 0,
    "fare_band": F.when(F.col("fare_amount") < 10, "short").otherwise("long")})
print("== withColumns: analyzed plan")
print(one._jdf.queryExecution().analyzed().toString())
print("same optimized plan:", qe.optimizedPlan().sameResult(one._jdf.queryExecution().optimizedPlan()))


# 4. The cost of a long chain is paid in the analyzer, on the driver, before any task runs.
def build_loop(n):
    df = fares
    for i in range(n):
        df = df.withColumn(f"c{i}", F.col("fare_amount") + i)
    return df


def build_once(n):
    return fares.withColumns({f"c{i}": F.col("fare_amount") + i for i in range(n)})


def analyze_ms(builder, n, repeats=7):
    times = []
    for _ in range(repeats + 1):                    # the first run is a warmup
        t0 = time.perf_counter()
        df = builder(n)
        df._jdf.queryExecution().optimizedPlan()    # forces analysis and optimization
        times.append((time.perf_counter() - t0) * 1000)
    times = sorted(times[1:])
    return times[len(times) // 2]


for _ in range(3):                                  # warm the analyzer's code paths up first
    build_loop(200)._jdf.queryExecution().optimizedPlan()
    build_once(200)._jdf.queryExecution().optimizedPlan()
print("\n== 4. build + analyze + optimize, median of 7 after a warmup (ms)")
print(f"{'columns':>8} {'withColumn loop':>16} {'withColumns':>12} {'Projects (loop)':>16}")
for n in (25, 50, 100, 200, 400):
    loop_ms = analyze_ms(build_loop, n)
    once_ms = analyze_ms(build_once, n)
    projects = build_loop(n)._jdf.queryExecution().analyzed().toString().count("Project [")
    print(f"{n:>8} {loop_ms:>16.1f} {once_ms:>12.1f} {projects:>16}")

# 5. Nullability: the analyzer tracks it, and the optimizer uses it.
print("\n== 5. nullability in the plan")
ids = spark.range(5)
print("range:", ids.schema.simpleString(), ids.schema["id"].nullable)
print(plan(ids.where("id IS NULL"), "simple").strip())
print("--- the same filter on a nullable column")
print(plan(fares.where("fare_amount IS NULL"), "simple").strip())
derived = ids.select((F.col("id") + 1).alias("a"), (F.col("id") / 0).alias("b"),
                     F.when(F.col("id") > 2, 1).alias("c"), F.coalesce(F.lit(None), F.lit(0)).alias("d"))
for f in derived.schema.fields:
    print(f"  {f.name}: {f.dataType.simpleString()} nullable={f.nullable}")

# 6. New in 4.3: DataFrame.zip pairs the columns of two DataFrames row by row.
print("\n== 6. DataFrame.zip (4.3)")
base = trips.select("PULocationID", "fare_amount", "tip_amount").limit(3)
left = base.select("PULocationID", "fare_amount")
right = base.select((F.col("tip_amount") / F.col("fare_amount")).alias("tip_pct"))
zipped = left.zip(right)
zipped.show()
print(plan(zipped, "simple").strip())
for name, other in (("a filtered side", base.where("fare_amount > 20").select("tip_amount")),
                    ("a different source", spark.range(3).toDF("n"))):
    try:
        left.zip(other).collect()
    except Exception as e:
        print(f"zip with {name}:", str(e).splitlines()[0][:88])
