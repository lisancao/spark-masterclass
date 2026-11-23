"""Chapter 43: how often Spark calls a Python UDF, what a Python filter does to pushdown, and the profiler.

Classic (accumulators, the UDF profiler). Run from code/:  uv run python ch43/calls.py
"""
import contextlib, io, sys, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

warnings.filterwarnings("ignore")
spark = session("ch43-calls", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
small = spark.read.parquet(str(TRIPS)).limit(1000).cache()
small.count()

calls = sc.accumulator(0)


def counted(fare):
    calls.add(1)                  # a side effect on the worker, summed on the driver
    return fare is not None and fare > 20


is_big = F.udf(counted, "boolean")


def count_calls(label, run):
    calls.value = 0
    run()
    print(f"  {label:<52}{calls.value:>6}")


print("calls to a counting @udf on 1,000 rows:")
count_calls("select(udf(fare)).collect()", lambda: small.select(is_big("fare_amount")).collect())
count_calls("withColumn(udf), then filter on that column", lambda: small.withColumn("big", is_big("fare_amount"))
            .where("big").select("fare_amount", "big").collect())
count_calls("same, with asNondeterministic()", lambda: small.withColumn(
    "big", F.udf(counted, "boolean").asNondeterministic()("fare_amount")).where("big").select("fare_amount", "big").collect())
df = small.withColumn("big", is_big("fare_amount"))
count_calls("two actions on one DataFrame", lambda: (df.collect(), df.collect()))
cached = small.withColumn("big", is_big("fare_amount")).cache()
count_calls("cache(), then two actions", lambda: (cached.collect(), cached.collect()))
cached.unpersist()
count_calls("count() on a DataFrame with a UDF column", lambda: small.withColumn("big", is_big("fare_amount")).count())

# Where the double call comes from: the optimized plan.
q = small.withColumn("big", is_big("fare_amount")).where("big").select("fare_amount", "big")
print("\nthe physical plan of the filter case:")
for l in plan(q, "simple").splitlines():
    if "EvalPython" in l or "Filter" in l or "Project" in l:
        print("  " + l.strip()[:110])

# A Python filter cannot go into the Parquet scan. A built-in filter can.
trips = spark.read.parquet(str(TRIPS))
pushed = lambda d: next((l.split("PushedFilters: ")[1].split("]")[0] + "]"
                         for l in plan(d, "formatted").splitlines() if "PushedFilters" in l), "(none)")
print("\nPushedFilters of the scan:")
print("  built-in  fare_amount > 20:  ", pushed(trips.where(F.col("fare_amount") > 20)))
print("  Python    is_big(fare_amount):", pushed(trips.where(is_big("fare_amount"))))
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")


def over_20(fare: float) -> bool:
    return fare is not None and fare > 20


over_20_t = F.udf(over_20, "boolean", useArrow=False)
print("  transpiled over_20(fare_amount):", pushed(trips.where(over_20_t("fare_amount"))))
tq = trips.where(over_20_t("fare_amount"))
print("  the transpiled filter:", next(l.strip() for l in plan(tq, "simple").splitlines() if "Filter" in l)[:150])
print("  a Python node in that plan:", any(n in plan(tq, "simple") for n in ("EvalPython",)))

# The UDF profiler (Spark 4.0+): cProfile output for each UDF, kept on the driver.
spark.conf.set("spark.sql.pyspark.udf.profiler", "perf")
import ipaddress


@F.udf("boolean")
def private_ip(n):
    return ipaddress.ip_address(f"10.{n % 256}.{n // 256 % 256}.1").is_private


spark.range(200_000).select(private_ip("id")).write.format("noop").mode("overwrite").save()
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    spark.profile.show(type="perf")
lines = buf.getvalue().splitlines()
print("\nspark.profile.show(type='perf'), the first lines:")
for l in lines[:16]:
    print("  " + l[:100])
spark.profile.clear()
spark.conf.unset("spark.sql.pyspark.udf.profiler")
