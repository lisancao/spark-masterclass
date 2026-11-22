"""Chapter 56, new in 4.3: spark.sql.experimental.optimizer.transpilePyUDFs.

Run from code/:  uv run python ch56/transpile.py
"""
import sys, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, TRIPS  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

FLAG = "spark.sql.experimental.optimizer.transpilePyUDFs"
spark = session("ch56-transpile")
trips = spark.read.parquet(str(TRIPS))
print("defaults:", FLAG, "=", spark.conf.get(FLAG), "| spark.sql.ansi.enabled =",
      spark.conf.get("spark.sql.ansi.enabled"), "| spark.sql.execution.pythonUDF.arrow.enabled =",
      spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled"))


def band(fare: float):
    return ("invalid" if fare is None or fare <= 0
            else "under $10" if fare < 10 else "$10 to $30" if fare < 30 else "over $30")


def define(fn, rtype, **kw):
    """Build the UDF, and return it with the warnings its construction raised."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        u = F.udf(fn, rtype, **kw)
    return u, [str(x.message) for x in w]


def python_node(df):
    p = df._jdf.queryExecution().executedPlan().toString()
    return next((l.strip(" +-").split(" [")[0] for l in p.splitlines() if "EvalPython" in l), "none")


def report(title, u, msgs):
    df = trips.select(u("fare_amount").alias("band"))
    print(f"\n== {title}")
    print("   eval type:", u.evalType, "| Python node in plan:", python_node(df))
    for m in msgs:
        print("   warning:", m[:150].replace(" at 0x", " @"))


spark.conf.set(FLAG, "true")

u, msgs = define(band, "string")
report("1. flag on, plain @udf (Arrow-optimized by default in 4.3)", u, msgs)

u, msgs = define(band, "string", useArrow=False)
report("2. flag on, useArrow=False", u, msgs)
df = trips.select(u("fare_amount").alias("band"))
proj = next(l.strip() for l in df._jdf.queryExecution().executedPlan().toString().splitlines()
            if "Project" in l)
print("   the plan's Project, start:", proj[:150])
print("   ...", len(proj), "characters in all")


def band_if(fare):
    if fare is None or fare <= 0:
        return "invalid"
    return "under $10" if fare < 10 else "$10 to $30" if fare < 30 else "over $30"


def tip_pct(tip: float, fare: float):
    return None if fare is None or fare <= 0 else 100 * tip / fare


def is_png(b):
    return b is not None and bytes(b[:8]) == b"\x89PNG\r\n\x1a\n"


print("\n== 3. what the transpiler refuses (useArrow=False)")
for fn, rt in ((band_if, "string"), (tip_pct, "double"), (is_png, "boolean")):
    u, msgs = define(fn, rt, useArrow=False)
    reason = msgs[0].split(": [", 1)[-1].split("', '")[0].strip("'[]") if msgs else "transpiled"
    print(f"   {fn.__name__:8} -> {reason[:120]}")

print("\n== 4. the flag is read twice: when the UDF is built, and when the query is optimized")
spark.conf.set(FLAG, "false")
early, _ = define(band, "string", useArrow=False)
spark.conf.set(FLAG, "true")
print("   built with the flag off, optimized with it on: Python node =",
      python_node(trips.select(early("fare_amount"))))
late, _ = define(band, "string", useArrow=False)
spark.conf.set(FLAG, "false")
print("   built with the flag on, optimized with it off: Python node =",
      python_node(trips.select(late("fare_amount"))))
spark.conf.set(FLAG, "true")
print("   built with the flag on, optimized with it on:  Python node =",
      python_node(trips.select(late("fare_amount"))))

print("\n== 5. ANSI mode off")
spark.conf.set("spark.sql.ansi.enabled", "false")
u, msgs = define(band, "string", useArrow=False)
print("   warning:", msgs[0][:120] if msgs else None)
print("   Python node:", python_node(trips.select(u("fare_amount"))))
spark.conf.set("spark.sql.ansi.enabled", "true")

print("\n== 6. same answer, Python and transpiled")
spark.conf.set(FLAG, "false")
py, _ = define(band, "string", useArrow=False)
spark.conf.set(FLAG, "true")
tr, _ = define(band, "string", useArrow=False)
a = trips.groupBy(py("fare_amount").alias("band")).count().orderBy("band").collect()
b = trips.groupBy(tr("fare_amount").alias("band")).count().orderBy("band").collect()
print("   Python:    ", [(r.band, r["count"]) for r in a])
print("   transpiled:", [(r.band, r["count"]) for r in b])
print("   equal:", a == b)
spark.stop()
