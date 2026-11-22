"""Chapter 20: which Python UDFs the 4.3 transpiler lowers to native expressions, and why the others stay Python.

Classic only: the transpiler is in pyspark/sql/udf.py of classic PySpark.
Run from code/:  uv run python ch20/transpile.py
"""
import re, sys, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

spark = session("ch20-transpile", spark_sql_session_timeZone="UTC")
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")
print("spark.sql.ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))
trips = spark.read.parquet(str(TRIPS)).select("fare_amount", "tip_amount", "store_and_fwd_flag")


def band_two_statements(fare):
    if fare is None or fare <= 0:
        return "invalid"
    return "under $10" if fare < 10 else "over $10"


def band_one_statement(fare: float) -> str:
    if fare is None or fare <= 0:
        return "invalid"
    else:
        return "under $10" if fare < 10 else "over $10"


def tip_ratio(tip: float, fare: float) -> float:
    return tip / fare


def tip_plus(tip, fare):
    return tip + fare


def flag_upper(s: str) -> str:
    return s.upper()


def flag_len(s: str) -> int:
    return len(s)


def is_big(fare: float) -> bool:
    return fare is not None and fare > 20


named_lambda = lambda fare: fare * 2

CASES = [
    ("if, then a second return", band_two_statements, "string", {}),
    ("if/else, one statement", band_one_statement, "string", {}),
    ("tip / fare", tip_ratio, "double", {}),
    ("tip + fare, no type hints", tip_plus, "double", {}),
    ("s.upper()", flag_upper, "string", {}),
    ("len(s)", flag_len, "int", {}),
    ("fare is not None and fare > 20", is_big, "boolean", {}),
    ("a lambda bound to a name", named_lambda, "double", {}),
    ("a lambda inside udf()", lambda fare: fare * 2, "double", {}),
    ("array return type", lambda_arr := (lambda fare: [fare]), "array<double>", {}),
    ("useArrow=True", is_big, "boolean", {"useArrow": True}),
]
cols = {"band_two_statements": ["fare_amount"], "band_one_statement": ["fare_amount"], "tip_ratio": ["tip_amount", "fare_amount"],
        "tip_plus": ["tip_amount", "fare_amount"], "flag_upper": ["store_and_fwd_flag"], "flag_len": ["store_and_fwd_flag"]}

print(f"\n{'UDF body':<34}{'Python node':<18}reason given (warning)")
for label, fn, rtype, kw in CASES:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        u = F.udf(fn, rtype, useArrow=kw.get("useArrow", False))
    args = cols.get(fn.__name__, ["fare_amount"])
    p = plan(trips.select(u(*args)), "simple")
    node = next((n for n in ("BatchEvalPython", "ArrowEvalPython") if n in p), "none")
    msgs = [str(x.message) for x in w if "transpil" in str(x.message).lower()]
    reason = re.sub(r"<function [^>]+>", "<fn>", msgs[0]).split(": ", 1)[-1].strip("[]'")[:62] if msgs else ""
    print(f"{label:<34}{node:<18}{reason}")

# What a transpiled UDF turns into.
u = F.udf(band_one_statement, "string", useArrow=False)
q = trips.select(u("fare_amount").alias("band"))
print("\nthe Project of the transpiled band_one_statement (first 300 characters):")
print("  " + next(l.strip() for l in plan(q, "simple").splitlines() if "Project" in l)[:300])

# Python's errors are kept: len(None) raises in Python, so the transpiled len raises too.
ln = F.udf(flag_len, "int", useArrow=False)
try:
    spark.createDataFrame([("Y",), (None,)], "s string").select(ln("s")).collect()
except Exception as e:
    print("\nlen(None) in the transpiled UDF ->", (re.findall(r"\[([A-Z][A-Z_.]+)\]", str(e)) or ["?"])[0],
          "|", next((l.strip() for l in str(e).splitlines() if "Python" in l), "")[:90])

# The optimizer reads the setting too: turn it off after the UDF exists, and the UDF runs in Python.
u = F.udf(band_one_statement, "string", useArrow=False)
before = "BatchEvalPython" in plan(trips.select(u("fare_amount")), "simple")
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "false")
after = "BatchEvalPython" in plan(trips.select(u("fare_amount")), "simple")
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")
print(f"\ncreated with the setting on; BatchEvalPython in the plan: setting on {before}, setting turned off later {after}")

# Without ANSI mode, the transpiler does not run.
spark.conf.set("spark.sql.ansi.enabled", "false")
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    u = F.udf(band_one_statement, "string", useArrow=False)
print("\nwith spark.sql.ansi.enabled=false:", "BatchEvalPython" in plan(trips.select(u("fare_amount")), "simple"),
      "| warning:", str(w[0].message)[:70] if w else None)
