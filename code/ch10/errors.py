"""Chapter 10: catch Spark errors by condition and SQLSTATE, on Classic and on Connect.

Run from code/:
  uv run python ch10/errors.py
  SPARK_REMOTE=sc://localhost:15117 uv run python ch10/errors.py    (server: ch90/connect_server.sh start)
"""
import glob
import json
import os
import sys
import zipfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyspark
from pyspark.errors import (AnalysisException, ArithmeticException, NumberFormatException, ParseException,
                            PySparkException, PySparkTypeError)
from pyspark.sql import functions as F
from common.book import session, TRIPS

spark = session("ch10-errors", spark_ui_showConsoleProgress="false")
mode = "Connect" if os.environ.get("SPARK_REMOTE") else "Classic"
print("session:", mode)
trips = spark.read.parquet(str(TRIPS))

print("\n== 1. one failure of each kind, and what the exception carries")
cases = [
    ("unknown column", lambda: trips.select("revenue").collect()),
    ("SQL syntax", lambda: spark.sql("SELEC 1").collect()),
    ("divide by zero", lambda: spark.sql("SELECT 10 / 0").collect()),
    ("bad cast", lambda: spark.sql("SELECT CAST('12a' AS INT)").collect()),
    ("table not found", lambda: spark.table("no_such_table").collect()),
    ("Python-side check", lambda: trips.withColumn("x", 5)),
    ("no condition", lambda: trips.sample(fraction=2.0).count()),
]
caught = {}
for label, fn in cases:
    try:
        fn()
    except PySparkException as e:
        caught[label] = e
        print(f"{label}:")
        print(f"  class:     {type(e).__module__}.{type(e).__name__}")
        print(f"  condition: {e.getCondition()}   SQLSTATE: {e.getSqlState()}")
        print(f"  params:    {e.getMessageParameters()}")
        if not e.getCondition():
            print(f"  message:   {str(e).splitlines()[0][:80]}")
        for c in e.getQueryContext()[:1]:
            print(f"  context:   {c.contextType().name} fragment={c.fragment()!r}")

print("\n== 2. the Python classes are the same on Classic and Connect")
for label, cls in (("unknown column", AnalysisException), ("SQL syntax", ParseException),
                   ("divide by zero", ArithmeticException), ("bad cast", NumberFormatException),
                   ("Python-side check", PySparkTypeError)):
    e = caught[label]
    print(f"  isinstance({label!r}, {cls.__name__}): {isinstance(e, cls)}   "
          f"TypeError: {isinstance(e, TypeError)}")

print("\n== 3. a handler that decides by condition first, then by SQLSTATE class")
SQLSTATE_CLASS = {"22": "data exception", "42": "syntax error or access rule violation",
                  "0A": "feature not supported", "XX": "internal error"}


def handle(e: PySparkException) -> str:
    cond, state = e.getCondition() or "", e.getSqlState() or ""
    if cond == "DIVIDE_BY_ZERO":
        return "retry with try_divide"
    if cond.startswith("UNRESOLVED_COLUMN"):
        return "fix the code: column " + e.getMessageParameters().get("objectName", "?")
    if cond == "TABLE_OR_VIEW_NOT_FOUND":
        return "create the table, then run again"
    if not cond:
        return "no condition: log the message and stop the job"
    return f"SQLSTATE class {state[:2]} ({SQLSTATE_CLASS.get(state[:2], 'other')}): stop the job"


for label, e in caught.items():
    print(f"  {label:18} -> {handle(e)}")

print("\n== 4. the error-conditions catalog")
home = Path(pyspark.__file__).parent
jar = glob.glob(str(home / "jars" / "spark-common-utils_*.jar"))[0]
with zipfile.ZipFile(jar) as z:
    conditions = json.loads(z.read("error/error-conditions.json"))
    states = json.loads(z.read("error/error-states.json"))
py_conditions = json.loads((home / "errors" / "error-conditions.json").read_text())
subs = sum(len(v.get("subClass", {})) for v in conditions.values())
print(f"JVM:    {Path(jar).name}!error/error-conditions.json: {len(conditions)} conditions, {subs} subconditions")
print(f"        error-states.json: {len(states)} SQLSTATE codes")
print(f"Python: pyspark/errors/error-conditions.json: {len(py_conditions)} conditions (client-side errors)")
for name in ("DIVIDE_BY_ZERO", "CAST_INVALID_INPUT"):
    c = conditions[name]
    print(f"{name}: sqlState={c['sqlState']}  message={' '.join(c['message'])[:88]}")
print("22012 in error-states.json:", {k: states["22012"][k] for k in ("description", "standard") if k in states["22012"]})
for label, e in caught.items():
    st = e.getSqlState()
    if st:
        print(f"  {st}  class {st[:2]}  {states[st]['description']}")
print("UNRESOLVED_COLUMN subconditions:", list(conditions["UNRESOLVED_COLUMN"]["subClass"]))
print("Python-side PySparkTypeError above:", caught["Python-side check"].getCondition(), "->",
      " ".join(py_conditions[caught["Python-side check"].getCondition()]["message"])[:70])

print("\n== 5. where the error appears: on the line that defines the DataFrame, or at the action")
step = "select('revenue')"
try:
    bad = trips.select("revenue")
    print("  select('revenue') returned a DataFrame")
    step = "collect()"
    bad.collect()
except AnalysisException as e:
    print("  raised by:", step, "->", e.getCondition())
