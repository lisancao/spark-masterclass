"""Chapter 56, Step 13 (new in 4.3): Python UDF transpilation, what it rewrites and what it leaves alone.

Run from code/:  uv run python ch56/transpile.py
"""
import math, re, sys, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan
from logs import access_parquet

spark = session("ch56-transpile")
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")   # read when a UDF is created
print("spark.sql.ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))
df = access_parquet(spark)

def python_node(q):
    m = re.search(r"(ArrowEvalPython|BatchEvalPython)", plan(q, "simple"))
    return m.group(1) if m else "none"

def make(f, ret, **kw):
    """Create the UDF and keep the transpiler's warning, if it gave one."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        u = F.udf(f, ret, **kw)
    why = [str(x.message) for x in w if "transpile" in str(x.message)]
    reason = re.sub(r"^Unable to transpile UDF <.*?>: \[.", "", why[0])[:200] if why else ""
    return u, reason

def is_error(status):
    return status >= 400

print("\n1. The default @udf is Arrow-optimized (eval type 101) and is not transpiled:")
u, _ = make(is_error, "boolean")
print(f"   evalType {u.evalType}, Python node in plan: {python_node(df.filter(u('status')))}")

print("\n2. With useArrow=False (eval type 100):")
u, _ = make(is_error, "boolean", useArrow=False)
q = df.filter(u("status"))
print(f"   evalType {u.evalType}, Python node in plan: {python_node(q)}")
q.explain("extended")

def is_error_guarded(status):
    return status is not None and status >= 400

shout = lambda s: s + "!"
def band(fare):
    return "invalid" if fare <= 0 else "short" if fare < 10 else "long"
def band_statements(fare):
    if fare <= 0:
        return "invalid"
    return "short" if fare < 10 else "long"
def first_segment(path):
    return path.split("/")[1]
def root(x):
    return math.sqrt(x)
def length(s):
    return len(s)

cases = [
    ("named lambda    shout = lambda s: s + '!'", shout, "string", "path"),
    ("inline lambda   udf(lambda s: s + '!')", lambda s: s + "!", "string", "path"),
    ("one expression  band(fare)", band, "string", "status"),
    ("two statements  band_statements(fare)", band_statements, "string", "status"),
    ("len(s)", length, "int", "path"),
    ("str.split", first_segment, "string", "path"),
    ("math.sqrt", root, "double", "status"),
]
u, _ = make(is_error_guarded, "boolean", useArrow=False)
q = df.filter(u("status"))
print("\n   with an `is not None` guard in the function:")
for l in plan(q, "simple").splitlines()[1:4]:
    print("   " + re.sub(r"Batched: true, DataFilters: .*?(PushedFilters: \[.*?\]).*", r"... \1", l))
print(f"   rows with status >= 400: {q.count()}\n")

print("3. What the transpiler lowers:\n")
for label, f, ret, col in cases:
    u, reason = make(f, ret, useArrow=False)
    q = df.select(u(col).alias("out"))
    node = python_node(q)
    proj = [l for l in plan(q, "simple").splitlines() if "Project" in l][0].strip()
    print(f"  {label:<44} {'stays Python' if node != 'none' else 'native':<13}")
    print(f"      {proj[:84] if node == 'none' else reason[:84]}")

print("\n4. ANSI semantics, not Python semantics:")
big = spark.range(1).selectExpr("4611686018427387904L AS x")    # 2^62
dbl = lambda x: x * 2
double_t, _ = make(dbl, "long", useArrow=False)
def twice(x):
    return x * 2
double_py = F.udf(twice, "long", useArrow=False).asNondeterministic()   # never transpiled
for name, u in [("interpreted", double_py), ("transpiled", double_t)]:
    try:
        print(f"  {name:<12} {big.select(u('x')).collect()[0][0]}")
    except Exception as e:
        print(f"  {name:<12} {type(e).__name__}: {str(e).splitlines()[0][:70]}")
