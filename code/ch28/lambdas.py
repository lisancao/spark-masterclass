"""Chapter 28, part 2: higher-order functions. How a Python lambda becomes a Catalyst expression,
the errors that show it, aggregate and its zero, three-valued exists/forall, comparators, and codegen.

Run from code/:  uv run python ch28/lambdas.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY

spark = session("ch28-lambdas", spark_sql_session_timeZone="UTC")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    if m:
        return m.group(1)
    return f"{type(e).__name__}: {str(e).splitlines()[0][:70]}"


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label:46} -> {r}")
    except Exception as e:
        print(f"  {label:46} -> {condition(e)}")


df = spark.sql("SELECT array(1, 2, 3, NULL) AS xs, array('a', 'B', ' c ') AS ss")

section("the lambda runs once, on the driver, with a placeholder Column")
calls = []


def double(x):
    calls.append(repr(x))
    return x * 2


col = F.transform("xs", double)
print("calls after building the Column:", len(calls))
print("argument:", calls[0])
rows = df.select(col.alias("doubled")).collect()
df.select(col.alias("doubled")).collect()
print("calls after two actions:", len(calls), " result:", rows[0].doubled)
print("the expression:", str(col)[:88])

section("the analyzed plan holds a lambda function, not Python")
an = plan(df.select(F.transform("xs", lambda x: x * 2).alias("d")), "extended")
print(next(l for l in an.split("== Analyzed Logical Plan ==")[1].splitlines() if "lambdafunction" in l).strip()[:90])
print(spark.sql("SELECT transform(xs, x -> x * 2) AS d FROM VALUES (array(1, 2)) AS t(xs)").first().d)

section("errors that show the lambda is traced, not run")
attempt("Python if on a Column", lambda: df.select(F.transform("xs", lambda x: 1 if x > 1 else 0)).collect())
attempt("lambda returns a Python int", lambda: df.select(F.transform("xs", lambda x: 1)).collect())
attempt("call the Column as a function", lambda: df.select(F.transform("xs", lambda x: x(1))).collect())
attempt("lambda with *args", lambda: df.select(F.transform("xs", lambda *a: a[0])).collect())
attempt("SQL lambda with the wrong arity", lambda: spark.sql(
    "SELECT filter(array(1, 2), (x, i, j) -> x > 1)").collect())


@F.udf("int")
def py_double(x):
    return None if x is None else x * 2


attempt("a Python UDF inside the lambda", lambda: df.select(F.transform("xs", lambda x: py_double(x))).collect())
show(df.select(F.transform("xs", lambda x: F.when(x > 1, 1).otherwise(0)).alias("when, not if")))

section("the index argument")
show(df.select(F.transform("ss", lambda s, i: F.concat(i.cast("string"), F.lit(":"), F.trim(s))).alias("i:s"),
               F.filter("xs", lambda x, i: i % 2 == 0).alias("even positions")))

section("aggregate: the type of the zero is the type of the result")
attempt("aggregate(xs, lit(0), acc + x)", lambda: df.select(F.aggregate("xs", F.lit(0), lambda a, x: a + x)).first()[0])
attempt("aggregate(xs, lit(0), acc + coalesce(x, 0))", lambda: df.select(
    F.aggregate("xs", F.lit(0), lambda a, x: a + F.coalesce(x, F.lit(0)))).first()[0])
prices = spark.sql("SELECT array(1.5D, 2.25D) AS p")
attempt("aggregate(p, lit(0), acc + p)  (int zero)", lambda: prices.select(
    F.aggregate("p", F.lit(0), lambda a, x: a + x)).first()[0])
attempt("aggregate(p, lit(0.0), acc + p)", lambda: prices.select(
    F.aggregate("p", F.lit(0.0), lambda a, x: a + x)).first()[0])
attempt("SQL: aggregate(p, 0.0, ...)  (0.0 is DECIMAL)", lambda: spark.sql(
    "SELECT aggregate(array(1.5D, 2.25D), 0.0, (acc, x) -> acc + x)").first()[0])
print("  SQL typeof(0.0):", spark.sql("SELECT typeof(0.0)").first()[0], " typeof(0D):", spark.sql("SELECT typeof(0D)").first()[0])
attempt("SQL: aggregate(p, 0D, ...)", lambda: spark.sql(
    "SELECT aggregate(array(1.5D, 2.25D), 0D, (acc, x) -> acc + x)").first()[0])
attempt("aggregate with a finish function (mean)", lambda: spark.sql(
    "SELECT aggregate(array(1, 2, 3, 4), named_struct('s', 0L, 'n', 0L), "
    "(a, x) -> named_struct('s', a.s + x, 'n', a.n + 1), a -> a.s / a.n)").first()[0])

section("exists and forall use three-valued logic")
for arr in ("array(1, 2)", "array(1, NULL)", "array(5, NULL)", "array()"):
    r = spark.sql(f"SELECT exists({arr}, x -> x > 3) AS e, forall({arr}, x -> x > 0) AS f, "
                  f"array_contains({arr}, 5) AS c").first()
    print(f"  {arr:16} exists(x > 3)={str(r.e):5} forall(x > 0)={str(r.f):5} array_contains(5)={r.c}")
spark.conf.set("spark.sql.legacy.followThreeValuedLogicInArrayExists", "false")
print("  legacy.followThreeValuedLogicInArrayExists=false, exists(array(1, NULL), x -> x > 3):",
      spark.sql("SELECT exists(array(1, NULL), x -> x > 3)").first()[0])
spark.conf.unset("spark.sql.legacy.followThreeValuedLogicInArrayExists")

section("array_sort with a comparator, and the struct trick")
h = spark.sql("""SELECT array(named_struct('status', 'shipped', 'at', TIMESTAMP '2026-10-07 10:00:00'),
                              named_struct('status', 'placed',  'at', TIMESTAMP '2026-10-05 08:00:00'),
                              named_struct('status', 'paid',    'at', TIMESTAMP '2026-10-05 09:00:00')) AS h""")
by_time = F.array_sort("h", lambda a, b: F.when(a.at < b.at, -1).when(a.at > b.at, 1).otherwise(0))
show(h.select(by_time.status.alias("by comparator"),
              F.element_at(by_time, -1).status.alias("latest"),
              F.array_max(F.transform("h", lambda e: F.struct(e.at, e.status))).status.alias("array_max of (at, status)")))

section("zip_with and arrays_zip")
z = spark.sql("SELECT array(1, 2, 3) AS a, array(10, 20) AS b")
show(z.select(F.zip_with("a", "b", lambda x, y: x + y).alias("zip_with(+)"),
              F.arrays_zip("a", "b").alias("arrays_zip")))

section("a comparator must return INT")
attempt("array_sort(xs, (a, b) -> b - a) on BIGINT", lambda: spark.range(1).select(
    F.array_sort(F.array(F.col("id"), F.lit(2)), lambda a, b: b - a)).collect())
attempt("the same with .cast('int')", lambda: spark.range(1).select(
    F.array_sort(F.array(F.col("id"), F.lit(2)), lambda a, b: (b - a).cast("int"))).first()[0])

section("new in 4.3: Python UDF transpilation, and a UDF inside a lambda")
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")


@F.udf("int", useArrow=False)   # transpilation applies only to the plain (pickled) Python UDF
def py_triple(x):
    return x * 3


print("  plan of py_triple(x):", next(l.strip() for l in plan(spark.range(3).select(py_triple(F.col("id").cast("int")).alias("t")), "simple")
                                       .splitlines() if "Project" in l or "EvalPython" in l)[:88])
attempt("transform(xs, x -> py_triple(x)), transpiled",
        lambda: df.select(F.transform("xs", lambda x: py_triple(x))).first()[0])
spark.conf.unset("spark.sql.experimental.optimizer.transpilePyUDFs")

section("new in 4.3: which higher-order functions take part in whole-stage codegen")
nums = spark.range(3).select(F.array(F.col("id"), F.lit(5), F.lit(2)).alias("xs"),
                             F.create_map(F.lit("a"), F.col("id")).alias("m"))
for name, e in [
    ("transform", F.transform("xs", lambda x: x * 2)),
    ("filter", F.filter("xs", lambda x: x > 1)),
    ("exists", F.exists("xs", lambda x: x > 4)),
    ("forall", F.forall("xs", lambda x: x > 0)),
    ("aggregate", F.aggregate("xs", F.lit(0).cast("bigint"), lambda a, x: a + x)),
    ("array_sort with a comparator", F.array_sort("xs", lambda a, b: (b - a).cast("int"))),
    ("zip_with", F.zip_with("xs", "xs", lambda a, b: a + b)),
    ("transform_values", F.transform_values("m", lambda k, v: v + 1)),
    ("map_filter", F.map_filter("m", lambda k, v: v > 0)),
]:
    top = plan(nums.select(e.alias("r")), "simple").splitlines()[1]
    print(f"  {name:30} {'*(n) Project: in codegen' if top.startswith('*(') else 'Project: interpreted'}")
orders = spark.read.json(str(MESSY / "nested" / "orders.json"))
q = orders.select("order_id", F.aggregate("lines", F.lit(0.0),
                                          lambda acc, l: acc + l.qty * l.unit_price).alias("gross"))
print(plan(q, "simple").splitlines()[1][:90])
cg = plan(q, "codegen")
print("generated Java loop:", next(l.strip() for l in cg.splitlines() if re.search(r"for \(int (\w+) = 0; \1 < \w*numElements", l)))

section("nested schema pruning: what each query reads from Parquet")
pq = "/tmp/ch28_orders_parquet"
spark.read.schema(spark.read.json(str(MESSY / "nested" / "orders.json")).schema).json(
    str(MESSY / "nested" / "orders.json")).write.mode("overwrite").parquet(pq)
po = spark.read.parquet(pq)
for label, q in [
    ("select lines.sku", po.select("lines.sku")),
    ("transform(lines, l -> l.sku)", po.select(F.transform("lines", lambda l: l.sku))),
    ("explode(lines), then line.sku", po.select(F.explode("lines").alias("l")).select("l.sku")),
    ("select customer.address.city", po.select("customer.address.city")),
]:
    rs = next(l for l in plan(q, "formatted").splitlines() if l.strip().startswith("ReadSchema"))
    print(f"  {label:32} {rs.split(': ', 1)[1][:100]}")
