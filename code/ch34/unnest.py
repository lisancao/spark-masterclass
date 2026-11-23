"""Chapter 34, part 3: the UNNEST clause (new in Spark 4.3) against explode, posexplode and inline.

Run from code/:  uv run python ch34/unnest.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, plan, MESSY

spark = session("ch34-unnest", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
ORDERS = str(MESSY / "nested" / "orders.json")
ORDER_SCHEMA = """
    order_id STRING, placed_at TIMESTAMP,
    customer STRUCT<id: STRING, name: STRING, email: STRING,
                    address: STRUCT<city: STRING, country: STRING, geo: STRUCT<lat: DOUBLE, lon: DOUBLE>>>,
    lines ARRAY<STRUCT<sku: STRING, qty: INT, unit_price: DOUBLE,
                       discounts: ARRAY<STRUCT<code: STRING, pct: INT>>, attrs: MAP<STRING, STRING>>>,
    tags ARRAY<STRING>, payments ARRAY<STRUCT<method: STRING, amount: DOUBLE>>,
    status_history ARRAY<STRUCT<status: STRING, at: TIMESTAMP>>"""
spark.read.schema(ORDER_SCHEMA).json(ORDERS).createOrReplaceTempView("orders")


def section(title):
    print(f"\n== {title} ==")


def q(sql, n=6):
    df = spark.sql(sql)
    df.show(n, truncate=False)
    return df


def gen_lines(df):
    out = []
    for l in plan(df, "simple").splitlines():
        if "Generate" in l or "LateralJoin" in l or "Join" in l:
            out.append("  " + re.sub(r"#\d+L?", "", l.strip())[:92])
    return out


section("UNNEST of a literal array, WITH ORDINALITY")
q("SELECT * FROM UNNEST(array('a', 'b', 'c')) WITH ORDINALITY AS t(tag, pos)")
print("default column names:", spark.sql("SELECT * FROM UNNEST(array(1), array(2)) WITH ORDINALITY").columns,
      "| one array:", spark.sql("SELECT * FROM UNNEST(array(1))").columns)

section("several arrays in parallel: padded with NULL, not a cross product")
q("""SELECT * FROM UNNEST(array('fragile', 'Gift', 'gift'), array(10, 20)) WITH ORDINALITY AS t(tag, n, pos)""")
print("two explodes, one after the other:", spark.sql("""SELECT tag, n FROM
      (SELECT explode(array('fragile', 'Gift', 'gift')) AS tag) LATERAL VIEW explode(array(10, 20)) x AS n""").count(),
      "rows")
print("arrays_zip + inline gives the same rows:",
      spark.sql("SELECT * FROM UNNEST(array('x','y','z'), array(1,2))").collect()
      == [tuple(r) for r in spark.sql("SELECT inline(arrays_zip(array('x','y','z'), array(1,2)))").collect()])
print("a NULL array counts as empty:",
      spark.sql("SELECT * FROM UNNEST(array(1, 2), CAST(NULL AS ARRAY<INT>)) AS t(a, b)").collect())

section("correlated UNNEST with LATERAL, against LATERAL VIEW posexplode")
U = """SELECT o.order_id, t.tag, t.pos
       FROM orders o, LATERAL UNNEST(o.tags) WITH ORDINALITY AS t(tag, pos)
       ORDER BY o.order_id, t.pos"""
V = """SELECT order_id, tag, pos + 1 AS pos
       FROM orders LATERAL VIEW posexplode(tags) t AS pos, tag
       ORDER BY order_id, pos"""
u = q(U, 4)
v = spark.sql(V)
print(f"UNNEST rows: {u.count()}  LATERAL VIEW posexplode rows: {v.count()}  same rows: {u.collect() == v.collect()}")
print("posexplode position starts at:", spark.sql("SELECT posexplode(array('a'))").first()[0],
      "| WITH ORDINALITY starts at:",
      spark.sql("SELECT * FROM UNNEST(array('a')) WITH ORDINALITY AS t(x, pos)").first()[1])

section("LEFT JOIN LATERAL keeps the orders with no tags")
L = """SELECT o.order_id, t.tag
       FROM orders o LEFT JOIN LATERAL UNNEST(o.tags) AS t(tag) ON true"""
O = "SELECT order_id, tag FROM orders LATERAL VIEW OUTER explode(tags) t AS tag"
l, o = spark.sql(L), spark.sql(O)
print(f"LEFT JOIN LATERAL UNNEST rows: {l.count()}  LATERAL VIEW OUTER explode rows: {o.count()}  "
      f"same rows: {sorted(l.collect(), key=str) == sorted(o.collect(), key=str)}")
print("rows with a NULL tag:", l.where("tag IS NULL").count(), "(6 orders with no tags, 1 NULL element)")

section("an array of structs: UNNEST keeps the struct, inline makes columns")
q("""SELECT o.order_id, t.line.sku, t.line.qty, t.n
     FROM orders o, LATERAL UNNEST(o.lines) WITH ORDINALITY AS t(line, n)
     WHERE o.order_id = 'O-1001'""")
print("UNNEST columns:", spark.sql("SELECT t.* FROM orders o, LATERAL UNNEST(o.lines) AS t(line)").columns)
print("inline columns:", spark.sql("SELECT inline(lines) FROM orders").columns)
print("t.line.* columns:", spark.sql("SELECT t.line.* FROM orders o, LATERAL UNNEST(o.lines) AS t(line)").columns)

section("two arrays of one order, side by side")
q("""SELECT o.order_id, t.tag, t.pay.method, t.pay.amount, t.i
     FROM orders o, LATERAL UNNEST(o.tags, o.payments) WITH ORDINALITY AS t(tag, pay, i)
     WHERE o.order_id IN ('O-1001', 'O-1004') ORDER BY o.order_id, t.i""")

section("the plans: Generate in each case, and whole-stage code")
for label, sql in (("LATERAL UNNEST", "SELECT o.order_id, t.tag FROM orders o, LATERAL UNNEST(o.tags) AS t(tag)"),
                   ("LEFT JOIN LATERAL UNNEST",
                    "SELECT o.order_id, t.tag FROM orders o LEFT JOIN LATERAL UNNEST(o.tags) AS t(tag) ON true"),
                   ("LATERAL VIEW explode", "SELECT order_id, tag FROM orders LATERAL VIEW explode(tags) t AS tag")):
    df = spark.sql(sql)
    df.collect()
    print(label)
    for line in gen_lines(df):
        print(line)

section("the optimized plan of LEFT JOIN LATERAL UNNEST")
df = spark.sql("SELECT o.order_id, t.tag FROM orders o LEFT JOIN LATERAL UNNEST(o.tags) AS t(tag) ON true")
for line in df._jdf.queryExecution().optimizedPlan().toString().splitlines():
    print("  " + re.sub(r"#\d+L?", "", line)[:92])

section("UNNEST is a non-reserved keyword")
spark.createDataFrame([(1,)], "unnest INT").createOrReplaceTempView("unnest")
print("a column and a view named unnest:", spark.sql("SELECT unnest FROM unnest").collect())
for s in ("SELECT * FROM UNNEST(42)", "SELECT * FROM UNNEST()"):
    try:
        spark.sql(s).collect()
    except Exception as e:
        print(f"  {s:<26} -> {str(e).splitlines()[0][:80]}")
