"""Chapter 28, part 1: structs, arrays and maps. Access, NULLs, struct edits, the array toolbox,
ANSI indexing, generators and maps.

Run from code/:  uv run python ch28/nested.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from common.book import session, show, plan, MESSY

spark = session("ch28-nested", spark_sql_session_timeZone="UTC")
ORDERS = str(MESSY / "nested" / "orders.json")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else type(e).__name__


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label:44} -> {r}")
    except Exception as e:
        print(f"  {label:44} -> {condition(e)}")


ORDER_SCHEMA = """
    order_id STRING,
    placed_at TIMESTAMP,
    customer STRUCT<id: STRING, name: STRING, email: STRING,
                    address: STRUCT<city: STRING, country: STRING,
                                    geo: STRUCT<lat: DOUBLE, lon: DOUBLE>>>,
    lines ARRAY<STRUCT<sku: STRING, qty: INT, unit_price: DOUBLE,
                       discounts: ARRAY<STRUCT<code: STRING, pct: INT>>,
                       attrs: MAP<STRING, STRING>>>,
    tags ARRAY<STRING>,
    payments ARRAY<STRUCT<method: STRING, amount: DOUBLE>>,
    status_history ARRAY<STRUCT<status: STRING, at: TIMESTAMP>>
"""

section("inference never gives a map")
print("inferred attrs:", spark.read.json(ORDERS).schema["lines"].dataType.elementType["attrs"].dataType.simpleString()[:90])
orders = spark.read.schema(ORDER_SCHEMA).json(ORDERS).cache()
print("declared attrs:", orders.schema["lines"].dataType.elementType["attrs"].dataType.simpleString())
print("orders:", orders.count())

section("struct access and the names of the output columns")
df = orders.select("customer.name", F.col("customer.address.city"), F.col("customer")["id"],
                   F.col("customer").getField("email"), F.col("customer.address.geo.lat"))
print(df.columns)
print("customer.* ->", orders.select("customer.*").columns)
attempt("select('customer.phone')", lambda: orders.select("customer.phone").columns)
odd = spark.createDataFrame([(1, 2)], "`a.b` INT, c INT")
attempt("select('a.b') on a column named a.b", lambda: odd.select("a.b").columns)
attempt("select('`a.b`')", lambda: odd.select("`a.b`").first()[0])

section("a NULL struct is not a struct of NULLs")
geo = orders.select("order_id", "customer.address.geo",
                    F.col("customer.address.geo").isNull().alias("geo IS NULL"),
                    F.col("customer.address.geo.lat").isNull().alias("geo.lat IS NULL"))
show(geo.where("order_id IN ('O-1001', 'O-1011')").orderBy("order_id"))
show(spark.sql("""SELECT CAST(NULL AS STRUCT<a: INT>) AS null_struct,
                         named_struct('a', CAST(NULL AS INT)) AS struct_of_null,
                         named_struct('a', CAST(NULL AS INT)) IS NULL AS is_null"""))

section("build structs: struct, named_struct, and the order of comparison")
show(orders.select(F.struct("order_id", F.size("lines"), F.lit(1)).alias("s")).limit(1))
print(orders.select(F.struct("order_id", F.size("lines"), F.lit(1)).alias("s")).schema["s"].dataType.simpleString())
show(spark.sql("""SELECT named_struct('a', 1, 'b', 9) < named_struct('a', 2, 'b', 0) AS first_field_decides"""))

section("edit a struct in place: withField and dropFields")
red = orders.withColumn("customer", F.col("customer").dropFields("email")
                        .withField("address.country", F.upper("customer.address.country")))
print(red.schema["customer"].dataType.simpleString())
show(red.select("order_id", "customer.address.country").orderBy("order_id"), 3)
nulls = spark.sql("SELECT CAST(NULL AS STRUCT<a: INT, b: INT>) AS s")
show(nulls.select(F.col("s").withField("c", F.lit(1)).alias("withField on a NULL struct")))
attempt("dropFields of every field", lambda: nulls.select(F.col("s").dropFields("a", "b")).collect())
attempt("withField on an array of structs", lambda: orders.select(F.col("lines").withField("x", F.lit(1))).collect())
show(orders.select(F.transform("lines", lambda l: l.dropFields("attrs", "discounts")).alias("lines")).limit(1))

section("a dot path goes through one array level")
show(orders.select("order_id", F.col("lines.sku").alias("lines.sku")).orderBy("order_id"), 3)
attempt("lines.discounts.code (two levels)",
        lambda: orders.select(F.col("lines.discounts.code")).schema[0].dataType.simpleString())
codes = F.flatten(F.transform("lines", lambda l: l.discounts.code))
show(orders.select("order_id", codes.alias("codes")).where("size(codes) > 0").orderBy("order_id").limit(3))

section("the array toolbox, and what each function does with NULL")
t = spark.sql("SELECT array('b', NULL, 'a', 'b') AS a, array(3, 1, NULL, 2) AS n")
for label, expr in [
    ("array_distinct(a)", F.array_distinct("a")),
    ("array_compact(a)", F.array_compact("a")),
    ("sort_array(n)", F.sort_array("n")),
    ("sort_array(n, asc=False)", F.sort_array("n", asc=False)),
    ("array_sort(n)", F.array_sort("n")),
    ("array_join(a, '-')", F.array_join("a", "-")),
    ("array_join(a, '-', '?')", F.array_join("a", "-", "?")),
    ("array_position(a, 'a')", F.array_position("a", "a")),
    ("array_remove(a, 'b')", F.array_remove("a", "b")),
    ("array_contains(a, 'a')", F.array_contains("a", "a")),
    ("array_contains(a, 'z')", F.array_contains("a", "z")),
    ("array_insert(n, 1, 0)", F.array_insert("n", 1, F.lit(0))),
    ("array_append(n, 9)", F.array_append("n", F.lit(9))),
    ("slice(n, 2, 2)", F.slice("n", 2, 2)),
    ("array_union(n, array(5))", F.array_union("n", F.array(F.lit(5)))),
    ("array_except(n, array(1))", F.array_except("n", F.array(F.lit(1)))),
    ("arrays_zip(a, n)[0]", F.arrays_zip("a", "n")[0]),
    ("sequence(1, 7, 3)", F.sequence(F.lit(1), F.lit(7), F.lit(3))),
]:
    v = t.select(expr).first()[0]
    print(f"  {label:28} {v}")
print("  size(NULL) under ANSI:", spark.sql("SELECT size(CAST(NULL AS ARRAY<INT>))").first()[0])
spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
print("  size(NULL), legacy.sizeOfNull=true, ANSI on: ", spark.sql("SELECT size(CAST(NULL AS ARRAY<INT>))").first()[0])
spark.conf.set("spark.sql.ansi.enabled", "false")
print("  size(NULL), legacy.sizeOfNull=true, ANSI off:", spark.sql("SELECT size(CAST(NULL AS ARRAY<INT>))").first()[0])
spark.conf.unset("spark.sql.legacy.sizeOfNull")
spark.conf.unset("spark.sql.ansi.enabled")

section("ANSI indexing")
for label, sql in [
    ("a[10]", "SELECT array(1, 2, 3)[10]"),
    ("element_at(a, 10)", "SELECT element_at(array(1, 2, 3), 10)"),
    ("element_at(a, 0)", "SELECT element_at(array(1, 2, 3), 0)"),
    ("element_at(a, -1)", "SELECT element_at(array(1, 2, 3), -1)"),
    ("try_element_at(a, 10)", "SELECT try_element_at(array(1, 2, 3), 10)"),
    ("get(a, 10)", "SELECT get(array(1, 2, 3), 10)"),
    ("a[0] (zero-based)", "SELECT array(1, 2, 3)[0]"),
    ("slice(a, 0, 1)", "SELECT slice(array(1, 2, 3), 0, 1)"),
    ("m['missing']", "SELECT map('a', 1)['missing']"),
    ("element_at(m, 'missing')", "SELECT element_at(map('a', 1), 'missing')"),
]:
    attempt(label, lambda sql=sql: spark.sql(sql).first()[0])

section("generators: rows from arrays and maps")
lines = orders.select("order_id", F.posexplode_outer("lines").alias("pos", "line"))
print("posexplode_outer rows:", lines.count(), " orders:", orders.count())
show(lines.select("order_id", (F.col("pos") + 1).alias("line_no"), "line.sku", "line.qty").orderBy("order_id", "line_no"), 4)
kv = orders.select(F.explode("lines").alias("l")).select(F.explode("l.attrs").alias("key", "value"))
show(kv.groupBy("key").agg(F.count("*").alias("n"), F.array_sort(F.collect_set("value")).alias("values")).orderBy("key"))
orders.createOrReplaceTempView("orders")
print("LATERAL VIEW OUTER explode:",
      spark.sql("SELECT order_id, t FROM orders LATERAL VIEW OUTER explode(tags) x AS t").count(), "rows")
print("LATERAL explode in FROM:",
      spark.sql("SELECT o.order_id, t.col FROM orders o, LATERAL explode(o.tags) t").count(), "rows")
lj = orders.lateralJoin(spark.tvf.explode_outer(F.col("tags").outer()))
print("DataFrame.lateralJoin(spark.tvf.explode_outer(tags.outer())):", lj.count(), "rows")
attempt("explode inside an expression", lambda: orders.select(F.upper(F.explode("tags"))).collect())
attempt("two generators in one select", lambda: orders.select(F.explode("tags"), F.explode("lines")).count())
print("  sum over orders of size(tags) * size(lines):",
      orders.select(F.sum(F.coalesce(F.size("tags"), F.lit(0)) * F.coalesce(F.size("lines"), F.lit(0)))).first()[0])
attempt("generator with an aggregate", lambda: orders.select(F.explode("tags"), F.count("*")).collect())
two = orders.select("order_id", F.explode("tags").alias("t")).select("order_id", "t", F.explode(F.array(F.lit(1), F.lit(2))))
print("two explodes, one after the other:", two.count(), "rows from",
      orders.select(F.explode("tags")).count(), "tags x 2")

section("the plan of explode: Spark adds a filter before Generate")
for label, q in [("explode(tags)", orders.select("order_id", F.explode("tags"))),
                 ("explode_outer(tags)", orders.select("order_id", F.explode_outer("tags")))]:
    print(label)
    for l in plan(q, "simple").splitlines()[1:]:
        if l.strip():
            print("  " + re.sub(r"#\d+", "", l)[:88])

section("collect_list keeps no order: sort by position")
long = orders.select("order_id", F.posexplode("lines").alias("pos", "line"))
back = (long.groupBy("order_id")
        .agg(F.array_sort(F.collect_list(F.struct("pos", "line.sku"))).alias("pairs"))
        .select("order_id", F.col("pairs.sku").alias("skus")))
show(back.orderBy("order_id"), 3)
show(spark.sql("""SELECT collect_list(x) AS default_list, collect_list(x) RESPECT NULLS AS respect_nulls
                  FROM VALUES (1), (NULL), (2) AS t(x)"""))
show(spark.sql("""SELECT max_by(x, y) AS max_by, max_by(x, y, 2) AS max_by_2, min_by(x, y, 2) AS min_by_2
                  FROM VALUES ('a', 1), ('b', 5), ('c', 3) AS t(x, y)"""))

section("maps")
m = spark.sql("SELECT map('b', 2, 'a', 1) AS m")
for label, expr in [
    ("cast(m as string)", F.col("m").cast("string")),
    ("m collected to Python", F.col("m")),
    ("map_keys(m)", F.map_keys("m")),
    ("element_at(m, 'a')", F.element_at("m", F.lit("a"))),
    ("map_contains_key(m, 'z')", F.map_contains_key("m", F.lit("z"))),
    ("map_filter(v > 1)", F.map_filter("m", lambda k, v: v > 1)),
    ("transform_values(v * 10)", F.transform_values("m", lambda k, v: v * 10)),
    ("transform_keys(upper)", F.transform_keys("m", lambda k, v: F.upper(k))),
    ("map_zip_with(m, m, v1 + v2)", F.map_zip_with("m", "m", lambda k, a, b: a + b)),
    ("map_from_arrays", F.map_from_arrays(F.array(F.lit("x")), F.array(F.lit(1)))),
    ("str_to_map('a:1,b:2')", F.str_to_map(F.lit("a:1,b:2"))),
]:
    print(f"  {label:30} {m.select(expr).first()[0]}")
dup = spark.sql("SELECT map('Gift', 1, 'gift', 2) AS m")
attempt("transform_keys(lower) makes a duplicate key",
        lambda: dup.select(F.transform_keys("m", lambda k, v: F.lower(k))).first()[0])
spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
attempt("  the same with mapKeyDedupPolicy=LAST_WIN",
        lambda: dup.select(F.transform_keys("m", lambda k, v: F.lower(k))).first()[0])
spark.conf.unset("spark.sql.mapKeyDedupPolicy")
attempt("map(NULL, 1)", lambda: spark.sql("SELECT map(CAST(NULL AS STRING), 1)").first()[0])
attempt("ORDER BY a map", lambda: spark.sql("SELECT m FROM (SELECT map('a', 1) AS m) ORDER BY m").collect())
attempt("INTERSECT on maps", lambda: spark.sql("SELECT map('a', 1) INTERSECT SELECT map('a', 1)").collect())
attempt("GROUP BY a map", lambda: spark.sql(
    "SELECT m, count(*) FROM VALUES (map('a', 1)), (map('a', 1)) AS t(m) GROUP BY m").collect())
attempt("COUNT(DISTINCT m), keys in two orders (4.3)", lambda: spark.sql(
    "SELECT count(DISTINCT m) FROM VALUES (map('a', 1, 'b', 2)), (map('b', 2, 'a', 1)) AS t(m)").first()[0])

section("to_json and back")
j = orders.select("order_id", F.to_json("customer").alias("json")).orderBy("order_id").first()
print(j.json)
print("from_json(to_json(customer)) == customer:",
      orders.select((F.from_json(F.to_json("customer"), orders.schema["customer"].dataType) == F.col("customer"))
                    .alias("same")).where("same IS DISTINCT FROM true").count() == 0)

section("flatten every struct into columns a_b_c")


def flatten_structs(df):
    def columns(schema, prefix):
        out = []
        for field in schema.fields:
            path = prefix + [field.name]
            if isinstance(field.dataType, StructType):
                out += columns(field.dataType, path)
            else:
                out.append(F.col(".".join(f"`{p}`" for p in path)).alias("_".join(path)))
        return out
    return df.select(columns(df.schema, []))


flat = flatten_structs(orders)
print(flat.columns)
