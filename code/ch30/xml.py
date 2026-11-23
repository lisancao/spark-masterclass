"""Chapter 30, part 1: XML. rowTag, attributes and _VALUE, one child or many, the VARIANT mode,
from_xml, schema_of_xml, malformed records, namespaces and how the XML source splits a file.

Run from code/:  uv run python ch30/xml.py
"""
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY, WORK

spark = session("ch30-xml", spark_sql_session_timeZone="UTC")
ORDERS = str(MESSY / "xml" / "orders.xml")
TMP = WORK / "ch30"
shutil.rmtree(TMP, ignore_errors=True)
TMP.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:60]}"


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label:44} -> {r}")
    except Exception as e:
        print(f"  {label:44} -> {condition(e)}")


def write(name, text):
    p = TMP / name
    p.write_text(text)
    return str(p)


section("rowTag is required")
attempt("spark.read.xml(path) without rowTag", lambda: spark.read.xml(ORDERS).count())
orders = spark.read.option("rowTag", "order").xml(ORDERS)
orders.printSchema()
show(orders.select("_id", "_channel", "customer", "coupon", "items").orderBy("_id"), 3)
print("orders:", orders.count(), " rowTag elements in the file:", open(ORDERS).read().count("<order "))

section("attributes, text and _VALUE")
price = write("prices.xml", """<rows>
<row><price cur="CAD">4.50</price><qty>2</qty></row>
<row><price>3.00</price><qty unit="kg">1.5</qty></row>
</rows>""")
p = spark.read.option("rowTag", "row").xml(price)
p.printSchema()
show(p)
show(spark.read.option("rowTag", "row").option("valueTag", "text").option("attributePrefix", "@").xml(price))
print("excludeAttribute=true:", spark.read.option("rowTag", "row").option("excludeAttribute", "true").xml(price).schema.simpleString())
print("inferSchema=false:    ", spark.read.option("rowTag", "row").option("inferSchema", "false").xml(price).schema.simpleString())

section("one child or many: inference sees the whole file")
one = write("one_item.xml", """<orders>
<order id="1"><items><item sku="P1"><qty>2</qty></item></items></order>
<order id="2"><items><item sku="P2"><qty>1</qty></item></items></order>
</orders>""")
print("orders.xml (1 to 3 items):", orders.schema["items"].dataType.simpleString())
print("a file where each order has one item:", spark.read.option("rowTag", "order").xml(one).schema["items"].dataType.simpleString())
attempt("explode(items.item) on that file", lambda: spark.read.option("rowTag", "order").xml(one)
        .select(F.explode("items.item")).count())
SCHEMA = "_id BIGINT, _channel STRING, customer STRING, coupon STRING, items STRUCT<item: ARRAY<STRUCT<_sku: STRING, qty: INT>>>"
pinned = spark.read.option("rowTag", "order").schema(SCHEMA).xml(one)
print("with ARRAY in the schema:", pinned.select(F.explode("items.item")).count(), "item rows")
items = (orders.select(F.col("_id").alias("order_id"), F.explode("items.item").alias("it"))
         .select("order_id", F.col("it._sku").alias("sku"), F.col("it.qty").alias("qty")))
print("item rows from orders.xml:", items.count())

section("VARIANT mode: one child is an OBJECT, many are an ARRAY")
ov = spark.read.option("rowTag", "order").option("singleVariantColumn", "v").xml(ORDERS)
ov.createOrReplaceTempView("ov")
show(spark.sql("""SELECT v:_id::int AS id, schema_of_variant(v:items.item) AS item_shape
                  FROM ov ORDER BY id LIMIT 3"""))
print(spark.sql("SELECT schema_of_variant_agg(v) FROM ov").first()[0])
for label, sql in [
    ("v:items.item::array<variant> on one item", "SELECT v:items.item::array<variant> FROM ov WHERE v:_id::int = 2"),
    ("try_variant_get(..., 'array<variant>')", "SELECT try_variant_get(v, '$.items.item', 'array<variant>') FROM ov WHERE v:_id::int = 2"),
    ("v:items.item[0] on one item", "SELECT v:items.item[0] FROM ov WHERE v:_id::int = 2"),
]:
    attempt(label, lambda sql=sql: spark.sql(sql).first()[0])
norm = spark.sql("""
    WITH norm AS (
      SELECT v:_id::bigint AS order_id,
             CASE WHEN schema_of_variant(v:items.item) LIKE 'ARRAY%'
                  THEN v:items.item::array<variant>
                  ELSE array(v:items.item) END AS items
      FROM ov)
    SELECT order_id, it:_sku::string AS sku, it:qty::int AS qty
    FROM norm LATERAL VIEW explode(items) t AS it""")
print("item rows from VARIANT, after the CASE:", norm.count())
norm43 = spark.sql("""
    SELECT v:_id::bigint AS order_id, it.value:_sku::string AS sku
    FROM ov, LATERAL variant_explode(
      CASE WHEN schema_of_variant(v:items.item) LIKE 'ARRAY%' THEN v:items.item
           ELSE variant_array_append(parse_json('[]'), '$', v:items.item) END) AS it""")
print("the same with variant_array_append (4.3):", norm43.count())

section("from_xml and schema_of_xml on a string column")
snips = spark.createDataFrame([("<price cur=\"CAD\">4.5</price>",), ("<price cur=\"USD\">12</price>",)], "xml STRING")
show(snips.select("xml", F.from_xml("xml", "_VALUE DOUBLE, _cur STRING").alias("p")).select("xml", "p._VALUE", "p._cur"))
print("schema_of_xml(lit):", spark.range(1).select(F.schema_of_xml(F.lit('<price cur="CAD">4.5</price>'))).first()[0])
attempt("schema_of_xml('<price ...>') as a column name", lambda: spark.range(1).select(
    F.schema_of_xml('<price cur="CAD">4.5</price>')).first()[0])
show(snips.select(F.from_xml("xml", "variant").alias("v")))

section("malformed XML: one bad tag")
bad = write("bad.xml", """<orders>
<order id="1"><customer>C1</customer></order>
<order id="2"><customer>C2</custmer></order>
<order id="3"><customer>C3</customer></order>
<order id="4"><customer>C4</customer></order>
</orders>""")
for legacy in ("false", "true"):
    spark.conf.set("spark.sql.legacy.useLegacyXMLParser", legacy)
    d = spark.read.option("rowTag", "order").option("columnNameOfCorruptRecord", "_bad").xml(bad).cache()
    print(f"useLegacyXMLParser={legacy}: rows {d.count()}, good {d.where('_bad IS NULL').count()}")
    show(d.select("_id", "customer", F.regexp_replace(F.substring("_bad", 1, 60), "\n", " ").alias("_bad")).orderBy("_id"))
    d.unpersist()
spark.conf.unset("spark.sql.legacy.useLegacyXMLParser")
attempt("mode=FAILFAST", lambda: spark.read.option("rowTag", "order").option("mode", "FAILFAST").xml(bad).count())

section("namespaces")
ns = write("ns.xml", """<feed xmlns:s="urn:shop">
<s:order s:id="1"><s:customer>C1</s:customer></s:order>
<s:order s:id="2"><s:customer>C2</s:customer></s:order>
</feed>""")
print("rowTag=s:order:            ", spark.read.option("rowTag", "s:order").xml(ns).columns)
print("rowTag=order, ignoreNamespace:", spark.read.option("rowTag", "order").option("ignoreNamespace", "true").xml(ns).columns,
      spark.read.option("rowTag", "order").option("ignoreNamespace", "true").xml(ns).count(), "rows")

section("the XML source splits a large file")
big = TMP / "big.xml"
with open(big, "w") as f:
    f.write("<orders>\n")
    for i in range(400_000):
        f.write(f'<order id="{i}"><customer>C{i % 50}</customer><qty>{i % 7}</qty></order>\n')
    f.write("</orders>\n")
spark.conf.set("spark.sql.files.maxPartitionBytes", str(4 * 1024 * 1024))
for ml in ("true", "false"):
    bx = (spark.read.option("rowTag", "order").option("multiLine", ml)
          .option("columnNameOfCorruptRecord", "_bad")
          .schema("_id BIGINT, customer STRING, qty INT, _bad STRING").xml(str(big))).cache()
    print(f"multiLine={ml:5}: {big.stat().st_size / 1e6:.1f} MB file, maxPartitionBytes 4 MB -> "
          f"{bx.rdd.getNumPartitions()} partitions, {bx.count()} rows, "
          f"{bx.select(F.countDistinct('_id')).first()[0]} distinct ids")
    print("  corrupt rows:", [r._bad for r in bx.where("_bad IS NOT NULL").collect()])
    bx.unpersist()
spark.conf.unset("spark.sql.files.maxPartitionBytes")

section("write XML")
out = str(TMP / "out")
items.write.mode("overwrite").option("rootTag", "items").option("rowTag", "item").xml(out)
part = next(Path(out).glob("part-*"))
print("\n".join(part.read_text().splitlines()[:5]))
