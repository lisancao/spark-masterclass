"""Chapter 21: a parsing UDTF against JSON_TABLE (4.3) and from_json + explode, on the same JSON documents.

Each document is one order with an array of items. All three methods make one row per item.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch21/json_table.py
"""
import json, os, random, statistics, sys, time, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

warnings.filterwarnings("ignore")
from pyspark.sql import functions as F
from pyspark.sql.functions import udtf
from common.book import session, plan

spark = session("ch21-json-table", spark_ui_showConsoleProgress="false")
N = int(os.environ.get("ORDERS", 200_000))
RUNS = int(os.environ.get("RUNS", 5))
path = Path(f"/tmp/masterclass-ch21/orders-{N}")
if not path.exists():
    rnd = random.Random(7)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_suffix(".txt"), "w") as f:
        for i in range(N):
            items = [{"sku": f"S{rnd.randrange(500):03d}", "qty": rnd.randint(1, 5),
                      "price": round(rnd.uniform(1, 80), 2)} for _ in range(rnd.randint(1, 6))]
            doc = {"order": i, "store": rnd.choice(["Toronto", "Ottawa", "Hanoi"]), "items": items}
            if i % 50_000 == 7:
                doc = '{"order": %d, "items": [{"sku": "S001", "qty": 2' % i     # a truncated document
            f.write((doc if isinstance(doc, str) else json.dumps(doc)) + "\n")
    spark.read.text(str(path.with_suffix(".txt"))).write.parquet(str(path))
docs = spark.read.parquet(str(path)).withColumnRenamed("value", "doc")
docs.createOrReplaceTempView("docs")


@udtf(returnType="order int, seq int, sku string, qty int, price double")
class Items:
    def eval(self, doc: str):
        try:
            d = json.loads(doc)
        except (TypeError, ValueError):
            return                                    # a bad document gives no rows
        for i, it in enumerate(d.get("items") or [], 1):
            yield d.get("order"), i, it.get("sku"), it.get("qty"), it.get("price")


spark.udtf.register("items", Items)
METHODS = {
    "Python UDTF": lambda: spark.sql("SELECT i.* FROM docs, LATERAL items(doc) i"),
    "JSON_TABLE": lambda: spark.sql("""
        SELECT get_json_object(doc, '$.order') :: int AS order, t.*
        FROM docs, LATERAL JSON_TABLE(doc, '$.items[*]'
             COLUMNS (seq FOR ORDINALITY, sku STRING PATH '$.sku', qty INT PATH '$.qty',
                      price DOUBLE PATH '$.price')) AS t"""),
    "from_json + explode": lambda: (docs.select(F.from_json("doc", "order int, items array<struct<sku string, "
                                                                   "qty int, price double>>").alias("d"))
                                    .select("d.order", F.posexplode("d.items").alias("pos", "it"))
                                    .select("order", (F.col("pos") + 1).alias("seq"), "it.*")),
}

print(f"{N:,} JSON documents (Parquet, one string column); one row per item\n")
ref = None
for name, build in METHODS.items():
    df = build()
    import re
    ops = [re.sub(r"^\*\(\d+\) ", "", l.strip().lstrip("+-: ")).split(" [")[0].split("(")[0]
           for l in plan(df, "simple").splitlines()
           if any(k in l for k in ("EvalPythonUDTF", "Generate", "JsonTable", "Lateral"))]
    rows = df.count()
    agg = df.agg(F.sum(F.col("qty") * F.col("price"))).first()[0]
    bad = df.where("sku IS NULL").count()
    print(f"{name:<22} rows {rows:>9,}  sum(qty*price) {agg:>14,.2f}  rows with NULL sku {bad:>3}  plan: {', '.join(dict.fromkeys(ops))}")

print(f"\nnoop write, median of {RUNS} runs after a warm-up:")
for name, build in METHODS.items():
    build().write.format("noop").mode("overwrite").save()
    ts = []
    for _ in range(RUNS):
        t = time.perf_counter(); build().write.format("noop").mode("overwrite").save(); ts.append(time.perf_counter() - t)
    print(f"  {name:<22}{statistics.median(ts):6.2f} s", flush=True)

print("\nJSON_TABLE on a truncated document, the default (NULL ON ERROR) and ERROR ON ERROR:")
bad_doc = "'{\"order\": 1, \"items\": [{\"sku\": \"S001\"'"
print("  NULL ON ERROR  ->", spark.sql(f"SELECT count(*) FROM JSON_TABLE({bad_doc}, '$.items[*]' COLUMNS (sku STRING))").first()[0], "rows")
try:
    spark.sql(f"SELECT * FROM JSON_TABLE({bad_doc}, '$.items[*]' COLUMNS (sku STRING) ERROR ON ERROR)").collect()
except Exception as e:
    import re
    print("  ERROR ON ERROR ->", (re.findall(r"\[([A-Z][A-Z_.]+)\]", str(e)) or [str(e)[:80]])[0])
