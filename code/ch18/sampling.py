"""Chapter 18: UNNEST in pipe syntax, and TABLESAMPLE, sample and sampleBy with what each one reads.

Run from code/:  uv run python ch18/sampling.py
"""
import json
import re
import sys
import urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS, ZONES

spark = session("ch18-sampling", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips")
spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES)).createOrReplaceTempView("zones")


def rows_read(spark):
    """'number of output rows' of the Parquet scan in the last SQL execution."""
    base = spark.sparkContext.uiWebUrl
    app = json.load(urllib.request.urlopen(base + "/api/v1/applications"))[0]["id"]
    ex = json.load(urllib.request.urlopen(f"{base}/api/v1/applications/{app}/sql?details=true&length=10000"))[-1]
    for n in ex["nodes"]:
        if n["nodeName"] == "Scan parquet":
            for m in n["metrics"]:
                if m["name"] == "number of output rows":
                    return int(m["value"].replace(",", ""))
    return None


def ops(df):
    out = []
    for l in plan(df, "simple").splitlines():
        s = l.strip().lstrip("+-: ").lstrip("*()0123456789 ")
        name = s.split(" ")[0].split("(")[0]
        if name in ("Sample", "Filter", "CollectLimit", "GlobalLimit", "LocalLimit", "Generate", "Exchange"):
            out.append(re.sub(r"#\d+L?", "", s)[:60])
    return out


def err(e):
    first = next(l for l in str(e).splitlines() if l.strip())
    return first.strip()[:96]


# 1. UNNEST in a pipe query: FROM UNNEST, and a LATERAL join in the middle of a pipe.
print("== 1. UNNEST in pipe syntax")
spark.sql("""
FROM UNNEST(array('Manhattan', 'Queens', 'Bronx')) WITH ORDINALITY AS b(name, pos)
|> JOIN zones ON zones.Borough = b.name
|> AGGREGATE count(*) AS zones GROUP BY pos, name
|> ORDER BY pos""").show()
try:
    spark.sql("""FROM UNNEST(array('Queens')) AS b(borough) |> JOIN zones ON zones.Borough = b.borough
                 |> AGGREGATE count(*) GROUP BY borough""").collect()
except Exception as e:
    print("alias column named borough:", err(e)[:60])
P = """
FROM zones
|> WHERE Borough = 'Queens' AND Zone LIKE 'J%'
|> EXTEND split(Zone, ' ') AS words
|> JOIN LATERAL UNNEST(words) WITH ORDINALITY AS w(word, n)
|> SELECT LocationID, Zone, word, n
|> ORDER BY LocationID, n"""
spark.sql(P).show(truncate=False)
print("LEFT JOIN LATERAL in a pipe:", spark.sql("""
FROM VALUES (1, array(10, 20)), (2, CAST(array() AS ARRAY<INT>)) AS d(id, arr)
|> LEFT JOIN LATERAL UNNEST(arr) AS u(x) ON true
|> ORDER BY id, x""").collect())

# 2. TABLESAMPLE: the forms, their row counts and what the scan returned.
print("\n== 2. TABLESAMPLE forms on 3,837,248 trips")
FORMS = [
    "TABLESAMPLE (1 PERCENT)",
    "TABLESAMPLE (1 PERCENT) REPEATABLE (42)",
    "TABLESAMPLE BERNOULLI (1 PERCENT) REPEATABLE (42)",
    "TABLESAMPLE (BUCKET 1 OUT OF 100)",
    "TABLESAMPLE (1000 ROWS)",
]
for f in FORMS:
    counts = []
    for _ in range(2):
        df = spark.sql(f"SELECT * FROM trips {f}")
        df.write.format("noop").mode("overwrite").save()
        counts.append(spark.sql(f"SELECT count(*) FROM trips {f}").first()[0])
    df.write.format("noop").mode("overwrite").save()
    print(f"  {f:<50} rows: {counts[0]:>6,} then {counts[1]:>6,}  scan returned: {rows_read(spark):>9,}")
    print(f"  {'':<50} plan: {ops(df)}")

# 3. What does not work.
print("\n== 3. forms that Spark refuses")
for f in ("TABLESAMPLE SYSTEM (1 PERCENT)", "TABLESAMPLE (100M)", "TABLESAMPLE (BUCKET 1 OUT OF 4 ON PULocationID)",
          "TABLESAMPLE (150 PERCENT)"):
    try:
        spark.sql(f"SELECT * FROM trips {f}").collect()
        print(f"  {f:<48} -> ran")
    except Exception as e:
        print(f"  {f:<48} -> {err(e)}")

# 4. A sample filters rows after the scan. A filter before it still reads less.
print("\n== 4. sample after a filter: Parquet pushdown still applies")
df = spark.sql("SELECT * FROM (SELECT * FROM trips WHERE payment_type = 3) TABLESAMPLE (10 PERCENT) REPEATABLE (7)")
n = df.count()
df.write.format("noop").mode("overwrite").save()
print(f"  rows: {n}  scan returned: {rows_read(spark):,}  plan: {ops(df)}")

# 5. The DataFrame API: sample and sampleBy.
print("\n== 5. DataFrame.sample and sampleBy")
trips = spark.table("trips")
a = trips.sample(fraction=0.01, seed=42).count()
b = trips.sample(fraction=0.01, seed=42).count()
c = spark.sql("SELECT count(*) FROM trips TABLESAMPLE (1 PERCENT) REPEATABLE (42)").first()[0]
print(f"  sample(0.01, seed=42): {a:,} and {b:,}   TABLESAMPLE (1 PERCENT) REPEATABLE (42): {c:,}")
print("  plan of sample:", ops(trips.sample(fraction=0.01, seed=42)))
w = trips.sample(withReplacement=True, fraction=0.01, seed=42)
dup = w.groupBy(*trips.columns).count().where("count > 1").count()
print(f"  sample(withReplacement=True, 0.01): {w.count():,} rows, {dup} rows that occur more than one time")
print("  sample(withReplacement=True, fraction=2.0):", f"{trips.sample(True, 2.0, 1).count():,}")
fr = {0: 0.001, 1: 0.001, 2: 0.01, 3: 0.1, 4: 0.1}
s = trips.sampleBy("payment_type", fractions=fr, seed=1)
print("  sampleBy('payment_type', ...) plan:", ops(s))
full = dict(trips.groupBy("payment_type").count().collect())
got = dict(s.groupBy("payment_type").count().collect())
print(f"  {'type':>4} {'all rows':>9} {'fraction':>8} {'sampled':>8} {'expected':>9}")
for k in sorted(full):
    print(f"  {k:>4} {full[k]:>9,} {fr.get(k, 0.0):>8} {got.get(k, 0):>8,} {full[k] * fr.get(k, 0.0):>9.0f}")

# 6. Pipe TABLESAMPLE.
print("\n== 6. TABLESAMPLE as a pipe operator")
print(spark.sql("""FROM trips |> WHERE payment_type = 2 |> TABLESAMPLE (1 PERCENT) REPEATABLE (3)
                   |> AGGREGATE count(*) AS n, round(avg(fare_amount), 2) AS avg_fare""").collect())
