"""Chapter 5: schemas for data you don't control. Inference, enforced schemas, drift and reshaping.

Uses SparkTutor's three days of order events (code/data/events), whose shape changes every day.
Run from code/:  uv run python ch05/drift.py
"""
import shutil
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType
from common.book import session, plan

EVENTS = Path(__file__).resolve().parents[1] / "data" / "events"
spark = session("ch05-drift", spark_sql_session_timeZone="UTC", spark_master="local[4]")
tmp = Path(tempfile.mkdtemp(prefix="ch05-"))


LAST = [None]


def err(f):
    try:
        return f()
    except Exception as e:
        LAST[0] = e
        return "ERROR " + error_line(e)[:150]


def error_line(e):
    """The first line that names an error class; Py4J errors bury it under a Java stack."""
    import re
    for line in str(e).splitlines():
        m = re.search(r"\[[A-Z_.]+\].*", line)
        if m:
            return m.group(0)
    return str(e).splitlines()[0]


def short(s, n=96):
    return s if len(s) <= n else s[: n - 3] + "..."


# 1. Inference: one schema per day.
print("== 1. inferred schema, one day at a time")
days = {d: spark.read.json(str(EVENTS / f"events_2026-10-{d}.json")) for d in ("01", "02", "03")}
for d, df in days.items():
    print(d, df.schema.simpleString())

# 2. samplingRatio: faster, and less stable still.
print("\n== 2. samplingRatio over the whole folder (local[4])")
for r in (1.0, 0.1, 0.05, 0.01):
    df = spark.read.option("samplingRatio", r).json(str(EVENTS))
    print(f"{r:<5} {len(df.columns)} columns, {df.count()} rows: {short(df.schema.simpleString(), 80)}")

# 3. Read through a schema you chose: values that don't fit become NULL, and you can count them.
print("\n== 3. an enforced schema")
c = spark.read.schema("event STRING, amount DOUBLE").json(str(EVENTS))
(c.groupBy(F.col("_metadata.file_name").alias("file"))
  .agg(F.count("*").alias("rows"), F.count("amount").alias("amount_ok"))
  .orderBy("file").show(truncate=False))

# 4. Parquet stores its types: a mismatch fails instead of giving NULL; mergeSchema unions columns.
print("== 4. Parquet: type mismatch and mergeSchema")
days["01"].select("event", "amount").write.parquet(str(tmp / "mix" / "a"))
days["02"].select("event", "amount").write.parquet(str(tmp / "mix" / "b"))   # amount is a string here
mixed = spark.read.schema("event STRING, amount DOUBLE").parquet(str(tmp / "mix" / "a"), str(tmp / "mix" / "b"))
import re
msg = err(lambda: mixed.collect())
print("double + string files:", msg[:70])
print("   ", re.search(r"Data type mismatches.*?BINARY\.", str(msg) + " " + str(LAST[0])).group(0))
days["01"].write.parquet(str(tmp / "p" / "day=1"))
days["02"].withColumn("amount", F.col("amount").try_cast("double")).write.parquet(str(tmp / "p" / "day=2"))
print("default    ", spark.read.parquet(str(tmp / "p")).schema.simpleString())
print("mergeSchema", spark.read.option("mergeSchema", True).parquet(str(tmp / "p")).schema.simpleString())
days["02"].write.parquet(str(tmp / "q" / "day=2"))
days["01"].write.parquet(str(tmp / "q" / "day=1"))
print("merge double + string:", err(lambda: spark.read.option("mergeSchema", True).parquet(str(tmp / "q")).schema)[:110])
print("   ", re.search(r"\[CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE\][^.]*\.", str(LAST[0])).group(0))


# 5. Detecting drift in code: a recursive comparison of two StructTypes.
def schema_drift(expected, actual, prefix=""):
    """(kind, path, detail) for each field missing, added or changed; nullability ignored."""
    drift = []
    actual_fields = {f.name: f for f in actual.fields}
    for field in expected.fields:
        path = prefix + field.name
        if field.name not in actual_fields:
            drift.append(("missing", path, field.dataType.simpleString()))
            continue
        e, a = field.dataType, actual_fields[field.name].dataType
        if isinstance(e, StructType) and isinstance(a, StructType):
            drift.extend(schema_drift(e, a, path + "."))
        elif e.simpleString() != a.simpleString():
            drift.append(("type_changed", path, f"{e.simpleString()} -> {a.simpleString()}"))
    for field in actual.fields:
        if field.name not in {f.name for f in expected.fields}:
            drift.append(("added", prefix + field.name, field.dataType.simpleString()))
    return sorted(drift)


CONTRACT = StructType.fromDDL("event STRING, user STRUCT<id: BIGINT, plan: STRING>, amount DOUBLE, "
                              "items ARRAY<STRUCT<qty: BIGINT, sku: STRING>>, ts TIMESTAMP")
print("\n== 5. drift against the contract")
(tmp / "contract.json").write_text(CONTRACT.json())          # the contract lives in a file
for d in ("01", "03"):
    observed = spark.read.option("inferTimestamp", True).json(str(EVENTS / f"events_2026-10-{d}.json")).schema
    print(f"day {d}:")
    for row in schema_drift(CONTRACT, observed):
        print("  ", row)

# 6. Combining shapes: union by position, by name, and with missing columns.
print("\n== 6. union, unionByName, allowMissingColumns")
d1, d2, d3 = days["01"], days["02"], days["03"]
print("union      :", err(lambda: d1.union(d3)))
print("unionByName:", err(lambda: d1.unionByName(d3)))
u = d1.unionByName(d2, allowMissingColumns=True)
print("d1+d2 allowMissing:", u.schema["amount"].dataType.simpleString())
print([l.strip() for l in plan(u, "simple").splitlines() if "amount" in l and "cast" in l.lower()][0][:110])
print("d1+d3 allowMissing:", err(lambda: d1.unionByName(d3, allowMissingColumns=True)))

# 7. Reshaping: try_cast protects values, not types; DataFrame.to reorders and casts safely.
print("\n== 7. try_cast and DataFrame.to")
print("struct try_cast double:", err(lambda: d3.select(F.col("amount").try_cast("double")).schema))
src = spark.createDataFrame([(2, "b", 1.5, "x")], "id BIGINT, name STRING, amount DOUBLE, extra STRING")
cases = [("columns in another order", "name STRING, id BIGINT, amount DOUBLE"),
         ("extra column dropped", "id BIGINT, name STRING"),
         ("bigint to int", "id INT, name STRING"),
         ("double to string", "id BIGINT, amount STRING"),
         ("string to double", "id BIGINT, name DOUBLE"),
         ("nullable to NOT NULL", "id BIGINT NOT NULL, name STRING"),
         ("missing nullable column", "id BIGINT, missing STRING"),
         ("missing NOT NULL column", "id BIGINT, missing STRING NOT NULL")]
for label, ddl in cases:
    r = err(lambda: str(src.to(StructType.fromDDL(ddl)).first()))
    print(f"  {label:<26} {short(r, 100)}")


def align_column(col, actual, expected):
    """An expression that turns `col` (of type `actual`) into `expected`, NULL where it can't."""
    if actual.simpleString() == expected.simpleString():
        return col
    if isinstance(actual, StructType) and isinstance(expected, StructType):
        have = {f.name: f.dataType for f in actual.fields}
        parts = [align_column(col.getField(f.name), have[f.name], f.dataType).alias(f.name) if f.name in have
                 else F.lit(None).cast(f.dataType).alias(f.name) for f in expected.fields]
        return F.when(col.isNull(), F.lit(None)).otherwise(F.struct(*parts)).cast(expected)
    plain = lambda t: not isinstance(t, (StructType, ArrayType, MapType))
    if plain(actual) and plain(expected):
        return col.try_cast(expected)
    return F.lit(None).cast(expected)


def align_to(df, contract):
    have = {f.name: f.dataType for f in df.schema.fields}
    return df.select(*[align_column(F.col(f.name), have[f.name], f.dataType).alias(f.name) if f.name in have
                       else F.lit(None).cast(f.dataType).alias(f.name) for f in contract.fields])


aligned = align_to(d1, CONTRACT).unionByName(align_to(d3, CONTRACT))
print("aligned day 1 + day 3:", aligned.count(), "rows,", aligned.schema.simpleString()[:70] + "...")
aligned.groupBy(F.col("event").isNull().alias("event_null"), F.col("amount").isNull().alias("amount_null")) \
       .count().orderBy("event_null", "amount_null").show()

# 8. Awkward names.
print("== 8. column names from the wild")
weird = spark.createDataFrame([(1, 2, 3)], ["order id", "user.name", "Amount"])
for label, f in [("select('order id')", lambda: weird.select("order id").columns),
                 ("selectExpr('order id')", lambda: weird.selectExpr("order id").columns),
                 ("selectExpr('`order id`')", lambda: weird.selectExpr("`order id`").columns),
                 ("select('user.name')", lambda: weird.select("user.name").columns),
                 ("select('`user.name`')", lambda: weird.select("`user.name`").columns),
                 ("select('amount')", lambda: weird.select("amount").columns),
                 ("withColumnsRenamed", lambda: weird.withColumnsRenamed(
                     {"order id": "order_id", "user.name": "user_name", "nope": "x"}).columns)]:
    print(f"  {label:<26} {short(str(err(f)), 70)}")
(tmp / "dup.json").write_text('{"amount": 1, "Amount": 2}\n')
print("  JSON amount + Amount      ", short(str(err(lambda: spark.read.json(str(tmp / "dup.json")).columns)), 70))
(tmp / "h.csv").write_text("id,name,name,,total amount\n1,a,b,c,9\n")
print("  CSV header                ", spark.read.option("header", True).csv(str(tmp / "h.csv")).columns)
shutil.rmtree(tmp)
