"""Chapter 90, Step 7: shredding. Spark 4.3 writes a VARIANT column to Parquet as a tree of typed columns,
infers that tree from the data, and reads only the leaves that a query asks for. pyarrow shows the columns;
from_shredded() puts the rows back together in Python, and Spark checks the result.

Run from code/:  uv run python ch90/shredding.py
"""
import glob, os, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow as pa
import pyarrow.parquet as pq
from common.book import session, plan, ROOT
from variant_codec import decode, parse_metadata, json_number_hook, spaced

spark = session("ch90-shredding")
EVENTS = ROOT / "code" / "data" / "events"
tmp = tempfile.mkdtemp(prefix="ch90-")
print(f"spark.sql.variant.writeShredding.enabled = {spark.conf.get('spark.sql.variant.writeShredding.enabled')}")
print(f"spark.sql.variant.inferShreddingSchema   = {spark.conf.get('spark.sql.variant.inferShreddingSchema')}")

def tree(t, name="v", depth=0):
    """The Parquet group of a shredded variant, one line per column, with types shortened."""
    pad = "  " * depth
    if pa.types.is_struct(t):
        print(f"{pad}{name}")
        for f in t:
            tree(f.type, f.name, depth + 1)
    elif pa.types.is_list(t):
        print(f"{pad}{name}: list of")
        tree(t.value_type, "element", depth + 1)
    else:
        print(f"{pad}{name}: {t}")

df = spark.read.option("singleVariantColumn", "v").json(str(EVENTS)).where("v IS NOT NULL")
df.coalesce(1).write.parquet(f"{tmp}/shredded")
f = glob.glob(f"{tmp}/shredded/*.parquet")[0]
vt = pq.read_schema(f).field("v").type
print("\n--- the Parquet columns of v (inferred from the 60 events):")
tree(vt)

table = pq.read_table(f).column("v").to_pylist()

def state(group):
    v, t = group.get("value"), group.get("typed_value")
    if v is None and t is None:
        return "missing"
    if t is not None and v is None:
        return f"typed_value = {t!r}"[:40]
    if t is None:
        return f"value = {spaced(v)}"[:40]
    return "both (a partially shredded object)"

print("\n--- three rows, field by field (value / typed_value):")
def first(field, also=None):
    return next(r for r in table if r["typed_value"][field]["typed_value"] is not None
                and (also is None or r["typed_value"][also]["typed_value"] is not None))
for label, row in [("day 1", first("user_id")), ("day 2", first("event", "items")), ("day 3", first("event_type"))]:
    print(f"{label}: top-level value = {row['value']!r}, metadata {len(row['metadata'])} bytes")
    for k, g in row["typed_value"].items():
        print(f"    {k:<11} {state(g)}")

def from_shredded(group, keys):
    """Rebuild a Python value from one shredded group. Returns MISSING for a field that is not there."""
    v, t = group.get("value"), group.get("typed_value")
    if t is not None:
        if isinstance(t, dict):                       # a shredded object
            out = {}
            for k, sub in t.items():
                x = from_shredded(sub, keys)
                if x is not MISSING:
                    out[k] = x
            if v is not None:                         # fields that the schema does not name
                out.update(decode(v, keys))
            return out
        if isinstance(t, list):
            return [from_shredded(e, keys) for e in t]
        return t
    if v is not None:
        return decode(v, keys)
    return MISSING

MISSING = object()
spark_rows = spark.read.parquet(f"{tmp}/shredded").collect()
ok = 0
for row, srow in zip(table, spark_rows):
    keys = parse_metadata(row["metadata"])["keys"]
    mine = from_shredded({"value": row["value"], "typed_value": row["typed_value"]}, keys)
    theirs = decode(bytes(srow.v.value), parse_metadata(bytes(srow.v.metadata))["keys"])
    ok += mine == theirs
print(f"\nfrom_shredded() vs Spark's own read of the file: {ok} of {len(table)} rows equal")

print("\n--- what one field shreds to, for a few mixes of values:")
for name, docs in [("int, decimal", ['{"x": 1}', '{"x": 2.5}']), ("int, double", ['{"x": 1}', '{"x": 2.5e0}']),
                   ("int, string", ['{"x": 1}', '{"x": "a"}', '{"x": "b"}']),
                   ("int, JSON null, missing", ['{"x": 1}', '{"x": null}', '{}'])]:
    p = f"{tmp}/{name.replace(', ', '_').replace(' ', '')}"
    spark.createDataFrame([(d,) for d in docs], "j string").selectExpr("parse_json(j) v").coalesce(1).write.parquet(p)
    ff = glob.glob(p + "/*.parquet")[0]
    xt = pq.read_schema(ff).field("v").type.field("typed_value").type.field("x").type
    cols = ", ".join(f"{c.name}: {c.type}" for c in xt)
    rows = [r["typed_value"]["x"] for r in pq.read_table(ff).column("v").to_pylist()]
    print(f"  {name:<24} x -> {cols}")
    for d, r in zip(docs, rows):
        print(f"      {d:<14} value={r.get('value')!r:<44} typed_value={r.get('typed_value')!r}")

print("\n--- the read side: a query for one field reads one leaf")
q = spark.read.parquet(f"{tmp}/shredded").selectExpr("variant_get(v, '$.user.plan', 'string') AS plan")
scan = [l for l in plan(q, "simple").splitlines() if "FileScan" in l][0]
print(" ", scan[scan.index("ReadSchema"):])
print("  rows:", sorted({r.plan for r in q.collect()}, key=str))

spark.conf.set("spark.sql.variant.writeShredding.enabled", "false")
df.coalesce(1).write.parquet(f"{tmp}/plain")
fp = glob.glob(f"{tmp}/plain/*.parquet")[0]
print("\n--- the same 60 rows with writeShredding.enabled = false:")
tree(pq.read_schema(fp).field("v").type)
print(f"file size: shredded {os.path.getsize(f):,} B, not shredded {os.path.getsize(fp):,} B")
