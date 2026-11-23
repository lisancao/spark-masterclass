"""Chapter 90, Step 6: bytes made in Python, given to Spark. Spark checks bounds, not the rules of the spec.

Run from code/:  uv run python ch90/handmade.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from pyspark.sql.types import VariantVal
from common.book import session, ROOT
from variant_codec import encode_json, spaced

spark = session("ch90-handmade")
EVENTS = ROOT / "code" / "data" / "events"

def short(e):
    """The error condition and the start of its message, also from inside a Py4JJavaError."""
    import re
    m = re.search(r"\[[A-Z_]+(\.[A-Z_]+)?\][^\n]*", str(e))
    return (m.group(0) if m else str(e).split("\n")[0])[:88]

# 1. the Python writer gives the same bytes as parse_json for every event
lines = [l for f in sorted(EVENTS.glob("*.json")) for l in f.read_text().splitlines() if l.strip()]
rows = spark.createDataFrame([(l,) for l in lines], "j string") \
            .select("j", F.expr("try_parse_json(j)").alias("v")).collect()
same = sum(1 for r in rows if r.v is not None and encode_json(r.j) == (bytes(r.v.value), bytes(r.v.metadata)))
print(f"Python writer vs parse_json: {same} of {sum(r.v is not None for r in rows)} rows byte-identical")

# 2. hand-made bytes go into a DataFrame as VariantVal, and Spark reads them
v, m = encode_json('{"event": "view", "user_id": 7}')
df = spark.createDataFrame([(VariantVal(v, m),)], "v variant")
r = df.selectExpr("to_json(v) j", "variant_get(v, '$.user_id', 'int') u").first()
print(f"\nhand-made value {spaced(v)}")
print(f"  to_json: {r.j}; variant_get user_id: {r.u}")

# 3. broken bytes: a value cut short, a bad primitive id, a bad metadata version
cases = {"value cut after 5 bytes": (v[:5], m),
         "primitive id 31 (unused)": (bytes([31 << 2]), m),
         "metadata version 2": (v, bytes([0x02]) + m[1:])}
for label, (vv, mm) in cases.items():
    try:
        out = spark.createDataFrame([(VariantVal(vv, mm),)], "v variant").selectExpr("to_json(v)").first()[0]
        print(f"  {label:<26} -> {out}")
    except Exception as e:
        print(f"  {label:<26} -> {short(e)}")

# 4. field ids that break the sort rule: linear search below 32 fields, binary search from 32
print("\nAn object whose field ids are NOT sorted by key (the spec requires sorted ids):")
for n in (5, 31, 32, 40):
    doc = "{" + ", ".join(f'"k{i:02d}": {i}' for i in range(n)) + "}"
    vs, ms = encode_json(doc, sort_ids=True)
    vu, mu = encode_json("{" + ", ".join(f'"k{i:02d}": {i}' for i in reversed(range(n))) + "}", sort_ids=False)
    got = []
    for vv, mm in ((vs, ms), (vu, mu)):
        rr = spark.createDataFrame([(VariantVal(vv, mm),)], "v variant") \
                  .selectExpr("variant_get(v, '$.k00', 'int') a", "variant_get(v, '$.k03', 'int') b").first()
        got.append(f"k00={rr.a} k03={rr.b}")
    print(f"  {n:>2} fields   sorted ids: {got[0]:<16}   reverse-order ids: {got[1]}")

# 5. duplicate keys
print("\nDuplicate keys in the JSON text:")
for conf in ("false", "true"):
    spark.conf.set("spark.sql.variant.allowDuplicateKeys", conf)
    try:
        out = spark.sql("""SELECT to_json(parse_json('{"a": 1, "a": 2}'))""").first()[0]
    except Exception as e:
        out = short(e)
    tp = spark.sql("""SELECT try_parse_json('{"a": 1, "a": 2}') IS NULL""").first()[0]
    print(f"  allowDuplicateKeys={conf:<5} parse_json -> {out}")
    print(f"  {'':<24} try_parse_json IS NULL -> {tp}")
spark.conf.unset("spark.sql.variant.allowDuplicateKeys")
