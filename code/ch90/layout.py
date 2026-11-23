"""Chapter 90, Steps 1-4: the two blobs of a VARIANT, the metadata dictionary, the value header, an object
and an array, byte by byte. Spark makes the bytes; variant_codec.py reads them; Spark checks the reading.

Run from code/:  uv run python ch90/layout.py
"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from common.book import session, ROOT
from variant_codec import (parse_metadata, object_layout, array_layout, decode, type_name, hexdump,
                           spaced, json_number_hook)

spark = session("ch90-layout")
EVENTS = ROOT / "code" / "data" / "events"

# Step 1: a VARIANT column is two binaries
df = spark.read.option("singleVariantColumn", "v").json(str(EVENTS))
print("schema:", df.schema.simpleString())
rows = df.collect()
print(f"{len(rows)} rows from 3 files; Python type of one value: {type(rows[0].v).__name__}")
vals = [r.v for r in rows if r.v is not None]
print(f"rows with a NULL variant (the broken last line of day 3): {len(rows) - len(vals)}")
print(f"value bytes: min {min(len(v.value) for v in vals)}, max {max(len(v.value) for v in vals)}; "
      f"metadata bytes: min {min(len(v.metadata) for v in vals)}, max {max(len(v.metadata) for v in vals)}")
distinct_meta = {bytes(v.metadata) for v in vals}
print(f"distinct metadata blobs among {len(vals)} rows: {len(distinct_meta)}")

# Step 2: the metadata dictionary of one row
first = spark.sql("""SELECT parse_json('{"event": "view", "user_id": 0, "amount": 6.13, '
                     '"ts": "2026-10-01T10:00:00"}') AS v""").first().v
m = bytes(first.metadata)
md = parse_metadata(m)
osz = md["offset_size"]
marks = [(0, "header: version, sorted_strings, offset_size"), (1, f"dictionary size ({osz} byte)"),
         (1 + osz, f"{md['size'] + 1} offsets"), (md["strings_at"], "UTF-8 strings, no separators")]
print("\n--- metadata of {\"event\", \"user_id\", \"amount\", \"ts\"}:", len(m), "bytes")
print(hexdump(m, marks))
print(f"header 0x{m[0]:02x}: version {md['version']}, sorted_strings {md['sorted_strings']}, "
      f"offset_size {md['offset_size']}")
print(f"size {md['size']}, offsets {md['offsets']}, keys {md['keys']}")

# Step 3: the value of the same row
v = bytes(first.value)
L = object_layout(v, 0)
print("\n--- value:", len(v), "bytes")
marks = [(0, "header"), (1, "size"), (L["id_at"], "field ids"), (L["off_at"], "field offsets"),
         (L["data_at"], "field data")]
print(hexdump(v, marks))
print(f"header 0x{v[0]:02x} = 0b{v[0]:08b}: basic type {v[0] & 3} (OBJECT), is_large {L['is_large']}, "
      f"id size {L['id_size']}, offset size {L['offset_size']}")
print(f"size {L['size']}; ids {L['ids']} = {[md['keys'][i] for i in L['ids']]}; offsets {L['offsets']}")
for i, o in zip(L["ids"], L["offsets"]):
    p = L["data_at"] + o
    nxt = min([L["data_at"] + x for x in L["offsets"] if L["data_at"] + x > p], default=len(v))
    print(f"  {md['keys'][i]:<8} at data+{o:<3} {spaced(v[p:nxt]):<45} {type_name(v, p):<9} {decode(v, md['keys'], p)!r}")

# Step 4: nested objects and arrays; check the reader against Spark on every row
nested = spark.sql("""SELECT parse_json('{"user": {"id": 2, "plan": "pro"}, '
                      '"items": [{"sku": "P9", "qty": 3}, {"sku": "P6", "qty": 2}]}') AS v""").first().v
v, m = bytes(nested.value), bytes(nested.metadata)
md = parse_metadata(m)
L = object_layout(v, 0)
print("\n--- a nested value:", len(v), "bytes; keys", md["keys"])
print(hexdump(v, [(0, "object: 2 fields"), (L["data_at"], "data of field 'user' (an object)")]))
items_at = L["data_at"] + L["offsets"][[md["keys"][i] for i in L["ids"]].index("items")]
A = array_layout(v, items_at)
print(f"'items' at {items_at:#06x}: header 0x{v[items_at]:02x} (ARRAY), size {A['size']}, "
      f"offset size {A['offset_size']}, offsets {A['offsets']}")
print("decoded:", decode(v, md["keys"]))
print("one dictionary for all levels: 'sku' and 'qty' appear once in the metadata, used by 2 objects")

lines = []
for f in sorted(EVENTS.glob("*.json")):
    lines += [l for l in f.read_text().splitlines() if l.strip()]
ok = bad = 0
for r in spark.createDataFrame([(l,) for l in lines], "j string") \
        .select("j", F.expr("try_parse_json(j)").alias("v")).collect():
    if r.v is None:
        bad += 1
        continue
    mine = decode(bytes(r.v.value), parse_metadata(bytes(r.v.metadata))["keys"])
    ok += mine == json.loads(r.j, **json_number_hook())
print(f"\nreader vs json.loads on every line: {ok} equal, {len(lines) - ok - bad} different, "
      f"{bad} not JSON (try_parse_json gave NULL)")
