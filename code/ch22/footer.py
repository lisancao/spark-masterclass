"""Chapter 22: a Parquet file, byte by byte: the magic, the footer length, and the Thrift footer decoded by hand.

Run from code/:  uv run python ch22/footer.py
"""
import json, shutil, struct, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.parquet as pq
from common.book import session, ZONES, TRIPS
sys.path.insert(0, str(Path(__file__).resolve().parent))
from thrift_compact import Compact, footer_bytes, page_headers, TYPES, CODECS, ENCODINGS

OUT = Path("/tmp/masterclass-ch22/zones")
shutil.rmtree(OUT, ignore_errors=True)
spark = session("ch22-footer", spark_sql_session_timeZone="UTC")
zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
zones.coalesce(1).write.parquet(str(OUT))
path = next(OUT.glob("*.parquet"))
data = path.read_bytes()


def hexdump(buf, start, n):
    for off in range(start, start + n, 16):
        row = buf[off:off + 16]
        hx = " ".join(f"{b:02x}" for b in row)
        txt = "".join(chr(b) if 32 <= b < 127 else "." for b in row)
        print(f"  {off:08x}  {hx:<47}  {txt}")


print(f"{path.name[:20]}...: {len(data):,} bytes, {zones.count()} rows, columns {zones.columns}")
print("\nthe first 16 bytes:")
hexdump(data, 0, 16)
print("\nthe last 32 bytes:")
hexdump(data, len(data) - 32, 32)
(length,) = struct.unpack("<I", data[-8:-4])
print(f"\nlast 4 bytes {data[-4:]!r}; the 4 bytes before them, little-endian: footer length {length:,}")
print(f"footer starts at byte {len(data) - 8 - length:,}; column data is bytes 4 to {len(data) - 8 - length - 1:,}")

meta = Compact(footer_bytes(path)).struct()
print("\nFileMetaData, decoded by hand (field id: meaning):")
print(f"  1 version: {meta[1]}   3 num_rows: {meta[3]}   6 created_by: {meta[6].decode()}")
print("  2 schema:")
for el in meta[2]:
    name = el[4].decode()
    kind = TYPES.get(el.get(1), "group")
    rep = {0: "required", 1: "optional", 2: "repeated"}.get(el.get(3), "")
    extra = f", {el[5]} children" if 5 in el else ""
    print(f"      {name:<14}{kind:<12}{rep}{extra}")
for i, rg in enumerate(meta[4]):
    print(f"  4 row_groups[{i}]: num_rows {rg[3]}, total_byte_size {rg[2]:,}")
    for chunk in rg[1]:
        md = chunk[3]
        st = md.get(12, {})
        lo, hi = st.get(6), st.get(5)
        dec = (lambda b: struct.unpack("<i", b)[0]) if TYPES[md[1]] == "INT32" else (lambda b: b.decode()[:16])
        print(f"      {md[3][0].decode():<13}{TYPES[md[1]]:<11}{CODECS[md[4]]:<8}"
              f"{','.join(ENCODINGS[e] for e in md[2]):<28}{md[7]:>6,} B  "
              f"min {dec(lo) if lo else None!s:<16} max {dec(hi) if hi else None}")
kv = {e[1].decode(): e[2].decode() for e in meta.get(5, [])}
print("  5 key_value_metadata keys:", sorted(kv), "| org.apache.spark.version:", kv["org.apache.spark.version"])
spark_schema = json.loads(kv["org.apache.spark.sql.parquet.row.metadata"])
print("    org.apache.spark.sql.parquet.row.metadata:", json.dumps(spark_schema["fields"][0]))

print("\nthe pages of each column chunk (PageHeader structs, read from the chunk offsets):")
for chunk in meta[4][0][1]:
    md = chunk[3]
    pages = page_headers(path, md)
    print(f"  {md[3][0].decode():<13}data_page_offset {md[9]:>5}, dictionary_page_offset {str(md.get(11)):>5}: "
          + "; ".join(f"{t} {e} {n} values {b} B" for t, e, n, b in pages))

print("\nthe same footer through pyarrow:")
md = pq.ParquetFile(path).metadata
print(f"  rows {md.num_rows}, row groups {md.num_row_groups}, created_by {md.created_by}")
c = md.row_group(0).column(0)
print(f"  {c.path_in_schema}: {c.compression}, {c.encodings}, stats min {c.statistics.min} max {c.statistics.max}")

print("\nthe June trips file (written by another program):")
md = pq.ParquetFile(next(Path(TRIPS).glob("*.parquet"))).metadata
print(f"  created_by {md.created_by}; {md.num_rows:,} rows; {md.num_row_groups} row groups; "
      f"rows per group {[md.row_group(i).num_rows for i in range(md.num_row_groups)]}")
print(f"  footer {Compact.footer_len(next(Path(TRIPS).glob('*.parquet'))):,} bytes; "
      f"compression {md.row_group(0).column(0).compression}")
