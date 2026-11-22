"""Chapter 80: a plan as a tree, as protobuf text and as bytes, and an Arrow batch from a response.

Run ch80/protocol.py first: it saves the raw messages in data/ch80-wire/.
Run from code/:  uv run python ch80/wire.py
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa
from google.protobuf.descriptor import FieldDescriptor as FD
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.connect.proto import base_pb2 as pb
from common.book import ROOT, TRIPS

WIRE = ROOT / "data" / "ch80-wire"
PORT = os.environ.get("CONNECT_PORT", "15002")


def compact(msg):
    """Remove the bookkeeping on every node (plan IDs, Python call sites) before printing."""
    from google.protobuf.message import Message
    if "common" in msg.DESCRIPTOR.fields_by_name:
        msg.ClearField("common")
    for field, value in msg.ListFields():
        for v in (value if getattr(field, "is_repeated", False) else [value]):
            if isinstance(v, Message):
                compact(v)
    return msg


def tree(rel, depth=0):
    """The relations of a plan, one per line, with the expressions that each one holds."""
    kind = rel.WhichOneof("rel_type")
    node = getattr(rel, kind)
    detail = {
        "read": lambda n: f"format={n.data_source.format}",
        "filter": lambda n: expr(n.condition),
        "project": lambda n: ", ".join(expr(e) for e in n.expressions),
        "limit": lambda n: f"limit={n.limit}",
    }.get(kind, lambda n: "")(node)
    print(f"{'  ' * depth}{kind:8} {detail}")
    if node.DESCRIPTOR.fields_by_name.get("input"):
        tree(node.input, depth + 1)


def expr(e):
    kind = e.WhichOneof("expr_type")
    if kind == "unresolved_attribute":
        return e.unresolved_attribute.unparsed_identifier
    if kind == "literal":
        lit = e.literal
        return repr(getattr(lit, lit.WhichOneof("literal_type")))
    if kind == "unresolved_function":
        f = e.unresolved_function
        return f"{f.function_name}({', '.join(expr(a) for a in f.arguments)})"
    return kind


def varint(buf, i):
    shift = value = 0
    while True:
        b = buf[i]; i += 1
        value |= (b & 0x7F) << shift
        shift += 7
        if b < 0x80:
            return value, i


def walk(buf, desc, depth=0, start=0, end=None, max_depth=4):
    """Print each field of a serialized message: offset, key bytes, field name, wire type, value."""
    i, end = start, len(buf) if end is None else end
    while i < end:
        key_at = i
        key, i = varint(buf, i)
        num, wt = key >> 3, key & 7
        f = desc.fields_by_number.get(num) if desc else None
        name = f.name if f else f"field {num}"
        key_hex = buf[key_at:i].hex(" ")
        if wt == 0:
            v, i = varint(buf, i)
            print(f"{key_at:5}  {key_hex:8} {'  ' * depth}{name} = {v}")
        elif wt == 2:
            n, j = varint(buf, i)
            body = buf[j:j + n]
            if f is not None and f.type == FD.TYPE_MESSAGE and depth < max_depth:
                print(f"{key_at:5}  {key_hex:8} {'  ' * depth}{name} {{ {n} bytes")
                walk(buf, f.message_type, depth + 1, j, j + n, max_depth)
            elif f is not None and f.type == FD.TYPE_STRING:
                print(f"{key_at:5}  {key_hex:8} {'  ' * depth}{name} = {body.decode()[:46]!r}")
            else:
                print(f"{key_at:5}  {key_hex:8} {'  ' * depth}{name}: {n} bytes")
            i = j + n
        elif wt in (1, 5):
            i += 8 if wt == 1 else 4
            print(f"{key_at:5}  {key_hex:8} {'  ' * depth}{name} (fixed)")
        else:
            raise ValueError(f"wire type {wt} at {key_at}")


def hexdump(data, start=0, end=None, width=16):
    end = len(data) if end is None else end
    for off in range(start, end, width):
        chunk = data[off:min(off + width, end)]
        text = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        print(f"  {off:04x}  {chunk.hex(' '):<{width * 3}} {text}")


with SparkSession.builder.remote(f"sc://localhost:{PORT}").create() as spark:
    df = (spark.read.parquet(str(TRIPS))
          .where(F.col("fare_amount") > 100)
          .select("PULocationID", "fare_amount")
          .limit(5))
    plan = df._plan.to_proto(spark.client)          # a spark.connect.Plan, built in the client

    print("== 1. The plan as a tree of relations")
    tree(plan.root)

    print("\n== 2. The same plan as protobuf text (plan IDs removed)")
    small = pb.Plan(); small.CopyFrom(plan)
    print(compact(small))

    raw = plan.SerializeToString()
    print(f"== 3. The same plan as bytes: {len(raw)} bytes, field by field")
    walk(raw, pb.Plan.DESCRIPTOR, max_depth=6)

# What the proxy saw: the request around the plan, and the first Arrow batch that came back.
req = pb.ExecutePlanRequest.FromString((WIRE / "call5-ExecutePlanRequest.bin").read_bytes())
print("\n== 4. The ExecutePlanRequest of df.collect(), without the plan")
req.ClearField("plan")
print(req)

resp = (WIRE / "call5-response.bin").read_bytes()
print(f"== 5. The ExecutePlanResponse that carried the rows: {len(resp)} bytes")
walk(resp, pb.ExecutePlanResponse.DESCRIPTOR, max_depth=1)

data = (WIRE / "call5-arrow-batch.bin").read_bytes()
print(f"\n== 6. arrow_batch.data: an Arrow IPC stream of {len(data)} bytes")
reader = pa.ipc.open_stream(data)
print("schema:", reader.schema.to_string(show_schema_metadata=False).replace("\n", ", "))
batch = reader.read_next_batch()
print("batch: ", batch.num_rows, "rows,", batch.to_pydict())

print("\nThe messages in the stream (continuation marker, metadata length, metadata, body):")
off = 0
stream = pa.BufferReader(data)
while True:
    at = stream.tell()
    msg = pa.ipc.read_message(stream) if at < len(data) - 8 else None
    if msg is None:
        print(f"  @{at:<4} end-of-stream marker: {data[at:].hex(' ')}")
        break
    meta_len = int.from_bytes(data[at + 4:at + 8], "little")
    print(f"  @{at:<4} {msg.type:13} marker {data[at:at + 4].hex()}  metadata {meta_len} B"
          f"  body {msg.body.size} B")
    if msg.type == "record batch":
        body_at = at + 8 + meta_len
        rb_body = (body_at, body_at + msg.body.size)

print("\nThe start of the stream (schema message):")
hexdump(data, 0, 64)
print("\nThe record batch body (the three rows, column by column):")
hexdump(data, *rb_body)
