"""Step 3: what is inside one ArrowBatch.data: an Arrow IPC stream with a schema and a record batch."""
import os
import struct

import pyarrow as pa
from miniconnect import Session, relations_pb2

s = Session(os.environ.get("SPARK_REMOTE", "sc://localhost:15106"))
rel = relations_pb2.Relation(sql=relations_pb2.SQL(query="SELECT 42L AS answer"))
data = next(r.arrow_batch.data for r in s.responses(rel) if r.WhichOneof("response_type") == "arrow_batch")
print(f"ArrowBatch.data: {len(data)} bytes")
i = 0
while i < len(data):
    cont, meta_len = struct.unpack_from("<Ii", data, i)
    if meta_len == 0:
        print(f"  @{i:03x} {data[i:i + 8].hex(' ')}  end of stream")
        break
    msg = pa.ipc.read_message(pa.py_buffer(data[i:]))
    print(f"  @{i:03x} {data[i:i + 8].hex(' ')}  continuation 0x{cont:08x}, metadata {meta_len} bytes:"
          f" {msg.type} message, body {msg.body.size if msg.body else 0} bytes")
    if msg.body and msg.body.size:
        print(f"       body: {msg.body.to_pybytes().hex(' ')}")
    i += 8 + meta_len + (msg.body.size if msg.body else 0)
with pa.ipc.open_stream(data) as reader:
    print(reader.schema)
    print(reader.read_all().to_pydict())
s.close()
