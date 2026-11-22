"""Step 2: open a gRPC channel, send ExecutePlan with a SQL relation, read the response stream."""
import os

import pyarrow as pa
from miniconnect import Session, relations_pb2

s = Session(os.environ.get("SPARK_REMOTE", "sc://localhost:15106"))
query = "SELECT id, id * id AS square FROM range(5)"
rel = relations_pb2.Relation(sql=relations_pb2.SQL(query=query))
print(f"ExecutePlan: {query}")
batches = []
for n, r in enumerate(s.responses(rel)):
    kind = r.WhichOneof("response_type")
    extra = ""
    if kind == "arrow_batch":
        extra = f"row_count={r.arrow_batch.row_count} data={len(r.arrow_batch.data)} bytes"
        with pa.ipc.open_stream(r.arrow_batch.data) as reader:
            batches.extend(reader)
    elif r.HasField("metrics"):
        extra = f"{len(r.metrics.metrics)} plan nodes with metrics"
    elif kind == "execution_progress":
        extra = f"stages={len(r.execution_progress.stages)}"
    if r.HasField("schema"):
        extra += f" schema={[f.name for f in r.schema.struct.fields]}"
    print(f"  response {n}: {kind or '(none)':<18} op={r.operation_id[:8]} {extra}")
table = pa.Table.from_batches(batches)
print(table)
s.close()
