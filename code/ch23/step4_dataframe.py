"""Step 4: AnalyzePlan for a schema, then the taxi query of Chapter 21 through the DataFrame builder."""
import os
from miniconnect import Session, col, fn

TRIPS = os.path.join(os.environ.get("BOOK_DATA", os.path.expanduser("~/spark-masterclass/data")), "nyc-taxi", "yellow")
s = Session(os.environ.get("SPARK_REMOTE", "sc://localhost:15106"))

trips = s.read_parquet(TRIPS)
schema = trips.schema()                                   # AnalyzePlan: nothing runs
print(f"AnalyzePlan(schema): {len(schema.struct.fields)} fields")
for f in schema.struct.fields[:3]:
    print(f"  {f.name}: {f.data_type.WhichOneof('kind')} nullable={f.nullable}")

pickup = col("tpep_pickup_datetime")
top = (trips.where((pickup >= "2026-06-01") & (pickup < "2026-07-01"))
       .group_by("PULocationID")
       .agg(fn("count", 1).alias("trips"), fn("round", fn("avg", col("fare_amount")), 2).alias("avg_fare"))
       .order_by(col("trips").desc())
       .limit(5))
print(f"\nthe plan is {len(top.relation.SerializeToString())} bytes of protobuf; its node chain:")
node, chain = top.relation, []
while node.WhichOneof("rel_type"):
    kind = node.WhichOneof("rel_type")
    chain.append(kind)
    inner = getattr(node, kind)
    node = inner.input if hasattr(inner, "input") else type(node)()
print("  " + " <- ".join(chain))
table = top.to_arrow()
print(f"\n{table.schema}")
for row in table.to_pylist():
    print(" ", row)
s.close()
