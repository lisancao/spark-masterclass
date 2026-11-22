"""Chapter 19: the protocol from its own descriptors, the protobuf classes that ship in pyspark 4.3.0.

Run from code/:  uv run python ch19/catalog.py
"""
from pyspark.sql.connect.proto import base_pb2, commands_pb2, expressions_pb2, relations_pb2

print("== The message types that a Plan can hold")
for msg in (base_pb2.Plan, relations_pb2.Relation, expressions_pb2.Expression, commands_pb2.Command):
    oneof = msg.DESCRIPTOR.oneofs[0]
    names = [f.name for f in oneof.fields]
    print(f"{msg.DESCRIPTOR.full_name:26} {oneof.name:12} {len(names):>3} kinds: {', '.join(names[:6])}, ...")

print("\n== The gRPC service spark.connect.SparkConnectService")
svc = base_pb2.DESCRIPTOR.services_by_name["SparkConnectService"]
for m in svc.methods:
    kind = ("stream " if m.client_streaming else "") + m.input_type.name + " -> " + \
        ("stream " if m.server_streaming else "") + m.output_type.name
    print(f"  {m.name:18} {kind}")

print("\n== What an ExecutePlanResponse can carry (response_type)")
r = base_pb2.ExecutePlanResponse.DESCRIPTOR
print("  " + ", ".join(f.name for f in r.oneofs_by_name["response_type"].fields))
print("  and, outside the oneof: " + ", ".join(f.name for f in r.fields if f.containing_oneof is None))
