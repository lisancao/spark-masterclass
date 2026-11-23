"""Step 1: build the smallest useful ExecutePlanRequest and read its bytes field by field.

The walker below reads the protobuf wire format by hand (tag = field number << 3 | wire type),
and uses the message descriptors only to name the fields and to know which ones hold messages.
"""
from miniconnect import base_pb2, relations_pb2

req = base_pb2.ExecutePlanRequest(
    session_id="00112233-4455-6677-8899-aabbccddeeff",          # the client chooses it
    user_context=base_pb2.UserContext(user_id="book"),
    plan=base_pb2.Plan(root=relations_pb2.Relation(sql=relations_pb2.SQL(query="SELECT 1 AS x"))),
    client_type="miniconnect/0.1",
)
data = req.SerializeToString()
print(f"ExecutePlanRequest: {len(data)} bytes")
for i in range(0, len(data), 16):
    chunk = data[i:i + 16]
    print(f"  {i:04x}  {chunk.hex(' '):<47}  {''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)}")


def varint(buf, i):
    shift = value = 0
    while True:
        b = buf[i]
        value |= (b & 0x7F) << shift
        i += 1
        if not b & 0x80:
            return value, i
        shift += 7


def walk(buf, desc, depth=0, base=0):
    i = 0
    while i < len(buf):
        start = i
        tag, i = varint(buf, i)
        number, wire = tag >> 3, tag & 7
        field = desc.fields_by_number.get(number)
        name = field.name if field else f"field {number}"
        pad = "  " * depth
        if wire == 2:                                   # length-delimited: bytes, string or message
            length, i = varint(buf, i)
            body = buf[i:i + length]
            head = buf[start:i].hex(" ")
            if field is not None and field.message_type is not None:
                print(f"  @{base + start:02x} {head:<9} {pad}{name} ({field.message_type.name}, {length} bytes)")
                walk(body, field.message_type, depth + 1, base + i)
            else:
                print(f"  @{base + start:02x} {head:<9} {pad}{name} = {body.decode()!r}")
            i += length
        elif wire == 0:
            value, i = varint(buf, i)
            print(f"  @{base + start:02x} {buf[start:i].hex(' '):<9} {pad}{name} = {value}")
        else:
            raise ValueError(f"wire type {wire} is not in this request")


print("\nfield by field (offset, tag and length bytes, field):")
walk(data, base_pb2.ExecutePlanRequest.DESCRIPTOR)
