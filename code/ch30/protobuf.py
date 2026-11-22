"""Chapter 30, part 3: Protobuf and Kafka. Descriptor sets, from_protobuf and its options, the
Protobuf wire format, proto3 default values, the Confluent header with message indexes, a dead
letter table for both topics, and the schema of the real Kafka source.

Run from code/:  uv run python ch30/protobuf.py
"""
import base64
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY

PKGS = ("org.apache.spark:spark-protobuf_2.13:4.3.0,org.apache.spark:spark-avro_2.13:4.3.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
spark = session("ch30-protobuf", spark_sql_session_timeZone="UTC", spark_jars_packages=PKGS)
from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf  # noqa: E402

BIN = MESSY / "binary"
DESC = str(BIN / "schemas" / "payments.desc")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_0-9.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:60]}"


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label:46} -> {r}")
    except Exception as e:
        print(f"  {label:46} -> {condition(e)}")


section("the descriptor set: protoc output, not the .proto file")
print(open(BIN / "schemas" / "payments.proto").read().strip())
print("payments.desc:", Path(DESC).stat().st_size, "bytes (protoc --include_imports --descriptor_set_out)")

section("encode one payment, and walk the bytes")
pay = spark.sql("""SELECT named_struct('payment_id', 'PAY-1', 'order_id', 'O-1', 'amount_cents', 1250L,
                                       'method', 'CARD', 'flags', array('manual'), 'note', CAST(NULL AS STRING)) AS p""")
b = pay.select(to_protobuf("p", "shop.Payment", DESC).alias("b")).first().b
print(len(b), "bytes:", b.hex(" "))
print("  0a 05 'PAY-1'  field 1, wire type 2 (length): 5 bytes")
print("  12 03 'O-1'    field 2, wire type 2: 3 bytes")
print("  18 e2 09       field 3, wire type 0 (varint): 1250")
print("  20 01          field 4, wire type 0: enum 1 = CARD")
print("  2a 06 'manual' field 5, wire type 2: one element of the repeated string")
print("  (field 6, note, is NULL, so it has no bytes)")

section("proto3 does not write default values")
zero = spark.sql("""SELECT named_struct('payment_id', 'PAY-0', 'order_id', 'O-0', 'amount_cents', 0L,
                                        'method', 'METHOD_UNSPECIFIED', 'flags', CAST(array() AS ARRAY<STRING>),
                                        'note', '') AS p""")
zb = zero.select(to_protobuf("p", "shop.Payment", DESC).alias("b"))
print("bytes:", zb.first().b.hex(" "), " (no amount_cents, method or flags)")
show(zb.select(from_protobuf("b", "shop.Payment", DESC).alias("p")).select("p.*"))
show(zb.select(from_protobuf("b", "shop.Payment", DESC, {"emit.default.values": "true"}).alias("p")).select("p.*"))
show(zb.select(from_protobuf("b", "shop.Payment", DESC, {"enums.as.ints": "true", "emit.default.values": "true"}).alias("p")).select("p.method"))

section("the Confluent header for Protobuf has a message index")
topic = (spark.read.schema("topic STRING, partition INT, offset BIGINT, key STRING, value STRING")
         .json(str(BIN / "payments_topic.jsonl")).withColumn("value", F.unbase64("value")))
print("offset 0:", topic.where("offset = 0").first().value.hex(" ")[:60], "...")
print("  00 | 00 00 00 03 | 00 | 0a 08 ...  magic byte | schema id 3 | message index 0 | Protobuf")
framed = topic.select("*",
                      (F.hex(F.substring("value", 1, 1)) == "00").alias("framed"),
                      F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int").alias("schema_id"),
                      F.expr("substring(value, 7)").alias("payload"))
attempt("from_protobuf on a payload with the index byte", lambda: framed.where("offset = 0")
        .select(from_protobuf(F.expr("substring(value, 6)"), "shop.Payment", DESC).alias("p")).first().p)
opts = {"mode": "PERMISSIVE", "emit.default.values": "true"}
dec = framed.withColumn("p", from_protobuf("payload", "shop.Payment", DESC, opts))
dec = dec.withColumn("problem", F.when(F.col("value").isNull(), "tombstone")
                     .when(~F.col("framed"), "no confluent header")
                     .when(F.col("schema_id") != 3, "unknown schema id")
                     .when(F.col("p").isNull(), "corrupt"))
show(dec.groupBy("problem").agg(F.count("*").alias("messages"), F.collect_list("offset").alias("offsets")).orderBy("problem"))
show(dec.where("problem IS NULL").select("offset", "p.order_id", "p.amount_cents",
                                         F.nullif("p.method", F.lit("METHOD_UNSPECIFIED")).alias("method"),
                                         "p.flags", "p.note").orderBy("offset"), 5)

section("PERMISSIVE: Protobuf gives NULL, Avro gives a struct of NULLs")
attempt("from_protobuf(X'FFFF'), FAILFAST", lambda: spark.sql("SELECT X'FFFF' AS b")
        .select(from_protobuf("b", "shop.Payment", DESC).alias("p")).first().p)
attempt("from_protobuf(X'FFFF'), PERMISSIVE", lambda: spark.sql("SELECT X'FFFF' AS b")
        .select(from_protobuf("b", "shop.Payment", DESC, {"mode": "PERMISSIVE"}).alias("p")).first().p)
attempt("an unknown message name", lambda: spark.sql("SELECT X'00' AS b")
        .select(from_protobuf("b", "shop.Refund", DESC).alias("p")).first().p)

section("the bytes of a different message type")
other = spark.sql("SELECT X'0a034f2d3112045041592d' AS b")   # field 1 = "O-1", field 2 = "PAY-"
show(other.select(from_protobuf("b", "shop.Payment", DESC).alias("p")).select("p.payment_id", "p.order_id"))
attempt("field 3 sent as a string (wire type 2)", lambda: spark.sql("SELECT X'1a0131' AS b")
        .select(from_protobuf("b", "shop.Payment", DESC).alias("p")).first().p)

section("encode errors")
attempt("method 'BITCOIN' into the enum", lambda: spark.sql(
    """SELECT named_struct('payment_id', 'P', 'order_id', 'O', 'amount_cents', 1L, 'method', 'BITCOIN',
       'flags', array('x'), 'note', 'n') AS p""").select(to_protobuf("p", "shop.Payment", DESC)).first()[0])
attempt("amount_cents as a STRING", lambda: spark.sql(
    """SELECT named_struct('payment_id', 'P', 'order_id', 'O', 'amount_cents', '1', 'method', 'CARD',
       'flags', array('x'), 'note', 'n') AS p""").select(to_protobuf("p", "shop.Payment", DESC)).first()[0])

section("SQL functions, and the binary descriptor instead of a path")
spark.sql("SELECT 1").count()
desc_bytes = Path(DESC).read_bytes()
show(framed.where("offset < 2").select(from_protobuf("payload", "shop.Payment", binaryDescriptorSet=desc_bytes)
                                       .alias("p")).select("p.payment_id", "p.amount_cents"))
show(framed.where("offset < 2").createOrReplaceTempView("pt") or spark.sql(
    f"SELECT from_protobuf(payload, 'shop.Payment', '{DESC}').order_id AS order_id FROM pt").withColumn(
        "note", F.lit("from SQL")))

section("the real Kafka source: its schema, with no broker")
k = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:1")
     .option("subscribe", "payments").load())
k.printSchema()
print("Kafka source options that matter here: startingOffsets, failOnDataLoss, includeHeaders")
