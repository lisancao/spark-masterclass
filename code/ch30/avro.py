"""Chapter 30, part 2: Avro. The package, Avro files and their header, codecs, type mapping, schema
evolution in a folder, from_avro/to_avro byte by byte, and the Kafka (Confluent) wire format.

Run from code/:  uv run python ch30/avro.py
"""
import base64
import glob
import json
import re
import shutil
import struct
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window, functions as F
from common.book import session, show, plan, MESSY, WORK, TRIPS

PKGS = "org.apache.spark:spark-avro_2.13:4.3.0"
spark = session("ch30-avro", spark_sql_session_timeZone="UTC", spark_jars_packages=PKGS)
from pyspark.sql.avro.functions import from_avro, to_avro  # noqa: E402

BIN = MESSY / "binary"
TMP = WORK / "ch30" / "avro"
shutil.rmtree(TMP, ignore_errors=True)
TMP.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_0-9.]+)\]", str(e))
    if m:
        return m.group(1)
    for name in ("IncompatibleSchemaException", "AvroTypeException", "EOFException", "ArrayIndexOutOfBounds",
                 "NegativeArraySize", "SparkException"):
        if name in str(e):
            return name
    return f"{type(e).__name__}: {str(e).splitlines()[0][:60]}"


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label:46} -> {r}")
    except Exception as e:
        print(f"  {label:46} -> {condition(e)}")


section("an Avro file: the header carries the schema")
orders = spark.createDataFrame(
    [("O-1", 7, 12.5, None), ("O-2", 8, 30.0, "SAVE5")],
    "order_id STRING NOT NULL, user_id BIGINT NOT NULL, total DOUBLE, coupon STRING")
out = str(TMP / "orders")
orders.coalesce(1).write.format("avro").save(out)
f = glob.glob(f"{out}/*.avro")[0]
raw = open(f, "rb").read()
print("first 4 bytes:", raw[:4], " file size:", len(raw), "bytes")
start = raw.index(b'{"type"')
schema_json = json.dumps(json.JSONDecoder().raw_decode(raw[start:].decode("latin-1"))[0], separators=(",", ":"))
print("avro.schema in the header:", schema_json)
print("other header keys:", [k.decode() for k in re.findall(rb"(org\.apache\.spark\.version|avro\.codec)", raw)],
      "codec:", re.search(rb"avro\.codec.(\w+)", raw).group(1).decode())
show(spark.read.format("avro").load(out))

section("Spark types to Avro types, and back")
print(spark.sql("""SELECT schema_of_avro('{"type":"record","name":"r","fields":[
  {"name":"a","type":"int"}, {"name":"b","type":["null","string"]},
  {"name":"c","type":{"type":"long","logicalType":"timestamp-millis"}},
  {"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
  {"name":"e","type":{"type":"enum","name":"E","symbols":["X","Y"]}},
  {"name":"f","type":["int","string"]},
  {"name":"g","type":{"type":"map","values":"long"}}]}', map())""").first()[0])
attempt("write a VARIANT column to Avro", lambda: spark.sql("SELECT parse_json('{}') AS v")
        .write.mode("overwrite").format("avro").save(str(TMP / "v")))
attempt("from_avro on a union without stable names", lambda: spark.sql(
    """SELECT schema_of_avro('["int","string"]', map())""").first()[0])
attempt("  with enableStableIdentifiersForUnionType", lambda: spark.sql(
    """SELECT schema_of_avro('["int","string"]', map('enableStableIdentifiersForUnionType', 'true'))""").first()[0])

section("compression codecs on one million taxi trips")
trips = spark.read.parquet(str(TRIPS)).where(F.col("_metadata.row_index") < 1_000_000)
for codec in ("uncompressed", "snappy", "deflate", "zstandard", "xz"):
    p = TMP / f"trips_{codec}"
    trips.write.format("avro").option("compression", codec).save(str(p))
    mb = sum(x.stat().st_size for x in p.glob("*.avro")) / 1e6
    print(f"  {codec:13} {mb:7.1f} MB")
pq = TMP / "trips_parquet"
trips.write.parquet(str(pq))
print(f"  {'(parquet)':13} {sum(x.stat().st_size for x in pq.glob('*.parquet')) / 1e6:7.1f} MB")
attempt("compression=lz5", lambda: trips.limit(1).write.format("avro").option("compression", "lz5")
        .save(str(TMP / "bad_codec")))

section("write with an Avro schema of your own")
v2 = open(BIN / "schemas" / "orders-value-2.avsc").read()
enum_schema = json.dumps({"type": "record", "name": "Pay", "fields": [
    {"name": "method", "type": {"type": "enum", "name": "M", "symbols": ["CARD", "CASH"]}}]})
attempt("method='CARD' into an enum", lambda: spark.createDataFrame([("CARD",)], "method STRING NOT NULL")
        .write.mode("overwrite").format("avro").option("avroSchema", enum_schema).save(str(TMP / "e1")) or "written")
attempt("method='BITCOIN' into an enum", lambda: spark.createDataFrame([("BITCOIN",)], "method STRING NOT NULL")
        .write.mode("overwrite").format("avro").option("avroSchema", enum_schema).save(str(TMP / "e2")))

section("a folder with two versions of the schema")
v1 = open(BIN / "schemas" / "orders-value-1.avsc").read()
d1 = spark.createDataFrame([("O-1", 1, 10.0, 1_790_000_000_000)], "order_id STRING NOT NULL, user_id BIGINT NOT NULL, amount DOUBLE NOT NULL, ts BIGINT NOT NULL") \
    .withColumn("ts", F.timestamp_millis("ts"))
d1.write.format("avro").option("avroSchema", v1).save(str(TMP / "mixed" / "day=1"))
d2 = spark.createDataFrame([("O-2", 2, 20.0, 1_790_000_000_000, "EUR", None, [("P1", 2)])],
                           "order_id STRING NOT NULL, user_id BIGINT NOT NULL, total DOUBLE NOT NULL, ts BIGINT NOT NULL, "
                           "currency STRING NOT NULL, coupon STRING, items ARRAY<STRUCT<sku: STRING NOT NULL, qty: INT NOT NULL>> NOT NULL") \
    .withColumn("ts", F.timestamp_millis("ts"))
d2.write.format("avro").option("avroSchema", v2).save(str(TMP / "mixed" / "day=2"))
m = spark.read.format("avro").load(str(TMP / "mixed"))
print("schema from one file:", m.columns)
print("mergeSchema=true:    ", spark.read.format("avro").option("mergeSchema", "true").load(str(TMP / "mixed")).columns)
show(spark.read.format("avro").option("avroSchema", v2).load(str(TMP / "mixed"))
     .select("order_id", "total", "currency", "coupon", "items", "day").orderBy("order_id"))

section("Avro schema resolution: what a reader schema may change")
w = json.dumps({"type": "record", "name": "R", "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]})
enc = spark.sql("SELECT named_struct('a', 7, 'b', 'x') AS s").select(to_avro("s", w).alias("bin"))


def reader(fields):
    return json.dumps({"type": "record", "name": "R", "fields": fields})


cases = [
    ("same schema", [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]),
    ("add a field with a default", [{"name": "a", "type": "int"}, {"name": "b", "type": "string"},
                                    {"name": "c", "type": "string", "default": "CAD"}]),
    ("add a nullable field, default null", [{"name": "a", "type": "int"}, {"name": "b", "type": "string"},
                                            {"name": "c", "type": ["null", "string"], "default": None}]),
    ("add a field without a default", [{"name": "a", "type": "int"}, {"name": "b", "type": "string"},
                                       {"name": "c", "type": "string"}]),
    ("remove field b", [{"name": "a", "type": "int"}]),
    ("rename b to c, alias b", [{"name": "a", "type": "int"}, {"name": "c", "type": "string", "aliases": ["b"]}]),
    ("promote a: int to long", [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]),
    ("promote a: int to double", [{"name": "a", "type": "double"}, {"name": "b", "type": "string"}]),
    ("narrow: read int a as a string", [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]),
]
for label, fields in cases:
    attempt(label, lambda fields=fields: enc.select(from_avro("bin", w, {"avroSchema": reader(fields)}).alias("r"))
            .first().r.asDict())

section("from_avro and to_avro: the bytes of one record")
one = spark.sql("SELECT named_struct('order_id', 'O-1', 'user_id', 7L, 'total', 12.5D, 'coupon', CAST(NULL AS STRING)) AS s")
w2 = json.dumps({"type": "record", "name": "O", "fields": [
    {"name": "order_id", "type": "string"}, {"name": "user_id", "type": "long"},
    {"name": "total", "type": "double"}, {"name": "coupon", "type": ["null", "string"]}]})
b = one.select(to_avro("s", w2).alias("b")).first().b
print("to_avro:", len(b), "bytes:", b.hex(" "))
print("  06 'O-1'               string: length 3 as a zigzag varint (06), then the UTF-8 bytes")
print("  0e                     long 7 as a zigzag varint (7 * 2 = 14 = 0x0e)")
print("  00 00 00 00 00 00 29 40 double 12.5, 8 bytes little-endian")
print("  00                     union branch 0 (null), no more bytes")
print("the same record as JSON:", len(one.select(F.to_json("s")).first()[0]), "bytes")

section("decode with the wrong schema")
bytes_df = one.select(to_avro("s", w2).alias("b"))
wrong = {
    "user_id as int (same bytes)": [{"name": "order_id", "type": "string"}, {"name": "user_id", "type": "int"},
                                    {"name": "total", "type": "double"}, {"name": "coupon", "type": ["null", "string"]}],
    "fields in another order": [{"name": "user_id", "type": "long"}, {"name": "order_id", "type": "string"},
                                {"name": "total", "type": "double"}, {"name": "coupon", "type": ["null", "string"]}],
    "total as float": [{"name": "order_id", "type": "string"}, {"name": "user_id", "type": "long"},
                       {"name": "total", "type": "float"}, {"name": "coupon", "type": ["null", "string"]}],
    "one extra string field at the end": [{"name": "order_id", "type": "string"}, {"name": "user_id", "type": "long"},
                                          {"name": "total", "type": "double"}, {"name": "coupon", "type": ["null", "string"]},
                                          {"name": "x", "type": "string"}],
}
for label, fields in wrong.items():
    attempt(label, lambda fields=fields: bytes_df.select(from_avro("b", reader(fields)).alias("r")).first().r)
for mode in ("FAILFAST", "PERMISSIVE", "DROPMALFORMED"):
    attempt(f"garbage bytes, mode={mode}", lambda mode=mode: spark.sql("SELECT X'FFFFFFFF' AS b")
            .select(from_avro("b", w2, {"mode": mode}).alias("r")).first().r)

section("the Kafka wire format: a Confluent header in front of the Avro bytes")
topic = (spark.read.schema("topic STRING, partition INT, offset BIGINT, key STRING, value STRING")
         .json(str(BIN / "orders_topic.jsonl")).withColumn("value", F.unbase64("value")))
first = topic.where("offset = 0").first().value
print("offset 0:", first.hex(" "))
print("  00          magic byte")
print("  00 00 00 01 schema id 1, 4 bytes big-endian")
print("  0c ...      the Avro record of schema 1")
framed = topic.select("*",
                      (F.hex(F.substring("value", 1, 1)) == "00").alias("framed"),
                      F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int").alias("schema_id"),
                      F.expr("substring(value, 6)").alias("payload"))
show(framed.groupBy("framed", "schema_id").agg(F.count("*").alias("messages"),
                                               F.min("offset").alias("first_offset")).orderBy("schema_id"))
attempt("from_avro on the full value (header included)",
        lambda: topic.where("offset = 0").select(from_avro("value", v1).alias("o")).first().o)

schemas = {1: v1, 2: v2}
decoded = None
for sid, writer in sorted(schemas.items()):
    branch = from_avro("payload", writer, {"avroSchema": v2, "mode": "PERMISSIVE"})
    decoded = F.when(F.col("schema_id") == sid, branch) if decoded is None else decoded.when(F.col("schema_id") == sid, branch)
classified = (framed.withColumn("o", decoded)
              .withColumn("problem", F.when(F.col("value").isNull(), "tombstone")
                          .when(~F.col("framed"), "no confluent header")
                          .when(~F.col("schema_id").isin(*schemas), "unknown schema id")
                          .when(F.col("o.order_id").isNull(), "corrupt")))
show(classified.groupBy("problem").agg(F.count("*").alias("messages"),
                                       F.collect_list("offset").alias("offsets")).orderBy("problem"))
latest = Window.partitionBy("key").orderBy(F.col("offset").desc())
current = (classified.where("problem IS NULL OR problem = 'tombstone'")
           .withColumn("rn", F.row_number().over(latest))
           .where("rn = 1 AND problem IS NULL")
           .select("o.order_id", "o.user_id", "o.total", "o.currency", "o.coupon",
                   F.size("o.items").alias("n_items"), "schema_id"))
print("current orders:", current.count(), " keys in the topic:", topic.select("key").distinct().count())
show(current.orderBy("order_id"), 4)
