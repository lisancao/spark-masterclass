"""Step 3: Kafka as a batch source. A range of offsets is a normal DataFrame.

Run from code/:  uv run python ch53/k3_batch.py   (after ch53/k2_start.py: topic `trips` has 6 partitions)
"""
from pyspark.sql import functions as F
from k89 import spark_session, end_offsets, publish, slices, trip_rows, j, PLAIN, SCHEMA


def error_text(e, width=80, lines=2):
    """The whole message on one line (some Kafka messages start with a newline), wrapped."""
    import textwrap
    msg = " ".join(str(e).split())
    w = textwrap.wrap(f"{type(e).__name__}: {msg}", width)
    return "\n".join("  " + x for x in w[:lines]) + (" ..." if len(w) > lines else "")

spark = spark_session("ch53-batch")
ends = end_offsets("trips")
print("end offsets:", ends)


def read(**opts):
    r = spark.read.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "trips")
    for k, v in opts.items():
        r = r.option(k, v)
    return r.load()


def per_partition(df):
    rows = (df.groupBy("partition").agg(F.count("*").alias("n"), F.min("offset").alias("lo"), F.max("offset").alias("hi"))
            .orderBy("partition").collect())
    return "  ".join(f"p{r.partition}:{r.lo}-{r.hi}" for r in rows) + f"  ({sum(r.n for r in rows)} rows)"


print("\n1. Default range, earliest to latest:")
print("  ", per_partition(read()))

print("\n2. startingOffsets and endingOffsets as JSON (end offset not included):")
rng = read(startingOffsets=j({"trips": {"0": 0, "1": 100, "2": -2, "3": -2, "4": -2, "5": -2}}),
           endingOffsets=j({"trips": {"0": 50, "1": 150, "2": 10, "3": 10, "4": 10, "5": -1}}))
print("  ", per_partition(rng))

print("\n3. The bytes become trips with from_json, as in a stream:")
trips = rng.select("partition", "offset", F.from_json(F.col("value").cast("string"), SCHEMA).alias("t")).select("partition", "offset", "t.*")
trips.groupBy("partition").agg(F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
                               F.min("pickup_ts").alias("first_pickup")).orderBy("partition").show(truncate=False)

print("4. Spark resolves latest (and -1) again at each action:")
everything = read()
c1, f1 = everything.count(), rng.count()
n = publish(spark, "trips", trip_rows(slices()[8:9]))
print(f"   before: latest range {c1}, JSON range (p5 ends at -1) {f1}")
print(f"   {n} trips published")
print(f"   after:  latest range {everything.count()}, JSON range (p5 ends at -1) {rng.count()}")

print("\n5. Options that one mode does not accept:")
cases = [
    ("batch, startingOffsets=latest", lambda: read(startingOffsets="latest").count()),
    ("batch, endingOffsets=earliest", lambda: read(endingOffsets="earliest").count()),
    ("batch, end before start", lambda: read(startingOffsets=j({"trips": {"0": 50, "1": -2, "2": -2, "3": -2, "4": -2, "5": -2}}),
                                             endingOffsets=j({"trips": {"0": 10, "1": -1, "2": -1, "3": -1, "4": -1, "5": -1}})).count()),
    ("batch, maxOffsetsPerTrigger", lambda: read(maxOffsetsPerTrigger="100").count()),
    ("stream, endingOffsets", lambda: spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN)
     .option("subscribe", "trips").option("endingOffsets", "latest").load()),
    ("kafka.auto.offset.reset", lambda: read(**{"kafka.auto.offset.reset": "earliest"}).count()),
    ("kafka.enable.auto.commit", lambda: read(**{"kafka.enable.auto.commit": "true"}).count()),
    ("kafka.value.deserializer", lambda: read(**{"kafka.value.deserializer": "x"}).count()),
]
for label, f in cases:
    try:
        r = f()
        print(f"   {label:<32} accepted ({r if isinstance(r, int) else 'no error'})")
    except Exception as e:
        print(f"   {label:<32}" + error_text(e, 80, 2).strip())
spark.stop()
