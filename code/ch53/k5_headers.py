"""Step 5: the sink's columns (key, value, headers, topic, partition) and headers on the source.

A batch job writes trips with two headers, routes each trip to a topic by borough with a
`topic` column, and pins airport trips to partition 0. A reader then filters on a header.
Run from code/:  uv run python ch53/k5_headers.py   (after ch53/kafka.sh start)
"""
from pyspark.sql import functions as F
from k89 import spark_session, kafka, slices, trip_rows, error_text, ZONES_CSV, PLAIN, SCHEMA

spark = spark_session("ch53-headers")
for t in ("trips_manhattan", "trips_other"):
    kafka("topic", t, "3")
zones = spark.read.option("header", True).csv(str(ZONES_CSV)).select(F.col("LocationID").cast("int").alias("PULocationID"), "Borough")
lines = trip_rows(slices()[:2])
trips = (spark.createDataFrame([(l,) for l in lines], "json string")
         .select("json", F.from_json("json", SCHEMA).alias("t")).select("json", "t.*")
         .join(zones, "PULocationID"))

out = trips.select(
    F.col("trip_id").alias("key"),                                         # (1) the key
    F.col("json").alias("value"),                                          # (2) the value
    F.array(F.struct(F.lit("schema").alias("key"), F.lit("trip-v1").cast("binary").alias("value")),
            F.struct(F.lit("borough").alias("key"), F.col("Borough").cast("binary").alias("value"))
            ).alias("headers"),                                            # (3) headers
    F.when(F.col("Borough") == "Manhattan", "trips_manhattan").otherwise("trips_other").alias("topic"),
    F.when(F.col("PULocationID").isin(132, 138), F.lit(0)).alias("partition"))   # airports -> p0, else null
print("1. The DataFrame for the sink:")
out.printSchema()
out.write.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("includeHeaders", "true").save()
print(f"   wrote {out.count()} records, no `topic` option: each row names its topic")

print("\n2. Read both topics back with includeHeaders=true:")
src = (spark.read.format("kafka").option("kafka.bootstrap.servers", PLAIN)
       .option("subscribe", "trips_manhattan,trips_other").option("includeHeaders", "true").load())
src.groupBy("topic", "partition").count().orderBy("topic", "partition").show()
print("   one record:")
src.select("topic", "partition", "offset", F.col("key").cast("string").alias("key"),
           F.transform("headers", lambda h: F.concat(h.key, F.lit("="), h.value.cast("string"))).alias("headers")
           ).orderBy("offset").limit(1).show(truncate=False)

print("3. Filter on a header without parsing the value:")
borough = F.expr("cast(filter(headers, h -> h.key = 'borough')[0].value as string)")
src.where(borough == "Queens").groupBy("partition").count().orderBy("partition").show()

print("4. The airport trips went to partition 0 of their topic, by the `partition` column:")
airports = (src.select("topic", "partition", F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"))
            .where(F.col("t.PULocationID").isin(132, 138)))
airports.groupBy("topic", "partition").count().orderBy("topic").show()

print("5. Sink errors:")
bad = [
    ("no value column", out.drop("value")),
    ("value as an int", out.withColumn("value", F.lit(1))),
    ("headers, wrong type", out.withColumn("headers", F.lit("x"))),
]
for label, df in bad:
    try:
        df.write.format("kafka").option("kafka.bootstrap.servers", PLAIN).save()
        print(f"   {label:<22} accepted")
    except Exception as e:
        print(f"   {label:<22}" + error_text(e, 84, 2).strip())
try:
    out.write.format("kafka").option("kafka.bootstrap.servers", PLAIN).mode("overwrite").save()
except Exception as e:
    print(f"   {'mode(overwrite)':<22}" + error_text(e, 84, 2).strip())
spark.stop()
