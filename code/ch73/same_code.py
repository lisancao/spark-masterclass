"""Chapter 73: one function, run as a batch query and as a streaming query.

Taxi trips for 1 to 3 June land in an inbox as JSON files, one file for each day. The function
`busiest_zones` does not know if its input is a table or a stream. The batch query reads the
inbox once. The streaming query reads it with Trigger.AvailableNow, then reads it again after a
fourth day lands, and processes only the new file.

Run from code/:  uv run python ch73/same_code.py
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, functions as F
from common.book import session, plan, work, TRIPS

spark = session("ch73-same-code", spark_ui_showConsoleProgress="false",
                spark_sql_shuffle_partitions="4")
w = work("ch73")
inbox, staging = w / "inbox", w / "staging"
inbox.mkdir()

trips = spark.read.parquet(str(TRIPS)).select("tpep_pickup_datetime", "PULocationID", "total_amount")
schema = trips.schema


def land(day):
    """Write one day of trips as one JSON file, then move it into the inbox in one rename."""
    out = staging / f"day={day}"
    (trips.where(F.to_date("tpep_pickup_datetime") == f"2026-06-{day:02d}")
     .coalesce(1).write.mode("overwrite").json(str(out)))
    part = next(out.glob("part-*.json"))
    part.rename(inbox / f"2026-06-{day:02d}.json")


def busiest_zones(df: DataFrame) -> DataFrame:
    """The business logic: trips and revenue for each pickup zone."""
    return (df.where(F.col("total_amount") > 0)
              .groupBy("PULocationID")
              .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue")))


for day in (1, 2, 3):
    land(day)
print("inbox:", sorted(p.name for p in inbox.iterdir()))

batch = busiest_zones(spark.read.schema(schema).json(str(inbox)))
top_batch = batch.orderBy(F.desc("trips")).limit(3).collect()
print("\nbatch, top three zones:")
for r in top_batch:
    print(f"  zone {r.PULocationID:>3}  trips {r.trips:>6,}  revenue {r.revenue:>12,.2f}")

stream_in = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(str(inbox))
query = (busiest_zones(stream_in).writeStream
         .format("memory").queryName("zones").outputMode("complete")
         .option("checkpointLocation", str(w / "checkpoint"))
         .trigger(availableNow=True).start())
query.awaitTermination()
print(f"\nstream, first run: {len(query.recentProgress)} micro-batches")
for p in query.recentProgress:
    print(f"  batch {p['batchId']}: {p['numInputRows']:>7,} input rows")
top_stream = spark.sql("SELECT * FROM zones ORDER BY trips DESC LIMIT 3").collect()
print("stream, top three zones:")
for r in top_stream:
    print(f"  zone {r.PULocationID:>3}  trips {r.trips:>6,}  revenue {r.revenue:>12,.2f}")
print("same rows as the batch query:", [tuple(r) for r in top_stream] == [tuple(r) for r in top_batch])

land(4)
query = (busiest_zones(stream_in).writeStream
         .format("memory").queryName("zones").outputMode("complete")
         .option("checkpointLocation", str(w / "checkpoint"))
         .trigger(availableNow=True).start())
query.awaitTermination()
print("\na fourth day lands; the same query restarts from its checkpoint:")
for p in query.recentProgress:
    print(f"  batch {p['batchId']}: {p['numInputRows']:>7,} input rows")
p = query.lastProgress
print("  state rows:", p["stateOperators"][0]["numRowsTotal"],
      " source startOffset:", p["sources"][0]["startOffset"], " endOffset:", p["sources"][0]["endOffset"])

import contextlib, io
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    query.explain()
print("\nthe physical plan of the last micro-batch:")
print("\n".join(l[:100] for l in buf.getvalue().splitlines() if l.strip()))
print("\nthe physical plan of the batch query:")
print("\n".join(l[:100] for l in plan(batch, "simple").splitlines() if l.strip()))
