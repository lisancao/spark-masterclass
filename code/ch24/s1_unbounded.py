"""Step 1: a streaming DataFrame is a plan over a table that keeps growing.

Run from code/:  uv run python ch24/s1_unbounded.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, plan, work, deliver, error_text
from replay import slices, SCHEMA

spark = session("ch24-unbounded", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
inbox = work("ch24/s1") / "inbox"
inbox.mkdir()
name, text, n = slices()[0]
deliver(inbox, name, text)

print("1. A file stream with no schema:")
try:
    spark.readStream.json(str(inbox))
except Exception as e:
    print(error_text(e))

trips = spark.readStream.schema(SCHEMA).json(str(inbox))
print("\n2. With a schema: isStreaming =", trips.isStreaming)
print(plan(trips.groupBy("PULocationID").count(), "extended").split("== Analyzed")[0])

print("3. An action on a streaming DataFrame:")
try:
    trips.count()
except Exception as e:
    print(error_text(e, lines=4))

print("\n4. The same files as a batch read:", spark.read.schema(SCHEMA).json(str(inbox)).count(), "rows")

spark.conf.set("spark.sql.streaming.schemaInference", "true")
inferred = spark.readStream.json(str(inbox))
print("\n5. spark.sql.streaming.schemaInference=true infers once, at start:")
print("  ", ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in inferred.schema.fields))
