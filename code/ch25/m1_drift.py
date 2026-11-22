"""Step 1: a stream's schema is fixed when the query starts, so drift is silent.

Run from code/:  uv run python ch25/m1_drift.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, error_text
from clicks import drop, V1_SCHEMA

spark = session("ch25-drift", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC")
w = work("ch25/m1")
inbox = w / "inbox"
inbox.mkdir()

print("1. schemaInference=true infers the schema once, when the query starts")
spark.conf.set("spark.sql.streaming.schemaInference", "true")
drop(inbox, 1)
print("   drop 1 only:      ", spark.readStream.json(str(inbox)).schema.simpleString())
drop(inbox, 2)
drop(inbox, 3)
print("   after drops 2, 3: ", spark.readStream.json(str(inbox)).schema.simpleString())
spark.conf.set("spark.sql.streaming.schemaInference", "false")


def run(df, name):
    q = (df.writeStream.format("memory").queryName(name)
         .option("checkpointLocation", str(w / f"ck_{name}")).trigger(availableNow=True).start())
    q.awaitTermination()
    return spark.table(name)


print("\n2. The tracker 1.0 schema meets tracker 2.0 (drop 3): no error, and type is NULL")
v1 = run(spark.readStream.schema(V1_SCHEMA).json(str(inbox)), "v1")
v1.groupBy((v1.type.isNull()).alias("type_is_null")).count().show()
v1.where("type IS NULL").show(3)

print("3. The corrupt record column catches broken lines, and only them")
raw = spark.readStream.schema(V1_SCHEMA + ", _corrupt_record STRING").json(str(inbox))
run(raw.where("_corrupt_record IS NOT NULL"), "corrupt").select("event_id", "ts", "_corrupt_record").show(truncate=48)

print("4. Selecting only the corrupt record column starts, then fails on the first batch:")
try:
    run(raw.select("_corrupt_record"), "only_corrupt")
except Exception as e:
    print(error_text(e, lines=4))
