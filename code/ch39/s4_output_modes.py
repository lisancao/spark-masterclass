"""Step 4: which output modes each query shape accepts, and the error for the rest.

Run from code/:  uv run python ch39/s4_output_modes.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver
from replay import slices, SCHEMA

spark = session("ch39-output-modes", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work("ch39/s4")
inbox = w / "inbox"
inbox.mkdir()
name, text, n = slices()[0]
deliver(inbox, name, text)
trips = spark.readStream.schema(SCHEMA).json(str(inbox))

shapes = {
    "filter (no state)": trips.where("fare_amount > 50"),
    "groupBy count": trips.groupBy("PULocationID").count(),
    "window + watermark": (trips.withWatermark("pickup_ts", "15 minutes")
                           .groupBy(F.window("pickup_ts", "10 minutes")).count()),
    "groupBy + orderBy": trips.groupBy("PULocationID").count().orderBy(F.desc("count")),
    "orderBy (no agg)": trips.orderBy("pickup_ts"),
}
errors = {}
print(f"{'query shape':<20}{'append':<11}{'update':<11}{'complete':<11}")
for label, df in shapes.items():
    cells = []
    for mode in ("append", "update", "complete"):
        try:
            q = (df.writeStream.format("memory").queryName(f"q_{len(errors)}_{mode}")
                 .outputMode(mode).option("checkpointLocation", str(w / f"ck_{label}_{mode}"))
                 .trigger(availableNow=True).start())
            q.awaitTermination()
            cells.append(f"ok ({spark.table(q.name).count()})")
        except Exception as e:
            cls = str(e).split("]")[0].lstrip("[") if str(e).startswith("[") else type(e).__name__
            errors.setdefault(str(e).split("\n")[0][:200], (label, mode, str(e).split("\n")[0]))
            cells.append("error")
    print(f"{label:<20}" + "".join(f"{c:<11}" for c in cells))

print()
for cls, (label, mode, msg) in errors.items():
    print(f"{label}, {mode}:\n  {msg[:260]}")
