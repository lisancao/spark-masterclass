"""Step 10: a stream-stream join. Pickup events and dropoff events arrive on two streams;
the join pairs them by trip_id, and watermarks plus a time-range condition bound its state.

Run from code/:  uv run python ch48/s10_join.py
"""
import datetime as dt
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, error_text
from replay import DAY, trips_between

spark = session("ch48-join", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work("ch48/s10")
(w / "pickups").mkdir()
(w / "dropoffs").mkdir()

start = DAY + dt.timedelta(hours=8)
trips = trips_between(start, start + dt.timedelta(hours=1))
trips = [t for t in trips if t["pickup_ts"] >= (start - dt.timedelta(hours=1)).isoformat()]
slot = lambda ts: max(start - dt.timedelta(minutes=5),
                      dt.datetime.fromisoformat(ts).replace(second=0, minute=dt.datetime.fromisoformat(ts).minute // 5 * 5))
batches = {}
for t in trips:
    batches.setdefault(slot(t["pickup_ts"]), {"p": [], "d": []})["p"].append(
        {"trip_id": t["trip_id"], "PULocationID": t["PULocationID"], "pickup_ts": t["pickup_ts"]})
    batches.setdefault(slot(t["dropoff_ts"]), {"p": [], "d": []})["d"].append(
        {"trip_id": t["trip_id"], "DOLocationID": t["DOLocationID"], "dropoff_ts": t["dropoff_ts"],
         "total_amount": t["total_amount"]})
for k, s in enumerate(sorted(batches)):              # one pickup file and one dropoff file per 5 minutes
    for side, key in (("pickups", "p"), ("dropoffs", "d")):
        text = "".join(json.dumps(r) + "\n" for r in batches[s][key])
        deliver(w / side, f"{side}_{s:%H%M}.jsonl", text, mtime=1_000_000 + k)

pickups = (spark.readStream.schema("trip_id STRING, PULocationID INT, pickup_ts TIMESTAMP")
           .option("maxFilesPerTrigger", 1).json(str(w / "pickups"))
           .withWatermark("pickup_ts", "10 minutes"))
dropoffs = (spark.readStream.schema("trip_id STRING, DOLocationID INT, dropoff_ts TIMESTAMP, total_amount DOUBLE")
            .option("maxFilesPerTrigger", 1).json(str(w / "dropoffs"))
            .withWatermark("dropoff_ts", "10 minutes")
            .withColumnRenamed("trip_id", "d_trip_id"))
joined = pickups.join(dropoffs, F.expr("""
    trip_id = d_trip_id AND
    dropoff_ts >= pickup_ts AND dropoff_ts <= pickup_ts + INTERVAL 1 HOUR"""))

for mode in ("append", "update"):
    try:
        q = (joined.writeStream.format("memory").queryName(f"joined_{mode}").outputMode(mode)
             .option("checkpointLocation", str(w / f"ck_{mode}")).trigger(availableNow=True).start())
        q.awaitTermination()
    except Exception as e:
        print(f"{mode}:\n{error_text(e)}")
        continue
    print(f"{mode} mode: {spark.table(f'joined_{mode}').count()} trips joined "
          f"(of {len(trips)} whose pickup was after 07:00)")
    if mode == "append":
        print("batch  rows in  joined  watermark  state rows")
        for p in q.recentProgress:
            op = p.stateOperators[0]
            print(f"{p.batchId:>5}  {p.numInputRows:>7}  {p.sink.numOutputRows:>6}  "
                  f"{(p.eventTime.get('watermark') or '')[11:16]:^9}  {op.numRowsTotal:>10}")
