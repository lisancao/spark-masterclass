"""Step 10, continued: outer stream-stream joins, the state that a join keeps, and the join types.

The same pickup and dropoff events as s10_join.py, but the dropoff events of trips whose trip_id
ends in 7 are lost on the way. A left outer join must write those pickups with NULL dropoffs.
Run from code/:  uv run python ch39/s10b_join_state.py
"""
import datetime as dt
import os
import time
os.environ["TZ"] = "UTC"          # collected timestamps print in UTC, as the session time zone
time.tzset()
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, error_text
from replay import DAY, trips_between

spark = session("ch39-join", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_ui_showConsoleProgress="false")
w = work("ch39/s10b")
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
lost = {t["trip_id"] for t in trips if t["trip_id"].endswith("7")}
for b in batches.values():
    b["d"] = [d for d in b["d"] if d["trip_id"] not in lost]
for k, s in enumerate(sorted(batches)):              # one pickup file and one dropoff file per 5 minutes
    for side, key in (("pickups", "p"), ("dropoffs", "d")):
        text = "".join(json.dumps(r) + "\n" for r in batches[s][key])
        deliver(w / side, f"{side}_{s:%H%M}.jsonl", text, mtime=1_000_000 + k)

PSCHEMA = "trip_id STRING, PULocationID INT, pickup_ts TIMESTAMP"
DSCHEMA = "trip_id STRING, DOLocationID INT, dropoff_ts TIMESTAMP, total_amount DOUBLE"


def sides(watermark=True):
    p = spark.readStream.schema(PSCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "pickups"))
    d = (spark.readStream.schema(DSCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "dropoffs"))
         .withColumnRenamed("trip_id", "d_trip_id"))
    if watermark:
        p, d = p.withWatermark("pickup_ts", "10 minutes"), d.withWatermark("dropoff_ts", "10 minutes")
    return p, d


RANGE = "trip_id = d_trip_id AND dropoff_ts >= pickup_ts AND dropoff_ts <= pickup_ts + INTERVAL 1 HOUR"
pickups, dropoffs = sides()
outer = pickups.join(dropoffs, F.expr(RANGE), "left")
print(f"{len(trips)} pickups after 07:00; dropoff events lost for {len(lost)} of them (trip_id ends in 7)\n")

rows = {}


def body(df, batch_id):
    df = df.persist()
    nulls = df.where("d_trip_id IS NULL")
    rows[batch_id] = (df.count() - nulls.count(), nulls.count(),
                      nulls.agg(F.min("pickup_ts"), F.max("pickup_ts")).first())
    df.unpersist()


q = (outer.writeStream.foreachBatch(body).outputMode("append")
     .option("checkpointLocation", str(w / "ck_outer")).trigger(availableNow=True).start())
q.awaitTermination()
print("1. Left outer join, append mode:")
print("batch  matched  null rows  their pickups    watermark  state rows  state stores")
for p in q.recentProgress:
    m, n, (lo, hi) = rows.get(p.batchId, (0, 0, (None, None)))
    span = f"{lo:%H:%M}-{hi:%H:%M}" if lo else ""
    op = p.stateOperators[0]
    print(f"{p.batchId:>5}  {m:>7}  {n:>9}  {span:^14}  {(p.eventTime.get('watermark') or '')[11:16]:^9}  "
          f"{op.numRowsTotal:>10}  {op.numStateStoreInstances:>12}")
total_null = sum(v[1] for v in rows.values())
print(f"null rows written: {total_null}, lost dropoffs: {len(lost)}")
print(f"operator: {q.recentProgress[-1].stateOperators[0].operatorName}")

print("\n2. What the checkpoint says the join keeps (state-metadata):")
(spark.read.format("state-metadata").load(str(w / "ck_outer"))
 .select("operatorName", "stateStoreName", "numPartitions", "maxBatchId").show(truncate=False))

print("3. The rows that each side still keeps (statestore, joinSide):")
for side in ("left", "right"):
    st = spark.read.format("statestore").option("joinSide", side).load(str(w / "ck_outer"))
    ts = "pickup_ts" if side == "left" else "dropoff_ts"
    agg = st.agg(F.count("*").alias("rows"), F.min(f"value.{ts}").alias("oldest"), F.max(f"value.{ts}").alias("newest")).first()
    print(f"   {side:<5} ({ts}): {agg.rows} rows, from {agg.oldest:%H:%M:%S} to {agg.newest:%H:%M:%S}")
    if side == "left":
        st.select("key.*", "value.*").orderBy("value.pickup_ts").show(2, truncate=False)

print("4. Join types for two streams, with and without watermarks:")
for how in ("inner", "left", "right", "full", "left_semi", "left_anti"):
    out = []
    for wm in (True, False):
        p, d = sides(wm)
        cond = F.expr(RANGE) if wm else F.expr("trip_id = d_trip_id")
        try:
            qq = (p.join(d, cond, how).writeStream.format("noop").outputMode("append")
                  .option("checkpointLocation", str(w / f"ck_{how}_{wm}")).start())
            qq.stop()
            out.append("ok")
        except Exception as e:
            out.append(error_text(e, 200, 1).strip().split(": ", 1)[1][:52] + " ...")
    print(f"   {how:<10} with watermarks and range: {out[0]}")
    print(f"   {'':<10} without:                   {out[1]}")
spark.stop()
