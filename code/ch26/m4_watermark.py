"""Steps 4 and 5: the watermark on real messy events, and the one future-dated event that
wrecks it. Sessions per user (dedupe within the watermark, then 5-minute session windows),
one drop per run.

  uv run python ch26/m4_watermark.py defended   # future events rejected before the watermark
  uv run python ch26/m4_watermark.py naive      # the same query without that check
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work
from clicks import drop, parse_events

MODE = sys.argv[1] if len(sys.argv) > 1 else "defended"
spark = session("ch26-watermark", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC")
w = work(f"ch26/m4-{MODE}")
inbox = w / "inbox"
inbox.mkdir()

events = parse_events(spark.readStream.text(str(inbox)), reject_future=(MODE == "defended"))
sessions = (events.where("error IS NULL")
            .withWatermark("ts", "10 minutes")
            .dropDuplicatesWithinWatermark(["event_id"])
            .groupBy("user_id", F.session_window("ts", "5 minutes").alias("s"))
            .agg(F.count("*").alias("events"), F.max(F.col("type") == "checkout").alias("checkout"))
            .select("user_id", F.col("s.start").alias("start"), F.col("s.end").alias("end"),
                    "events", "checkout"))

print(f"{MODE}: drop  batch  rows  watermark      dedup          sessions      written")
print(f"{'':<36}(state/late)   (state/late)")
for i in range(1, 5):
    drop(inbox, i)
    q = (sessions.writeStream.format("parquet").outputMode("append")
         .option("path", str(w / "sessions")).option("checkpointLocation", str(w / "ck"))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    written = spark.read.parquet(str(w / "sessions")).count() if (w / "sessions/_spark_metadata").exists() else 0
    for k, p in enumerate(q.recentProgress):
        ops = {op.operatorName: op for op in p.stateOperators}
        s, d = ops["sessionWindowStateStoreSaveExec"], ops["dedupeWithinWatermark"]
        wm = p.eventTime.get("watermark", "")[5:16].replace("T", " ")
        last = k == len(q.recentProgress) - 1
        print(f"{'':<10}{i:>4}  {p.batchId:>5}  {p.numInputRows:>4}  {wm:<12}  "
              f"{d.numRowsTotal:>5}/{d.numRowsDroppedByWatermark:<3}      "
              f"{s.numRowsTotal:>3}/{s.numRowsDroppedByWatermark:<3}     {written if last else '':>6}")

out = spark.read.parquet(str(w / "sessions")).orderBy("user_id", "start")
print(f"\nfinished sessions written: {out.count()}")
out.select("user_id", F.date_format("start", "MM-dd HH:mm:ss").alias("start"),
           F.date_format("end", "MM-dd HH:mm:ss").alias("end"), "events", "checkout").show(truncate=False)
