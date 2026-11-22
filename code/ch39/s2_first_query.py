"""Step 2: a first streaming query, run three times on one checkpoint.

Trips by pickup borough (a stream-static join with the zone lookup), complete mode,
memory sink, trigger(availableNow=True). Each run reads only the files that are new.
Run from code/:  uv run python ch39/s2_first_query.py
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, ZONES
from replay import slices, SCHEMA

spark = session("ch39-first-query", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work("ch39/s2")
inbox, ckpt = w / "inbox", w / "checkpoint"
inbox.mkdir()

zones = (spark.read.option("header", True).csv(str(ZONES))
         .select(F.col("LocationID").cast("int").alias("PULocationID"), "Borough"))


def run():
    trips = spark.readStream.schema(SCHEMA).json(str(inbox))
    by_borough = (trips.join(zones, "PULocationID")          # stream-static join
                  .groupBy("Borough").agg(F.count("*").alias("trips"),
                                          F.round(F.sum("total_amount"), 2).alias("revenue")))
    q = (by_borough.writeStream
         .format("memory").queryName("by_borough")
         .outputMode("complete")
         .option("checkpointLocation", str(ckpt))
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    return q


files = slices()
progress = {}
for run_no, batch in enumerate([files[0:2], files[2:3], []], start=1):
    for name, text, n in batch:
        deliver(inbox, name, text)
    q = run()
    progress[run_no] = q.recentProgress
    got = [(p.batchId, p.numInputRows) for p in q.recentProgress]
    print(f"run {run_no}: delivered {[n for _, _, n in batch]}, batches (id, rows) {got}")
    spark.table("by_borough").orderBy(F.desc("trips")).show(3)
    print("  checkpoint commits:", sorted(f.name for f in (ckpt / "commits").iterdir()
                                         if not f.name.startswith(".")))

p = json.loads(progress[2][-1].json)
keep = ["id", "runId", "name", "batchId", "numInputRows", "durationMs", "stateOperators", "sources", "sink"]
p = {k: p[k] for k in keep}
p["stateOperators"] = [{k: op[k] for k in ("operatorName", "numRowsTotal", "numRowsUpdated",
                                            "numShufflePartitions", "numStateStoreInstances")}
                       for op in p["stateOperators"]]
p["sources"] = [{k: s[k] for k in ("description", "startOffset", "endOffset", "numInputRows")}
                for s in p["sources"]]
print("Run 2's progress report, trimmed:")
print(json.dumps(p, indent=1).replace(str(Path.home()), "~"))
