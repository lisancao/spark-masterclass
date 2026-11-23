"""Chapter 24: the quick start's question over Spark Connect, and what 4.3 tells you about each operation.

Start the server first:  ch24/server.sh start
Run from code/:          uv run python ch24/connect.py
"""
import json, os, sys, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F
from common.book import ROOT, TRIPS, MONTH_START, MONTH_END, ui_api

PORT = os.environ.get("CONNECT_PORT", "15002")      # ch24/server.sh uses the same variables
UI = os.environ.get("CONNECT_UI_PORT", "4041")


def compact(msg):
    """The plan protobuf without the bookkeeping on every node (plan IDs, Python call sites)."""
    from google.protobuf.message import Message
    if "common" in msg.DESCRIPTOR.fields_by_name:
        msg.ClearField("common")
    for field, value in msg.ListFields():
        for v in (value if isinstance(value, (list, tuple)) or hasattr(value, "extend") else [value]):
            if isinstance(v, Message):
                compact(v)
    return msg

with SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate() as spark:   # 4.3: a context manager
    print(type(spark).__module__, "| server runs Spark", spark.version)

    trips = spark.read.parquet(str(TRIPS))
    june = trips.where((F.col("tpep_pickup_datetime") >= MONTH_START) &
                       (F.col("tpep_pickup_datetime") < MONTH_END))
    busiest = june.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(3)

    # Nothing has been sent yet. This is the plan the client will send, as protobuf.
    print("\nThe unresolved plan the client builds:\n")
    print(compact(busiest._plan.to_proto(spark.client)))

    rows = busiest.collect()
    print("busiest pickup zones:", [(r.PULocationID, r["count"]) for r in rows])
    print("succeeded, operation", busiest.executionInfo.operation_id)

    # A mistake the client can't catch: the column doesn't exist, and only the server knows.
    bad = june.select("revenue")
    print("built a DataFrame over a column that doesn't exist; no error yet")
    try:
        bad.collect()
    except Exception as e:
        failed = e.operation_id
        (ROOT / "data" / "failed_op").write_text(failed)      # for looking it up later
        import textwrap
        print(f"failed,    operation {failed}")
        print(textwrap.fill(f"{type(e).__name__}: {str(e).splitlines()[0]}", 90,
                            initial_indent="  ", subsequent_indent="  "))

    # What isn't there: anything that lives in the JVM.
    try:
        spark.sparkContext
    except Exception as e:
        print(textwrap.fill(f"spark.sparkContext -> {type(e).__name__}: {str(e).splitlines()[0]}", 90,
                            initial_indent="\n", subsequent_indent="  "))

# 4.3: the server's record of every session and operation, from the monitoring REST API.
ops = ui_api("connect/operations", port=UI)
mine = next(o for o in ops if o["operationId"] == failed)
print("\nThe server's record of the failed operation:\n")
print(json.dumps({k: mine[k] for k in mine if k not in ("statement",)}, indent=2)[:1500])
