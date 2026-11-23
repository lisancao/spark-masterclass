"""Chapter 74, Step 12: the plan a Spark Connect client sends for a query with a Python UDF, and lazy analysis.

Start the server first:  ch74/server.sh start
Run from code/:          uv run python ch74/connect_plan.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F
from logs import ACCESS_LOG, _area


def compact(msg):
    """Drop the bookkeeping fields on every node (plan IDs, Python call sites) and shorten the pickle."""
    from google.protobuf.message import Message
    for f in ("common", "python_origin"):
        if f in msg.DESCRIPTOR.fields_by_name:
            msg.ClearField(f)
    if "command" in msg.DESCRIPTOR.fields_by_name and isinstance(msg.command, bytes) and msg.command:
        msg.command = msg.command[:12]
    for field, value in msg.ListFields():
        for v in (value if hasattr(value, "extend") else [value]):
            if isinstance(v, Message):
                compact(v)
    return msg


classic = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
try:
    classic.range(3).select("nope")
except Exception as e:
    classic_msg = ("Classic: select('nope') ->", type(e).__name__, str(e).splitlines()[0][:80])
classic.stop()

with SparkSession.builder.remote("sc://localhost:15061").getOrCreate() as spark:
    area = F.udf(_area, "string")
    df = (spark.read.text(str(ACCESS_LOG))
          .select(F.split("value", " ")[6].alias("path"))
          .filter(area("path") == "api"))
    p = df._plan.to_proto(spark.client)
    print(f"plan message: {p.ByteSize()} bytes, UDF command: "
          f"{len(p.root.filter.condition.unresolved_function.arguments[0].common_inline_user_defined_function.python_udf.command)} bytes\n")
    print(compact(p))
    print("rows:", df.count())

    bad = spark.range(3).select("nope")
    print("\nConnect: spark.range(3).select('nope') returned a DataFrame; no error yet")
    try:
        bad.columns
    except Exception as e:
        print("Connect: bad.columns ->", type(e).__name__, str(e).splitlines()[0][:80])

print(*classic_msg)
