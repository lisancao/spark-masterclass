"""Chapter 54: Spark Connect plugins, called from a Python client that has no JVM.

Start the server first:  ch54/server.sh start
Run from code/:          uv run python ch54/plugins.py
"""
import json, os, sys, tempfile, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from common.book import ROOT, ui_api  # noqa: E402
from jplug_client import MESSAGES, client_classes  # noqa: E402

IMAGES = str(ROOT / "code" / "data" / "messy" / "images")


def head(t):
    print(f"\n== {t}")


def error(e, key=None):
    lines = str(e).splitlines()
    line = next((l for l in lines if key and key in l), lines[0])
    return f"{type(e).__name__}: {line.strip()[:150]}"


head("1. Bytes on the wire: a jplug.PlanNodes inside a google.protobuf.Any")
from google.protobuf import any_pb2  # noqa: E402
msg = MESSAGES["PlanNodes"](input=b"ab", stage="physical")
print("message:", msg.SerializeToString())
a = any_pb2.Any()
a.Pack(msg)
print("type_url:", a.type_url)
print("Any:    ", a.SerializeToString())

with SparkSession.builder.remote("sc://localhost:15060").getOrCreate() as spark:
    plan_nodes, has_magic, note = client_classes()

    head("2. A Connect session has no JVM")
    print(type(spark).__module__, "| server runs Spark", spark.version)
    try:
        spark._jvm
    except Exception as e:
        print("spark._jvm ->", error(e))

    head("3. What works without a plugin: names the server knows")
    print("jdocs by short name:",
          spark.read.format("jdocs").option("path", IMAGES).load().select("name", "width").collect()[:2])
    print(spark.sql("SELECT is_png(X'89504E470D0A1A0A') AS p").collect())
    print(spark.range(1).select(F.call_function("is_png", F.lit(b"\x89PNG\r\n\x1a\n")).alias("p")).collect())

    head("4. plan_nodes: a relation plugin")
    df = spark.range(10).filter("id > 3").groupBy((F.col("id") % 2).alias("k")).count()
    plan_nodes(df).show(truncate=False)
    plan_nodes(df, "physical").show(truncate=False)
    try:
        plan_nodes(df, "parsed").collect()
    except Exception as e:
        print("stage 'parsed' ->", error(e, "stage must be"))

    head("5. Plan facts a Connect test can now assert on")
    tmp = tempfile.mkdtemp(prefix="ch54-")
    spark.range(100).selectExpr("id", "id % 7 AS k").write.parquet(os.path.join(tmp, "t"))
    t = spark.read.parquet(os.path.join(tmp, "t")).filter("k = 3")
    scan = plan_nodes(t, "physical").filter(F.col("node").startswith("Scan parquet"))
    detail = scan.first().detail
    print("pushed:", detail[detail.index("PushedFilters"):].split(", ReadSchema")[0])

    @F.udf("long")
    def double(v):
        return v * 2

    @F.udf("boolean")
    def is_png(b):
        return b is not None and bytes(b[:8]) == b"\x89PNG\r\n\x1a\n"

    imgs = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(IMAGES)
    for name, q in (("double(id)", spark.range(5).select(double("id"))),
                    ("is_png(content), rewritten by jext.PngRule", imgs.select(is_png("content")))):
        nodes = [r.node for r in plan_nodes(q, "physical").orderBy("id").collect()]
        print(f"{name}: {nodes}")

    head("6. An extension nobody handles")
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.connect.plan import LogicalPlan

    class Unknown(LogicalPlan):
        def plan(self, session):
            rel = self._create_proto_relation()
            rel.extension.Pack(MESSAGES["Note"](text="not a relation"))
            return rel

    try:
        ConnectDataFrame(Unknown(None), spark).collect()
    except Exception as e:
        print(error(e, "NO_HANDLER"))

    head("7. has_magic: an expression plugin")
    (imgs.select(F.substring_index("path", "/", -1).alias("file"),
                 has_magic(F.col("content"), b"\x89PNG").alias("png"))
     .orderBy("file").show(truncate=False))

    head("8. note: a command plugin")
    note(spark, "checked by chapter 54")
    print("spark.conf.get('jplug.note') =", spark.conf.get("jplug.note"))
    session_id = spark.client._session_id

head("9. The server's record of the plugin operations (4.3 REST API)")
import re  # noqa: E402
ops = [o for o in ui_api("connect/operations", port=4160) if o["sessionId"] == session_id]
ops.sort(key=lambda o: o["startTimestamp"])
print(len(ops), "operations in this session; those whose recorded statement names a jplug type:")
for o in ops:
    urls = re.findall(r"type\.googleapis\.com/(jplug\.\w+)", o.get("statement") or "")
    if urls:
        print(f"  {urls[0]:16} {o['state']:7} {o['executionTime']:5} ms  jobs {o['jobIds']}  {o['detail'][:60]!r}")
