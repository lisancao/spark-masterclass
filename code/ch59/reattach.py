"""Chapter 59, Step 10: Connect operation IDs and reattachable execution under a cut connection.

Start the server with a small stream size, so that one result needs several streams:
    ch59/server.sh start --conf spark.connect.execute.reattachable.senderMaxStreamSize=16m
Run from code/:
    uv run python ch59/reattach.py
"""
import collections
import re
import os
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession

from flaky_proxy import FlakyProxy
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import TRIPS, ui_api  # noqa: E402

PORT = int(os.environ.get("CONNECT_PORT", "10280"))
UI = os.environ.get("CONNECT_UI_PORT", "4128")
proxy = FlakyProxy(listen=PORT + 1, target=PORT).start()


def count_rpcs(spark):
    """Wrap the client's gRPC stub so that every RPC it sends is counted."""
    calls = collections.Counter()
    stub = spark.client._stub
    for name in ("ExecutePlan", "ReattachExecute", "ReleaseExecute"):
        real = getattr(stub, name)

        def wrapped(*a, _real=real, _name=name, **kw):
            calls[_name] += 1
            return _real(*a, **kw)
        setattr(stub, name, wrapped)
    return calls


def fmt(calls):
    return "  ".join(f"{k} {calls[k]}" for k in ("ExecutePlan", "ReattachExecute", "ReleaseExecute") if calls[k])


def server_record(op):
    rec = next(o for o in ui_api("connect/operations", port=UI) if o["operationId"] == op)
    return f"state={rec['state']} jobIds={rec['jobIds']} executionTime={rec['executionTime']} ms"


def big_query(spark):
    return spark.read.parquet(str(TRIPS)).select("PULocationID", "DOLocationID", "total_amount")


with SparkSession.builder.remote(f"sc://localhost:{PORT + 1}").getOrCreate() as spark:
    calls = count_rpcs(spark)

    # 1. No fault. The server ends each stream after senderMaxStreamSize bytes, and the client
    #    reattaches to the same operation to read the rest.
    df = big_query(spark)
    pdf = df.toPandas()
    op = df.executionInfo.operation_id
    print(f"1. no fault: {len(pdf):,} rows, operation {op[:8]}\n   rpcs: {fmt(calls)}")
    print(f"   server: {server_record(op)}")

    # 2. Cut the TCP connection in the middle of the result.
    calls.clear()
    proxy.cut_after(30_000_000)
    df = big_query(spark)
    pdf = df.toPandas()
    op = df.executionInfo.operation_id
    print(f"2. connection cut at 30 MB: {len(pdf):,} rows, operation {op[:8]}, cuts {proxy.cuts}\n"
          f"   rpcs: {fmt(calls)}")
    print(f"   server: {server_record(op)}")

    # 3. The same cut without reattachable execution: the stream cannot resume.
    calls.clear()
    spark.client.disable_reattachable_execute()
    proxy.cut_after(30_000_000)
    df = big_query(spark)
    try:
        df.toPandas()
    except Exception as e:
        cls = re.search(r"\[([A-Z_.]+)\]", str(e))
        print(f"3. not reattachable, cut at 30 MB: {type(e).__name__} {cls.group(1) if cls else ''}")
        print(f"   rpcs: {fmt(calls)}, operation {getattr(e, 'operation_id', '?')[:8]}")
    spark.client.enable_reattachable_execute()

    # 4. An operation ID names one execution. A second ExecutePlan with the same ID is refused.
    #    (This uses the client's internal request builder: the public API always makes a new ID.)
    op = str(uuid.uuid4())
    for attempt in (1, 2):
        req = spark.client._execute_plan_request_with_metadata(operation_id=op)
        req.plan.CopyFrom(spark.range(3)._plan.to_proto(spark.client))
        try:
            n = sum(1 for r in spark.client._stub.ExecutePlan(req, metadata=spark.client._builder.metadata())
                    if r.HasField("arrow_batch"))
            print(f"4. ExecutePlan #{attempt} with operation {op[:8]}: ok, {n} arrow batch(es)")
        except Exception as e:
            detail = e.details() if hasattr(e, "details") else str(e)
            cls = re.search(r"\[([A-Z_.]+)\]", detail)
            print(f"4. ExecutePlan #{attempt} with operation {op[:8]}: {cls.group(1) if cls else detail[:70]}")
