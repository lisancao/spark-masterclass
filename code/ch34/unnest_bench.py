"""Chapter 34: the cost of LEFT JOIN LATERAL UNNEST against explode_outer, on the June taxi trips.
Run under the bench lock:  flock $BOOK_DATA/.bench.lock uv run python ch34/unnest_bench.py
"""
import re
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session, plan, TRIPS

spark = session("ch34-unnest-bench", spark_driver_memory="4g", spark_ui_showConsoleProgress="false")
spark.read.parquet(str(TRIPS)).selectExpr(
    "VendorID", "array(PULocationID, DOLocationID) AS zones").createOrReplaceTempView("t")
spark.sql("CACHE TABLE c AS SELECT * FROM t")
print("rows:", spark.table("c").count(), "| distinct arrays:", spark.sql("SELECT count(DISTINCT zones) FROM c").first()[0])
CASES = {
    "LATERAL VIEW OUTER explode": "SELECT VendorID, z FROM c LATERAL VIEW OUTER explode(zones) x AS z",
    "LATERAL UNNEST": "SELECT VendorID, x.z FROM c, LATERAL UNNEST(zones) AS x(z)",
    "LEFT JOIN LATERAL UNNEST": "SELECT VendorID, x.z FROM c LEFT JOIN LATERAL UNNEST(zones) AS x(z) ON true",
}
for label, sql in CASES.items():
    df = spark.sql(sql)
    df.write.format("noop").mode("overwrite").save()
    ts = []
    for _ in range(5):
        t0 = time.perf_counter()
        spark.sql(sql).write.format("noop").mode("overwrite").save()
        ts.append(time.perf_counter() - t0)
    names = ("Generate", "BroadcastHashJoin", "SortMergeJoin", "HashAggregate", "Exchange")
    ops = [n for l in plan(df, "simple").split("== Initial Plan ==")[0].splitlines()
           for n in names if re.search(r"(^|[ *)])" + n + r"[ (]", l)]
    print(f"  {label:<28} median {statistics.median(ts):6.3f} s  operators: {ops}")
