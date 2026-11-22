"""Chapter 37: the other part of Feather in 4.3: an Arrow format for df.cache().

spark.sql.cache.serializer is static, so each serializer runs in its own process.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch37/arrow_cache.py
"""
import json, statistics, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ARROW = "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer"
CASES = [("default", {}),
         ("Arrow", {"spark.sql.cache.serializer": ARROW}),
         ("Arrow, zstd", {"spark.sql.cache.serializer": ARROW, "spark.sql.execution.arrow.compression.codec": "zstd"})]

if len(sys.argv) > 1:                                     # child: one serializer
    from pyspark.sql import functions as F
    from common.book import session, ui_api, TRIPS
    conf = json.loads(sys.argv[1])
    spark = session("ch37-arrow-cache", spark_driver_memory="4g", spark_ui_showConsoleProgress="false",
                    **{k.replace(".", "_"): v for k, v in conf.items()})
    df = spark.read.parquet(str(TRIPS)).select("tpep_pickup_datetime", "PULocationID", "DOLocationID",
                                              "fare_amount", "tip_amount", "payment_type").cache()
    t = time.perf_counter(); n = df.count(); fill = time.perf_counter() - t
    info = ui_api("storage/rdd")[0]
    q = lambda: df.groupBy("PULocationID").agg(F.avg("tip_amount"), F.count("*")).collect()
    q()
    ts = []
    for _ in range(9):
        t = time.perf_counter(); q(); ts.append(time.perf_counter() - t)
    plan_node = next((l.strip()[:60] for l in df._jdf.queryExecution().executedPlan().toString().splitlines()
                      if "InMemoryTableScan" in l), "")
    print(json.dumps({"rows": n, "fill": fill, "mem": info["memoryUsed"], "disk": info["diskUsed"],
                      "query": statistics.median(ts) * 1000, "serializer": spark.conf.get("spark.sql.cache.serializer")}))
    sys.exit(0)

print("June trips, 6 columns, df.cache(); a groupBy from the cache, median of 9 after a warm-up\n")
print(f"{'cache':<14}{'rows':>11}{'fill s':>8}{'memory MiB':>12}{'query ms':>10}  serializer")
for label, conf in CASES:
    try:
        r = subprocess.run([sys.executable, __file__, json.dumps(conf)], capture_output=True, text=True, timeout=600)
        out = json.loads(r.stdout.strip().splitlines()[-1])
        print(f"{label:<14}{out['rows']:>11,}{out['fill']:>8.1f}{out['mem'] / 2**20:>12.1f}{out['query']:>10.0f}  "
              f"{out['serializer'].split('.')[-1]}")
    except subprocess.TimeoutExpired:
        print(f"{label:<14}  did not finish in 600 s")
    except Exception as e:
        print(f"{label:<14}  failed: {(r.stderr.strip().splitlines() or [str(e)])[-1][:80]}")
