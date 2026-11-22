"""Helpers for Chapter 86 only: a warehouse, the catalog tables, the bench lock and a median timer."""
import contextlib
import fcntl
import os
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, ZONES, TAXI, DATA, ROOT

WH = ROOT / "data" / "work" / "ch86-warehouse"     # gitignored; tables stay between scripts


def start(app, **conf):
    """A session with a warehouse folder under data/work, so saveAsTable tables have a stable path."""
    WH.mkdir(parents=True, exist_ok=True)
    conf.setdefault("spark_sql_session_timeZone", "UTC")
    conf.setdefault("spark_ui_showConsoleProgress", "false")
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app).master(os.environ.get("SPARK_MASTER", "local[*]"))
    b = b.config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040")).config("spark.sql.warehouse.dir", str(WH))
    # a Hive metastore in Derby under data/work, so tables and their statistics outlive one script
    b = b.config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={WH}/metastore_db;create=true")
    b = b.config("spark.driver.extraJavaOptions", f"-Dderby.system.home={WH}")
    for k, v in conf.items():
        b = b.config(k.replace("_", "."), v)
    spark = b.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def trips_clean(spark, src=TRIPS):
    """The June trips, inside the month and with a positive distance (the same cleaning as Chapter 33)."""
    t = spark.read.parquet(str(src))
    return (t.where((F.col("tpep_pickup_datetime") >= "2026-01-01") & (F.col("tpep_pickup_datetime") < "2026-07-01")
                    & (F.col("trip_distance") > 0))
             .select("VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID", "passenger_count",
                     "trip_distance", "payment_type", "fare_amount", "tip_amount", "total_amount"))


def zones(spark):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))


@contextlib.contextmanager
def bench_lock():
    """Hold the shared bench lock only while a timing runs. BOOK_NO_LOCK=1 skips it (films, test runs)."""
    if os.environ.get("BOOK_NO_LOCK"):
        yield
        return
    with open(DATA / ".bench.lock", "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def median_s(run, runs=5):
    """Median wall time of run() over `runs` runs after one warmup, under the bench lock.
    BOOK_RUNS overrides `runs` (films use 1; the book quotes only the full runs saved in output/)."""
    runs = int(os.environ.get("BOOK_RUNS", runs))
    with bench_lock():
        run()
        ts = []
        for _ in range(runs):
            t0 = time.perf_counter()
            run()
            ts.append(time.perf_counter() - t0)
    return statistics.median(ts)


def noop(df):
    """Run a query to the end without a result on the driver."""
    df.write.format("noop").mode("overwrite").save()


def last_sql_metrics(spark, node_prefix):
    """Metrics of the plan nodes whose name starts with node_prefix, in the last SQL execution (REST API)."""
    ex = ui_api("sql?details=true&planDescription=false&length=1000")
    last = max(ex, key=lambda e: e["id"])
    return [(n["nodeName"], {m["name"]: m["value"] for m in n["metrics"]}) for n in last["nodes"]
            if n["nodeName"].startswith(node_prefix)]


def last_query_work():
    """Task time (sum of executorRunTime, s) and input bytes of all stages of the last SQL query (REST API).
    Less sensitive than wall time to other work on a shared machine."""
    ex = max(ui_api("sql?details=false&length=1000"), key=lambda e: e["id"])
    run_ms = in_bytes = 0
    for j in ex["successJobIds"]:
        for st in ui_api(f"jobs/{j}")["stageIds"]:
            for att in ui_api(f"stages/{st}"):
                if att["status"] == "COMPLETE":
                    run_ms += att["executorRunTime"]
                    in_bytes += att["inputBytes"]
    return run_ms / 1000, in_bytes
