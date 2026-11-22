"""Chapter 38: the engines, their configs, and the taxi queries that every engine runs.

Each engine is a set of Spark configs, nothing more: a jar on the classpath, a plugin class
in spark.plugins, and its memory and shuffle settings. The other ch38 scripts import this file.
The Spark version comes from the Python that runs the script (see ch38/setup.sh for the venvs).
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import DATA, ZONES

YEAR = DATA / "ch38" / "yellow"          # twelve months, 2025-07 to 2026-06 (setup.sh)
JARS = DATA / "ch38" / "jars"
START, END = "2025-07-01", "2026-07-01"

GLUTEN_JAR = JARS / "gluten-1.7.0" / "apache-gluten-1.7.0-bin-spark-4.1" / \
    "gluten-velox-bundle-spark4.1_2.13-linux_amd64-1.7.0.jar"
COMET_JAR = JARS / "comet-spark-spark4.2_2.13-1.2.0.jar"
RAPIDS_JAR = JARS / "rapids-4-spark_2.13-26.10.0-cuda13.jar"
AURON_JAR = JARS / "auron-spark-4.2_2.13-release-linux-x86_64-9.0.0.jar"   # built: setup.sh

# The same driver for every engine: local mode, 16 task slots, the same heap.
BASE = {
    "spark.driver.memory": "12g",
    "spark.sql.shuffle.partitions": "16",
    "spark.sql.session.timeZone": "UTC",
}


# Two settings that decide how much of a plan the engines can take (Step 4 shows why).
# The timed runs use them for every engine, the CPU baseline too, so all run the same query.
OFFLOAD_FRIENDLY = {
    "spark.sql.ansi.enabled": "false",                     # Gluten 1.7.0 falls back under ANSI
    "spark.sql.parquet.inferTimestampNTZ.enabled": "false",  # read pickup times as TIMESTAMP
}


def jar_conf(jar):
    return {"spark.jars": str(jar), "spark.driver.extraClassPath": str(jar),
            "spark.executor.extraClassPath": str(jar)}


ENGINES = {
    "cpu": {},
    # Apache Gluten 1.7.0 with the Velox backend, built for Spark 4.1.1.
    "gluten": {
        **jar_conf(GLUTEN_JAR),
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "8g",
    },
    # Apache DataFusion Comet 1.2.0, built for Spark 4.2.
    "comet": {
        **jar_conf(COMET_JAR),
        "spark.plugins": "org.apache.spark.CometPlugin",
        "spark.shuffle.manager":
            "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
        "spark.comet.explain.fallback.enabled": "true",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "8g",
    },
    # NVIDIA RAPIDS Accelerator 26.10.0 (cuDF, CUDA 13), Spark 4.2.0 shim.
    "rapids": {
        **jar_conf(RAPIDS_JAR),
        "spark.plugins": "com.nvidia.spark.SQLPlugin",
        "spark.rapids.sql.concurrentGpuTasks": "2",
        "spark.rapids.sql.explain": "NOT_ON_GPU",
    },
    # Apache Auron 9.0.0, compiled from the source release for Spark 4.2.0 (no binary exists).
    # Auron has no plugin class: it uses spark.sql.extensions directly.
    "auron": {
        **jar_conf(AURON_JAR),
        "spark.auron.enable": "true",
        "spark.sql.extensions": "org.apache.spark.sql.auron.AuronSparkSessionExtension",
        "spark.shuffle.manager":
            "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager",
        "spark.memory.offHeap.enabled": "false",
        "spark.executor.memoryOverhead": "8g",       # native memory = overhead x 0.6
        "spark.driver.memoryOverhead": "8g",         # local mode: the driver is the executor
        "spark.auron.native.log.level": "warn",
    },
}


def start(engine, app=None, **extra):
    """A local session for one engine. Extra keyword confs use dots, passed as a dict."""
    from pyspark.sql import SparkSession
    conf = {**BASE, **ENGINES[engine], **extra}
    b = (SparkSession.builder.appName(app or f"ch38-{engine}")
         .master(os.environ.get("SPARK_MASTER", "local[16]"))
         .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040")))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("CH24_LOG", "ERROR"))
    return spark


def tables(spark):
    trips = spark.read.parquet(str(YEAR))
    zones = (spark.read.option("header", True)
             .schema("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")
             .csv(str(ZONES)))
    return trips, zones


def queries(spark):
    """The taxi queries, as DataFrames. Each one stresses a different part of the plan."""
    trips, zones = tables(spark)
    year = trips.where((F.col("tpep_pickup_datetime") >= START) &
                       (F.col("tpep_pickup_datetime") < END))
    return {
        # scan + filter + two-phase hash aggregate + sort: Chapter 5's query, over a year
        "hourly": (year.groupBy(F.hour("tpep_pickup_datetime").alias("hour"))
                   .agg(F.count("*").alias("trips"),
                        F.round(F.avg("total_amount"), 2).alias("avg_total"))
                   .orderBy("hour")),
        # a broadcast hash join to the zone lookup, then an aggregate
        "boroughs": (year.join(zones, year.PULocationID == zones.LocationID)
                     .groupBy("Borough")
                     .agg(F.count("*").alias("trips"),
                          F.round(F.sum("fare_amount"), 0).alias("fares"),
                          F.round(F.avg("tip_amount"), 3).alias("avg_tip"))
                     .orderBy("Borough")),
        # a high-cardinality aggregate (zone pairs), then a top-N
        "routes": (year.groupBy("PULocationID", "DOLocationID")
                   .agg(F.count("*").alias("trips"),
                        F.round(F.avg("trip_distance"), 2).alias("avg_miles"))
                   .orderBy(F.desc("trips"), "PULocationID", "DOLocationID")
                   .limit(10)),
    }


def fare_band(fare):                     # plain Python: no engine can run this natively
    if fare is None or fare <= 0:
        return "invalid"
    return "under $10" if fare < 10 else "$10 to $30" if fare < 30 else "over $30"


def udf_query(spark):
    trips, _ = tables(spark)
    return (trips.where(F.col("trip_distance") > 0)
            .groupBy(F.udf(fare_band, "string")("fare_amount").alias("band")).count().orderBy("band"))


def walk(df):
    """The operators of df's executed plan (run the query first), top to bottom.

    Returns (depth, operator class name, supportsColumnar) for each node. It reads the
    final plan of an adaptive query and goes into query stages, so it sees what ran."""
    out = []

    def visit(node, depth):
        name = node.getClass().getSimpleName()
        if name == "AdaptiveSparkPlanExec":
            return visit(node.executedPlan(), depth)
        if name.endswith("QueryStageExec"):
            return visit(node.plan(), depth)
        if name in ("ReusedExchangeExec",):
            return visit(node.child(), depth)
        out.append((depth, name, bool(node.supportsColumnar())))
        kids = node.children()
        for i in range(kids.size()):
            visit(kids.apply(i), depth + 1)

    visit(df._jdf.queryExecution().executedPlan(), 0)
    return out


# Nodes that only connect other nodes: stage wrappers, batch resizers, shuffle readers.
GLUE = {"AQEShuffleReadExec", "ColumnarInputAdapter", "InputIteratorTransformer",
        "WholeStageTransformer", "WholeStageCodegenExec", "InputAdapter",
        "VeloxResizeBatchesExec", "CometSinkPlaceHolder", "GpuShuffleCoalesceExec",
        "GpuCoalesceBatches", "GpuCustomShuffleReaderExec", "ShuffleQueryStageExec",
        "ForceNativeExecutionWrapper"}


def kind(name):
    """'transition', 'glue', 'engine' (runs in the native or GPU engine) or 'spark'."""
    if any(t in name for t in ("ColumnarToRow", "RowToColumnar", "RowToVelox", "SparkToColumnar", "ConvertToNative")):
        return "transition"
    if name in GLUE:
        return "glue"
    if name.startswith(("Gpu", "Comet", "Velox", "Columnar", "Native")) or "Transformer" in name:
        return "engine"
    return "spark"


def native_share(nodes):
    """Counts of (engine operators, Spark JVM operators, transitions) in a walked plan."""
    kinds = [kind(n) for _, n, _ in nodes]
    return kinds.count("engine"), kinds.count("spark"), kinds.count("transition")
