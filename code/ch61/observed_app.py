"""Chapter 61, Steps 1 to 4: one application, observed while it runs.

A skewed aggregation through a slow Python UDF and a sort that is forced to spill, with the event
log on and the Prometheus servlet on. While the application is alive, the script reads the REST API
and the metrics endpoints. It saves the REST stage list for eventlog.py.

Run from code/:  uv run python ch61/observed_app.py
"""
import json
import shutil
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ROOT, ui_port  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

LOGS = ROOT / "data" / "ch61" / "eventlog"
shutil.rmtree(LOGS, ignore_errors=True)
LOGS.mkdir(parents=True)

spark = (SparkSession.builder.master("local[2]").appName("observed")
         .config("spark.ui.port", ui_port())
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", str(LOGS))
         .config("spark.sql.adaptive.enabled", "false")        # a fixed stage layout
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.shuffle.spill.numElementsForceSpillThreshold", "2000")   # internal: spill early
         .config("spark.metrics.conf.*.sink.prometheusServlet.class",
                 "org.apache.spark.metrics.sink.PrometheusServlet")
         .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus")
         .config("spark.metrics.namespace", "observed")
         .config("spark.ui.prometheus.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
BASE = f"http://localhost:{ui_port()}"


def get(path, raw=False):
    with urllib.request.urlopen(BASE + path) as r:
        body = r.read().decode()
    return body if raw else json.loads(body)


@F.udf("long")
def score(user, n):
    h = 0
    for i in range(1000):                       # slow on purpose: about 1000 steps for each row
        h = (h * 31 + n + i + len(user)) % 1000003
    return h


events = (spark.range(0, 60000, numPartitions=2)
          .withColumn("user", F.when(F.col("id") % 10 < 9, F.lit("bot-7"))      # 90% on one key
                      .otherwise(F.concat(F.lit("u"), (F.col("id") % 97).cast("string"))))
          .withColumn("n", F.col("id") % 13))

sc.setJobDescription("skewed_scoring")
(events.repartition(4, "user").select("user", score("user", "n").alias("s"))
 .groupBy("user").agg(F.sum("s").alias("s")).write.format("noop").mode("overwrite").save())

sc.setJobDescription("spilling_sort")
(spark.range(0, 30000, numPartitions=2)
 .select((F.col("id") * 7919 % 30011).alias("k"), F.col("id"))
 .repartition(2, "k").sortWithinPartitions("k").write.format("noop").mode("overwrite").save())
sc.setJobDescription(None)

# --- Step 3: the REST API ---------------------------------------------------------------------
app = sc.applicationId
stages = get(f"/api/v1/applications/{app}/stages")
print("\nREST /stages:")
for s in sorted(stages, key=lambda s: s["stageId"]):
    print(f"  stage {s['stageId']}  {s['description'] or '':15} tasks {s['numCompleteTasks']}  "
          f"run {s['executorRunTime']:>5} ms  in {s['inputRecords'] + s['shuffleReadRecords']:>6} rows  "
          f"spill {s['diskBytesSpilled']:>7} B")
udf_stage = max((s for s in stages if s["description"] == "skewed_scoring"),
                key=lambda s: s["shuffleReadRecords"])
q = get(f"/api/v1/applications/{app}/stages/{udf_stage['stageId']}/0/taskSummary"
        "?quantiles=0.0,0.25,0.5,0.75,1.0")
print(f"\nREST taskSummary for stage {udf_stage['stageId']} (quantiles 0, .25, .5, .75, 1):")
print("  shuffle records read:", [int(x) for x in q["shuffleReadMetrics"]["readRecords"]])
print("  executor run time ms:", [int(x) for x in q["executorRunTime"]])

sql = get(f"/api/v1/applications/{app}/sql?details=true")
first = next(x for x in sql if x["description"] == "skewed_scoring")
print(f"\nREST /sql execution {first['id']} ({first['description']}): nodes with metrics")
for node in first["nodes"]:
    for m in node["metrics"]:
        if m["name"] in ("time to execute Python code", "number of output rows", "data sent to Python workers"):
            value = m["value"].splitlines()[-1]          # the line after "total (min, med, max ...)"
            print(f"  {node['nodeName'][:16]:16} {m['name']:28} {value[:44]}")

# --- Step 4: the metrics system ---------------------------------------------------------------
prom = get("/metrics/prometheus", raw=True).splitlines()
print(f"\n/metrics/prometheus: {len(prom)} lines, for example")
for line in prom:
    if "DAGScheduler_job_allJobs_Value" in line or "BlockManager_memory_maxMem_MB_Value" in line:
        print("  ", line)
ex = get("/metrics/executors/prometheus", raw=True).splitlines()
print(f"/metrics/executors/prometheus: {len(ex)} lines, for example")
for line in ex:
    if "metrics_executor_totalTasks_total" in line or "metrics_executor_diskUsed_bytes" in line:
        print("  ", line)
metrics_json = get("/metrics/json")
print("/metrics/json keys:", sorted(metrics_json)[:5], "version", metrics_json["version"])

(ROOT / "data" / "ch61" / "rest_stages.json").write_text(json.dumps(stages))
spark.stop()
print("\nevent log folder:", [p.name for p in LOGS.iterdir()])
