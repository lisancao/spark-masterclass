"""Chapter 42: one application that fills every tab of the Spark UI, for the tab-by-tab reference.

It runs a batch job, a cache, a spilling sort, a streaming query and a Spark Connect session (the Connect
server runs as a plugin inside this application), prints what the REST API reports, and then waits until
the file named in UI_TABS_DONE exists, so that film/ch42_ui_tabs.py can take screenshots.
Run from code/:  uv run python ch42/ui_tabs.py
"""
import os, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, ui_api, ui_port, TRIPS

CONNECT_PORT = os.environ.get("CONNECT_PORT", "15114")
spark = session("ch42-ui-tabs", spark_sql_session_timeZone="UTC",
                spark_ui_showConsoleProgress="false",
                spark_plugins="org.apache.spark.sql.connect.SparkConnectPlugin",
                spark_connect_grpc_binding_port=CONNECT_PORT,
                spark_sql_shuffle_partitions="8")
sc = spark.sparkContext

sc.setJobDescription("1. trips by pickup zone")
trips = spark.read.parquet(str(TRIPS))
trips.groupBy("PULocationID").agg(F.count("*").alias("n")).orderBy(F.desc("n")).limit(5).collect()

sc.setJobDescription("2. cache the June tips")
tips = trips.select("PULocationID", "tip_amount", "fare_amount").where("fare_amount > 0").cache()
tips.count()

sc.setJobDescription("3. sort all June trips by distance, 2 partitions")
spark.conf.set("spark.sql.shuffle.partitions", "2"); spark.conf.set("spark.sql.adaptive.enabled", "false")
trips.orderBy("trip_distance").write.format("noop").mode("overwrite").save()
spark.conf.unset("spark.sql.adaptive.enabled"); spark.conf.set("spark.sql.shuffle.partitions", "8")
sc.setJobDescription(None)

# A streaming query: rate source, a window count, the memory sink.
stream = (spark.readStream.format("rate").option("rowsPerSecond", 200).load()
          .groupBy(F.window("timestamp", "5 seconds")).count()
          .writeStream.queryName("rate_windows").outputMode("complete").format("memory")
          .trigger(processingTime="2 seconds").start())

# A Spark Connect client in its own process: one session, two operations, with a tag.
client = f"""
from pyspark.sql import SparkSession
s = SparkSession.builder.remote("sc://localhost:{CONNECT_PORT}/;user_id=reader").getOrCreate()
s.addTag("ui-tour")
print("connect:", s.range(1_000_000).selectExpr("sum(id)").first()[0], s.read.parquet("{TRIPS}").count())
"""
r = subprocess.run([sys.executable, "-c", client], capture_output=True, text=True)
print(r.stdout.strip().replace(str(TRIPS), "TRIPS") or r.stderr[-500:])
time.sleep(12)

print(f"UI on port {ui_port()} (default 1g heap, so that the sort spills)")
jobs = ui_api("jobs")
print(f"  jobs {len(jobs)}, stages {len(ui_api('stages'))}")
st = ui_api("storage/rdd")[0]
print(f"  storage: {st['name'][:60]}, {st['numCachedPartitions']}/{st['numPartitions']} partitions, {st['memoryUsed'] / 2**20:,.1f} MiB")
ex = ui_api("executors")
print(f"  executors: {len(ex)} ({', '.join(e['id'] for e in ex)}), cores {ex[0]['totalCores']}, "
      f"max memory {ex[0]['maxMemory'] / 2**20:,.1f} MiB")
env = ui_api("environment")
print(f"  environment: {len(env['sparkProperties'])} Spark properties, Java {env['runtime']['javaVersion'][:12]}")
sql = ui_api("sql?details=false&length=1000")
print(f"  SQL executions: {len(sql)}")
print(f"  streaming query: {stream.name}, id {stream.id}, batches so far {stream.lastProgress['batchId'] + 1 if stream.lastProgress else 0}")
sys.stdout.flush()
Path(os.environ.get("UI_TABS_READY", "/tmp/masterclass-ch42/tabs_ready")).write_text("ready")
done = Path(os.environ.get("UI_TABS_DONE", "/tmp/masterclass-ch42/tabs_done"))
for _ in range(900):
    if done.exists():
        break
    time.sleep(1)
stream.stop()
