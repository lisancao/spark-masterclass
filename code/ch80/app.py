"""Chapter 80: the application that every submit in the chapter sends.

It prints where its driver runs, the master and deploy mode it got, its executors, and one
aggregation over the taxi month. With --hold N it keeps running N more seconds (Step 4 kills
the master during that time).
"""
import argparse
import json
import os
import socket
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("trips")
ap.add_argument("--hold", type=int, default=0)
args = ap.parse_args()

spark = SparkSession.builder.appName("ch80-app").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel(os.environ.get("CH80_LOG_LEVEL", "ERROR"))
conf = sc.getConf()
print(f"driver host   {socket.gethostname()}  pid {os.getpid()}")
print(f"master        {sc.master}")
print(f"deploy mode   {conf.get('spark.submit.deployMode', 'client')}")
print(f"app id        {sc.applicationId}")

ui = sc.uiWebUrl
with urllib.request.urlopen(f"{ui}/api/v1/applications/{sc.applicationId}/executors") as r:
    for e in json.load(r):
        print(f"executor {e['id']:>6}  {e['hostPort']:<22} cores {e['totalCores']}")

df = spark.read.parquet(args.trips)
rows = (df.groupBy("payment_type").agg(F.count("*").alias("trips"))
          .orderBy("payment_type").collect())
print("trips by payment_type: " + ", ".join(f"{r.payment_type}={r.trips:,}" for r in rows))

for i in range(args.hold):
    n = spark.range(0, 2_000_000, numPartitions=4).selectExpr("sum(id)").first()[0]
    print(f"  hold {i + 1:>2}s  sum={n}  executors={sc._jsc.sc().getExecutorMemoryStatus().size() - 1}", flush=True)
    time.sleep(1)
spark.stop()
