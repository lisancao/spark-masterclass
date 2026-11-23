"""Chapter 2: the driver's own account of one job, from the scheduler's INFO log lines.

Runs the same aggregation as anatomy.py on local-cluster[2,2,1024] with the log level at INFO,
and keeps only the lines from DAGScheduler, TaskSchedulerImpl and TaskSetManager.
Classic only. Run from code/:
  uv run python ch02/scheduler_log.py 2>&1 | grep -E 'DAGScheduler|TaskSchedulerImpl|TaskSetManager'
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, TRIPS

spark = session("ch02-scheduler-log", spark_master="local-cluster[2,2,1024]",
                spark_sql_shuffle_partitions="6", spark_sql_adaptive_enabled="false",
                spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
while sc._jsc.sc().getExecutorMemoryStatus().size() < 3:
    time.sleep(0.2)
trips = spark.read.parquet(str(TRIPS))
q = trips.groupBy("PULocationID").agg(F.count("*").alias("trips"), F.sum("total_amount"))
sc.setLogLevel("INFO")
q.collect()
sc.setLogLevel("ERROR")
spark.stop()
