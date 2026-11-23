"""Chapter 63: the same question over Spark Connect, from a task that has no JVM.

@task.pyspark gives the function a SparkSession. With a spark_connect connection, that session is a
thin Connect client (pyspark-client), and the driver is the long-running server on sc://localhost:15064.
"""
import os
from datetime import datetime

from airflow.sdk import DAG, task

TAXI = os.environ.get("CH48_TAXI", os.path.expanduser("~/spark-masterclass/data/nyc-taxi"))

with DAG(
    dag_id="ch63_taxi_connect",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    tags=["ch63"],
) as dag:

    @task.pyspark(conn_id="spark_connect_ch48")
    def busiest_zones(spark=None):
        from pyspark.sql import functions as F

        trips = spark.read.parquet(f"{TAXI}/yellow")
        rows = (trips.where(F.col("tpep_pickup_datetime") >= "2026-06-01")
                     .groupBy("PULocationID").count()
                     .orderBy(F.desc("count")).limit(3).collect())
        print("CONNECT session:", type(spark).__module__, "server", spark.version)
        return [(r.PULocationID, r["count"]) for r in rows]       # an XCom: plain data, not a DataFrame

    @task
    def report(zones):
        for zone, n in zones:
            print(f"CONNECT zone {zone:>3}: {n:,} pickups")

    report(busiest_zones())
