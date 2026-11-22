"""Chapter 48: run a Spark 4.3 batch job with spark-submit, then query its output with spark-sql.

Both tasks start a Spark driver in a child process of the Airflow worker (client mode, master local[2]).
Connections come from the environment: AIRFLOW_CONN_SPARK_LOCAL and AIRFLOW_CONN_SPARK_SQL_LOCAL.
"""
import os
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

CH48 = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TAXI = os.environ.get("CH48_TAXI", os.path.expanduser("~/spark-masterclass/data/nyc-taxi"))
OUT = os.environ.get("CH48_OUT", "/tmp/ch48/zone_revenue")

with DAG(
    dag_id="ch48_taxi_submit",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    params={"out": OUT},
    template_searchpath=[f"{CH48}/jobs"],
    tags=["ch48"],
) as dag:
    revenue = SparkSubmitOperator(
        task_id="zone_revenue",
        conn_id="spark_local",
        application=f"{CH48}/jobs/zone_revenue.py",
        application_args=[TAXI, OUT, "{{ run_id }}"],
        name="ch48-zone-revenue",
        conf={"spark.ui.port": "4064", "spark.sql.shuffle.partitions": "8"},
        driver_memory="1g",
    )

    top3 = SparkSqlOperator(
        task_id="top_zones",
        conn_id="spark_sql_local",
        sql="top_zones.sql",          # a file in template_searchpath, rendered with Jinja
        conf={"spark.ui.port": "4064"},
        name="ch48-top-zones",
        verbose=False,
    )

    revenue >> top3
