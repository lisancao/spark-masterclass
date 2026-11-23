"""Chapter 77: print what the Apache Spark provider's hooks build, without running Spark.

    $CH48_WORK/airflow-venv/bin/python ch77/commands.py

It needs no Airflow database and no Spark: the connections are environment variables (set below),
and the script only calls the methods that build a command line or a URL.
"""
import json
import os
import warnings

warnings.simplefilter("ignore")
os.environ.update({
    "AIRFLOW__LOGGING__LOGGING_LEVEL": "WARNING",   # the hooks also log each command: print it once
    "AIRFLOW_CONN_SPARK_LOCAL": json.dumps({"conn_type": "spark", "host": "local[2]"}),
    "AIRFLOW_CONN_SPARK_SQL_LOCAL": json.dumps({"conn_type": "spark_sql", "host": "local[2]"}),
    "AIRFLOW_CONN_SPARK_K8S": json.dumps({
        "conn_type": "spark", "host": "k8s://https://127.0.0.1:6443",
        "extra": {"deploy-mode": "cluster", "namespace": "spark-jobs"}}),
    "AIRFLOW_CONN_SPARK_CONNECT_CH48": json.dumps({
        "conn_type": "spark_connect", "host": "localhost", "port": 15064}),
    "AIRFLOW_CONN_SPARK_CONNECT_PROD": json.dumps({
        "conn_type": "spark_connect", "host": "spark.example.com", "port": 443,
        "login": "etl", "password": "s3cret", "extra": {"use_ssl": True}}),
    "AIRFLOW_CONN_SPARK_CONNECT_PATH": json.dumps({
        "conn_type": "spark_connect", "host": "sc://spark.example.com/warehouse"}),
})

from airflow.providers.apache.spark.hooks.spark_connect import SparkConnectHook
from airflow.providers.apache.spark.hooks.spark_sql import SparkSqlHook
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook


def show(title, cmd):
    print(f"# {title}")
    line = ""
    for word in cmd:
        if len(line) + len(word) > 84:
            print(line + " \\")
            line = "   "
        line += " " + word if line else word
    print(line + "\n")


local = SparkSubmitHook(conn_id="spark_local", name="ch77-zone-revenue", driver_memory="1g",
                              conf={"spark.sql.shuffle.partitions": "8"})
show("SparkSubmitOperator, conn spark_local", local._build_spark_submit_command("jobs/zone_revenue.py"))

k8s = SparkSubmitHook(conn_id="spark_k8s", name="ch77-zone-revenue", track_driver_via_k8s_api=True,
                            conf={"spark.kubernetes.scheduler.name": "yunikorn"})
show("SparkSubmitOperator, conn spark_k8s, track_driver_via_k8s_api=True",
     k8s._build_spark_submit_command("local:///opt/ch77/jobs/zone_revenue.py"))

sql = SparkSqlHook(sql="SELECT 1", conn_id="spark_sql_local", name="ch77-top-zones", verbose=False)
show("SparkSqlOperator, conn spark_sql_local", sql._prepare_command(""))

from airflow.providers.apache.spark.hooks.spark_pipelines import SparkPipelinesHook  # noqa: E402

for conn_id in ["spark_local", "spark_connect_ch48"]:
    sdp = SparkPipelinesHook(pipeline_spec="pipeline.yml", pipeline_command="dry-run", conn_id=conn_id)
    cmd = sdp._build_spark_pipelines_command()
    cmd = ["python" if "python" in w else w for w in cmd]     # the Airflow worker's interpreter
    show(f"SparkPipelinesOperator, pipeline_command='dry-run', conn {conn_id}", cmd)

for conn_id in ["spark_connect_ch48", "spark_connect_prod"]:
    print(f"# SparkConnectHook({conn_id!r}).get_connection_url()")
    print(SparkConnectHook(conn_id).get_connection_url() + "\n")

print("# SparkConnectHook('spark_connect_path').get_connection_url()")
try:
    SparkConnectHook("spark_connect_path").get_connection_url()
except ValueError as e:
    print(f"ValueError: {e}")

# Jinja renders every template field, and conf and yaml_conf are template fields.
from airflow.sdk import DAG  # noqa: E402

env = DAG("ch77_jinja").get_template_env()
print("\n# Jinja on a template field (the DAG's own template environment)")
for text in ["{{APP_ID}}", "{{ '{{APP_ID}}' }}"]:
    try:
        print(f"{text!r:24} -> {env.from_string(text).render()!r}")
    except Exception as e:
        print(f"{text!r:24} -> {type(e).__name__}: {e}")
