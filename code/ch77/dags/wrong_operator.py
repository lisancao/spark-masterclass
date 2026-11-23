"""Chapter 77: SparkKubernetesOperator with a SparkApplication for the Apache Spark Kubernetes Operator.

SparkKubernetesOperator (cncf.kubernetes provider) is written for the Kubeflow Spark operator's resource
(sparkoperator.k8s.io/v1beta2, with spec.driver and spec.executor). The Apache operator's resource
(spark.apache.org/v1) has a different spec and status. This DAG shows what happens.
"""
import os
from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sdk import DAG

CH48 = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with DAG(dag_id="ch77_wrong_operator", start_date=datetime(2026, 6, 1), schedule=None,
         catchup=False, template_searchpath=[f"{CH48}/k8s"], tags=["ch77"]) as dag:
    SparkKubernetesOperator(
        task_id="apache_app_with_kubeflow_operator",
        namespace="spark-apps",
        application_file="pi-apache.yaml",
    )
