"""Chapter 77: the same Spark 4.3 job on Kubernetes, submitted from Airflow in two ways.

1. zone_revenue_submit: SparkSubmitOperator in cluster mode. spark-submit creates the driver pod and
   exits, and the operator polls the pod phase through the Kubernetes API (track_driver_via_k8s_api).
2. zone_revenue_app: a SparkApplication for the Apache Spark Kubernetes Operator (Chapter 76), made by
   KubernetesCreateResourceOperator. A sensor reads the resource status until the operator reports an end.

Both jobs go to YuniKorn's queue root.etl. Connections come from the environment (see af.sh).
"""
import os
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.resource import KubernetesCreateResourceOperator
from airflow.sdk import DAG, PokeReturnValue, task

CH48 = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
K = "spark.kubernetes"
YUNIKORN = {
    f"{K}.scheduler.name": "yunikorn",
    f"{K}.driver.label.queue": "root.etl",
    f"{K}.executor.label.queue": "root.etl",
    # conf is a template field: without the string literal, Jinja stops the task (APP_ID is undefined)
    f"{K}.driver.annotation.yunikorn.apache.org/app-id": "{{ '{{APP_ID}}' }}",
    f"{K}.executor.annotation.yunikorn.apache.org/app-id": "{{ '{{APP_ID}}' }}",
}
VOLUMES = {f"{K}.{role}.volumes.hostPath.{name}.{key}": path
           for role, name, path in [("driver", "taxi", "/data/nyc-taxi"), ("executor", "taxi", "/data/nyc-taxi"),
                                    ("driver", "jobs", "/opt/ch77/jobs")]
           for key in ("mount.path", "options.path")}

with DAG(
    dag_id="ch77_taxi_k8s",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    tags=["ch77"],
) as dag:
    submit = SparkSubmitOperator(
        task_id="zone_revenue_submit",
        # master k8s://..., deploy-mode cluster, namespace spark. CH48_SUBMIT_CONN=spark_k8s_jobs uses the
        # namespace spark-jobs, where provider 6.3.4 cannot find the driver pod (see the chapter).
        conn_id=os.environ.get("CH48_SUBMIT_CONN", "spark_k8s"),
        application="local:///opt/ch77/jobs/zone_revenue.py",
        application_args=["/data/nyc-taxi", "-", "{{ run_id }}"],
        name="ch77-submit",
        conf={
            f"{K}.container.image": "apache/spark:4.3.0-python3",
            f"{K}.container.image.pullPolicy": "Never",
            f"{K}.authenticate.driver.serviceAccountName": "spark",
            "spark.executor.instances": "2", "spark.executor.cores": "1",
            "spark.executor.memory": "1g", "spark.driver.memory": "1g",
            **YUNIKORN, **VOLUMES,
        },
        track_driver_via_k8s_api=True,             # spark-submit exits; Airflow polls the driver pod
        status_poll_interval=20,
    )

    with open(f"{CH48}/k8s/zone-revenue-app.yaml") as f:
        manifest = f.read()

    create_app = KubernetesCreateResourceOperator(
        task_id="zone_revenue_app",
        yaml_conf=manifest,                        # rendered with Jinja: name, run_id
        custom_resource_definition=True,
    )

    @task.sensor(poke_interval=10, timeout=600, mode="poke")
    def wait_for_app(ts_nodash=None):
        hook = KubernetesHook()
        name = f"zone-revenue-{ts_nodash.lower()}"
        app = hook.get_custom_object(group="spark.apache.org", version="v1", plural="sparkapplications",
                                     name=name, namespace="spark-apps")
        status = app.get("status", {})
        history = [h["currentStateSummary"] for _, h in sorted(
            status.get("stateTransitionHistory", {}).items(), key=lambda kv: int(kv[0]))]
        print(f"APP {name} state {status.get('currentState', {}).get('currentStateSummary', '-')}")
        end = next((s for s in history if s in ("Succeeded", "Failed", "SchedulingFailure")), None)
        if end:
            print("APP history:", " > ".join(history))
            return PokeReturnValue(is_done=True, xcom_value=end)
        return PokeReturnValue(is_done=False)

    @task
    def driver_log(state, ts_nodash=None):
        hook = KubernetesHook()
        pods = hook.core_v1_client.list_namespaced_pod(
            "spark-apps", label_selector="spark-role=driver").items
        pod = next(p for p in pods if p.metadata.name.startswith(f"zone-revenue-{ts_nodash.lower()}"))
        # get_pod_logs returns the raw response: decode the bytes here
        log = hook.get_pod_logs(pod.metadata.name, namespace="spark-apps").data.decode()
        print(f"APP driver pod {pod.metadata.name}, scheduler {pod.spec.scheduler_name}, "
              f"yunikorn app-id {pod.metadata.annotations.get('yunikorn.apache.org/app-id')}")
        for line in log.splitlines():
            if line.startswith("ZONE_REVENUE"):
                print(line)
        if state != "Succeeded":
            raise RuntimeError(f"SparkApplication ended in state {state}")

    submit >> create_app
    driver_log(create_app >> wait_for_app())
