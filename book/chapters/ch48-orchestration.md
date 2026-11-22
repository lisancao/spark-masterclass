# Chapter 48: Orchestrating Spark: Airflow, operators, YuniKorn and Celeborn

A Spark job that runs once from a terminal is easy. A Spark job that runs each night, after its inputs arrive, on a cluster that other teams also use, is a different problem. Three systems help with this problem, and each system has a different job:

- **An orchestrator** (in this chapter, Apache Airflow) decides when a job runs and what runs after it.
- **A batch scheduler for Kubernetes** (Apache YuniKorn or Volcano) decides which pods get CPU and memory, and in which order.
- **A remote shuffle service** (Apache Celeborn) keeps shuffle data outside the executors, so that executors can stop before the job ends.

**Airflow does not run Spark. It starts a Spark driver, or sends a plan to a driver that already runs, and then it waits. Each Airflow operator puts the driver in a different location. The location controls what the Airflow worker must contain. It also controls what occurs when a task fails.**

**On Kubernetes, the default scheduler and the default shuffle are not sufficient for Spark. A driver can start and then wait forever for executors, and an executor that holds shuffle data cannot stop. YuniKorn and Celeborn correct these two problems. Celeborn 0.7.0 does not support Spark 4.3.0.**

This chapter runs each part for real. A local Airflow 3.4.0 runs DAGs against Spark 4.3.0, on the laptop and on a kind cluster with three nodes. YuniKorn schedules Spark pods on that cluster, and Celeborn holds the shuffle data of a Spark job. Chapter 47 explains the Apache Spark Kubernetes Operator. This chapter only uses that operator from Airflow.

The code is [`code/ch48`](https://github.com/lisancao/spark-masterclass/tree/main/code/ch48).

## The layers

```mermaid
flowchart TB
  classDef af fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  classDef spark fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  classDef k8s fill:#4a7a4a,fill-opacity:0.72,stroke:#4a7a4a,color:#ffffff
  dag["Airflow DAG<br/>(when, and in which order)"]:::af
  dag -->|"SparkSubmitOperator<br/>SparkSqlOperator<br/>SparkPipelinesOperator"| local["driver in a child process<br/>of the Airflow worker"]:::spark
  dag -->|"@task.pyspark<br/>+ spark_connect connection"| connect["Connect server<br/>(a driver that runs all the time)"]:::spark
  dag -->|"SparkSubmitOperator<br/>master k8s://"| pod["driver pod"]:::spark
  dag -->|"KubernetesCreateResourceOperator<br/>SparkApplication"| op["Spark Kubernetes Operator<br/>(Chapter 47)"]:::k8s
  op --> pod
  pod --> yk["YuniKorn: queues and<br/>gang scheduling"]:::k8s
  yk --> ex["executor pods"]:::spark
  ex -->|"shuffle write and read"| clb["Celeborn workers"]:::k8s
```

The versions in this chapter:

```text
apache-airflow                             3.4.0
apache-airflow-providers-apache-spark      6.3.4
apache-airflow-providers-cncf-kubernetes   10.25.0
pyspark-client                             4.3.0
kubernetes                                 36.0.3

RELEASE                    NAMESPACE       CHART                            APP_VERSION
celeborn                   celeborn        celeborn-0.1.0                   0.7.0
spark-kubernetes-operator  spark-operator  spark-kubernetes-operator-1.9.0  1.1.0
yunikorn                   yunikorn        yunikorn-1.10.0                  -

NAME                STATUS  VERSION
ch48-control-plane  Ready   v1.37.0
ch48-worker         Ready   v1.37.0
ch48-worker2        Ready   v1.37.0
```

- Apache Airflow 3.4.0, released on 6 November 2026, with its official constraints file for Python 3.12.
- The Apache Spark provider, `apache-airflow-providers-apache-spark==6.3.4`, released on 24 October 2026. It is the current release on PyPI (6.3.5 is a release candidate).
- The Kubernetes provider, `apache-airflow-providers-cncf-kubernetes==10.25.0`. This version is the version in the Airflow 3.4.0 constraints file. Version 11.0.0 is also on PyPI, but it is not in the constraints file.
- Apache YuniKorn 1.10.0 (Helm chart 1.10.0), Apache Celeborn 0.7.0, and the Apache Spark Kubernetes Operator 1.1.0 (Helm chart 1.9.0).
- kind 0.33.0 with Kubernetes 1.37.0: one control plane and two workers.

## Step 1: Install Airflow next to Spark, not inside it

`code/ch48/airflow_env.sh` makes a separate virtual environment for Airflow. It installs Airflow and the provider with the constraints file of the Airflow release:

```bash
VIRTUAL_ENV="$WORK/airflow-venv" uv pip install -q \
  "apache-airflow==$AIRFLOW_VERSION" \
  "apache-airflow-providers-apache-spark[cncf-kubernetes]" \
  --constraint "$WORK/constraints-$PY.txt"
```

The `[cncf-kubernetes]` extra adds the Kubernetes provider. Step 5 uses it. The provider requires `pyspark-client>=4.0.0`, the thin Connect client from [Chapter 4](ch04-spark-connect.md). The provider does not require `pyspark` (that is the `[pyspark]` extra). Thus, the Airflow environment does not contain a JVM or the Spark jars.

The operators that start `spark-submit` or `spark-sql` need these commands on the `PATH` of the Airflow worker. In this chapter, they come from the Spark 4.3.0 tarball of [Chapter 2](ch02-installing.md). `code/ch48/af.sh` runs each `airflow` command with this environment. It also gives Airflow its connections as environment variables, so the metadata database contains no secrets:

```bash
# Connections, in Airflow's JSON form.
export AIRFLOW_CONN_SPARK_LOCAL='{"conn_type": "spark", "host": "local[2]"}'
export AIRFLOW_CONN_SPARK_SQL_LOCAL='{"conn_type": "spark_sql", "host": "local[2]"}'
export AIRFLOW_CONN_SPARK_CONNECT_CH48='{"conn_type": "spark_connect", "host": "localhost", "port": 15064, "login": "airflow"}'
```

This Airflow uses its own `AIRFLOW_HOME` and port 8148. The chapter runs each DAG with `airflow dags test`. This command runs all the tasks of one DAG run in one process, with no scheduler and no web server. Each task still runs through the Task SDK, with the same operator code as in production.

## Step 2: See the command that each operator builds

Each operator in the provider is a thin layer over a hook, and each hook builds a command line or a URL. `code/ch48/commands.py` calls only the methods that build these commands. Thus, it needs no Spark and no Airflow database:

```text
# SparkSubmitOperator, conn spark_local
spark-submit --master local[2] --conf spark.sql.shuffle.partitions=8 --driver-memory \
    1g --name ch48-zone-revenue jobs/zone_revenue.py

# SparkSubmitOperator, conn spark_k8s, track_driver_via_k8s_api=True
spark-submit --master k8s://https://127.0.0.1:6443 --conf \
    spark.kubernetes.scheduler.name=yunikorn --conf \
    spark.kubernetes.namespace=spark-jobs --name ch48-zone-revenue --deploy-mode \
    cluster --conf spark.kubernetes.submission.waitAppCompletion=false \
    local:///opt/ch48/jobs/zone_revenue.py

# SparkSqlOperator, conn spark_sql_local
spark-sql -e SELECT 1 --master local[2] --name ch48-top-zones --queue default

# SparkPipelinesOperator, pipeline_command='dry-run', conn spark_local
spark-pipelines dry-run --spec pipeline.yml --master local[2] --name default-name

# SparkPipelinesOperator, pipeline_command='dry-run', conn spark_connect_ch48
python -m pyspark.pipelines.cli dry-run --spec pipeline.yml

# SparkConnectHook('spark_connect_ch48').get_connection_url()
sc://localhost:15064/

# SparkConnectHook('spark_connect_prod').get_connection_url()
sc://spark.example.com:443/;user_id=etl;token=s3cret;use_ssl=True

# SparkConnectHook('spark_connect_path').get_connection_url()
ValueError: Path {url.path} is not supported in Spark Connect connection URL

# Jinja on a template field (the DAG's own template environment)
'{{APP_ID}}'             -> UndefinedError: 'APP_ID' is undefined
"{{ '{{APP_ID}}' }}"     -> '{{APP_ID}}'
```

These are the important points in this output:

- **`SparkSubmitOperator`** builds a `spark-submit` command. The `--master` value and the deploy mode come from the connection. The other arguments (`conf`, `driver_memory`, `name`) come from the operator.
- With a `k8s://` master and `track_driver_via_k8s_api=True`, the hook adds `spark.kubernetes.submission.waitAppCompletion=false`. Thus, `spark-submit` exits when the driver pod exists, and Airflow polls the pod. Step 5 shows this mode.
- **`SparkSqlOperator`** builds a `spark-sql -e` command. It adds `--queue default`, which is a YARN option. The local master ignores it.
- **`SparkPipelinesOperator`** runs Spark Declarative Pipelines. `pipeline_command` is `run` or `dry-run`. With a Spark connection, the hook starts the `spark-pipelines` launcher. With a Spark Connect connection, it runs `python -m pyspark.pipelines.cli` in the interpreter of the Airflow worker, and it sets `SPARK_REMOTE` from the connection. A `dry-run` task is a good first task in a pipeline DAG: it finds broken references and unresolved columns before Spark reads any data.
- **`SparkConnectHook`** makes an `sc://` URL from a `spark_connect` connection. The login becomes `user_id`, the password becomes `token`, and the extra field `use_ssl` becomes a URL parameter. The token is in clear text in the URL. Thus, do not log this URL.
- A host that contains a path gives a `ValueError`. In provider 6.3.4, the message shows the literal text `{url.path}` and not the path, because the string has no `f` prefix. A later provider release can correct this message.
- **Jinja renders every template field.** The `conf` of `SparkSubmitOperator` and the `yaml_conf` of `KubernetesCreateResourceOperator` are template fields. Spark uses `{{APP_ID}}` as its own placeholder in some config values. Airflow 3 renders templates with `StrictUndefined`, so this placeholder stops the task with `UndefinedError`. The Jinja string literal `{{ '{{APP_ID}}' }}` gives the text to Spark without a change.

## Step 3: A DAG that runs `spark-submit`, then `spark-sql`

The first DAG has two tasks. The first task runs a batch job that writes the revenue of June 2026 by pickup borough to Parquet. The second task queries that Parquet output with SQL:

```python
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

```

`top_zones.sql` is a file in `template_searchpath`. Airflow renders it with Jinja before the task starts, so `{{ params.out }}` becomes the output path:

```sql
SELECT Borough, trips, CAST(revenue AS BIGINT) AS revenue
FROM parquet.`{{ params.out }}`
ORDER BY revenue DESC
LIMIT 3;
```

The output of `af.sh dags test ch48_taxi_submit`, shortened by `airflow_log.py` to the lines that matter:

```text
09:53:11  running task zone_revenue
09:53:17  Spark-Submit cmd: spark-submit --master local[2] --conf spark.ui.port=4064 --conf spark.sql.shuffle.partitions=8 --driver-memory 1g --name ch48-zone-revenue …/zone_revenue.py ~/spark-masterclass/data/nyc-taxi …/zone_revenue manual__<run>
09:53:41  ZONE_REVENUE run=manual__<run> spark=4.3.0 master=local[2]
09:53:41  ZONE_REVENUE Manhattan       3,374,269 trips  $    90,000,789
09:53:41  ZONE_REVENUE Queens            319,164 trips  $    22,374,570
09:53:41  ZONE_REVENUE Brooklyn          106,652 trips  $     4,107,067
09:53:41  ZONE_REVENUE Bronx              14,955 trips  $       533,102
09:53:41  ZONE_REVENUE N/A                 2,355 trips  $       258,590
09:53:41  ZONE_REVENUE Unknown             4,109 trips  $       155,703
09:53:41  ZONE_REVENUE EWR                   565 trips  $        67,976
09:53:41  ZONE_REVENUE Staten Island         236 trips  $        12,809
09:53:41  ZONE_REVENUE 8 rows written to …/out/zone_revenue in 8.9 s
09:53:41  task state -> success
09:53:41  running task top_zones
09:54:04  Manhattan	3374269	90000789
09:54:04  Queens	319164	22374570
09:54:04  Brooklyn	106652	4107067
09:54:04  Time taken: 6.062 seconds, Fetched 3 row(s)
09:54:05  task state -> success
09:54:05  DagRun Finished: run_duration=54.307048, state=success
```

```mermaid
sequenceDiagram
  participant W as Airflow worker
  participant S as spark-submit (JVM)
  participant D as driver (local[2])
  W->>S: Popen(spark-submit ... zone_revenue.py)
  S->>D: start driver in the same JVM
  D-->>S: log lines on stdout and stderr
  S-->>W: each line goes to the task log
  D->>D: job ends, spark.stop()
  S-->>W: exit code 0
  W->>W: task state -> success
```

Each task starts a JVM in a child process of the Airflow worker. In client mode, the driver runs in that JVM. These are the results of this design:

- The Airflow worker must have Java and a Spark installation of the correct version. The provider does not install them.
- The driver uses the memory of the Airflow worker. `driver_memory="1g"` gives the driver JVM 1 GB of heap on the worker.
- The hook reads each line of the `spark-submit` output and sends it to the task log. If Airflow stops the task, the hook kills the `spark-submit` process.
- Each task is a new Spark application. The second task cannot use a DataFrame of the first task. It reads the Parquet files that the first task wrote. Between tasks, data moves through storage, not through Airflow.

## Step 4: A task with no JVM, over Spark Connect

The second DAG asks a question over Spark Connect. The `@task.pyspark` decorator gives the function a `SparkSession`. With a `spark_connect` connection, that session is a thin Connect client, and the driver is the Connect server on `sc://localhost:15064`:

```python
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
```

`code/ch48/connect_server.sh` starts the server from the pip install of Spark 4.3.0, with gRPC on port 15064 and the UI on port 4064. The DAG run:

```text
09:56:10  running task busiest_zones
          CONNECT session: pyspark.sql.connect.session server 4.3.0
09:56:20  Done. Returned value was: [(237, 177610), (161, 161198), (236, 157113)]
09:56:20  task state -> success
09:56:20  running task report
          CONNECT zone 237: 177,610 pickups
          CONNECT zone 161: 161,198 pickups
          CONNECT zone 236: 157,113 pickups
09:56:20  task state -> success
09:56:20  DagRun Finished: run_duration=10.221635, state=success
```

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch48-connect.webp">
    <source src="../../media/clips/ch48-connect.mp4" type="video/mp4">
  </video>
  <figcaption>An Airflow task with no JVM sends its query to a Connect server. Then the 4.3 REST API of the server shows the session of the task and its one operation.</figcaption>
</figure>

The provider code that makes this session is `PySparkOperator.execute_callable`. It does these steps:

1. It reads the connection. If the connection type is `spark_connect`, it makes the `sc://` URL with `SparkConnectHook` and sets `spark.remote`. For other connection types, it sets `spark.master` (the default is `local[*]`).
2. It copies the extra fields of the connection, and then `config_kwargs`, into the session config.
3. It calls `getOrCreate()`, and it gives the session to the function as `spark`.
4. It calls `spark.stop()` in a `finally` block, also when the function fails.

The function returns a list of tuples. Airflow keeps the return value as an XCom in its metadata database. Thus, return small, plain data, such as counts, paths or IDs. Do not return a DataFrame or many rows.

Spark 4.3 adds sessions and operations to the monitoring REST API of the server ([Chapter 4](ch04-spark-connect.md#step-5-find-the-failure-on-the-server-new-in-43)). `code/ch48/connect_rest.py` reads these records after the DAG run:

```text
# GET .../connect/sessions
session 96d207f9  user=airflow  started 09:56:15  finished 09:56:20  operations=1

# GET .../connect/operations
operation eb52e92f  session 96d207f9  state=CLOSED  jobs=0,1,2  sql=0
  call site: ch48/dags/taxi_connect.py:28
```

The record shows these facts about the task:

- The user is `airflow`, from the login of the connection.
- The session has a finish time, because the operator stopped the session at the end of the task. Without this stop, the server keeps the session until its idle timeout.
- The call site is the line in the DAG file that built the query. When a Connect server runs queries for many DAGs, this field tells you which task sent which query.

## Step 5: Spark on Kubernetes from Airflow, in two ways

For production, the driver usually runs on the cluster, not on the Airflow worker. On Kubernetes, there are two methods to start it from Airflow:

- **`SparkSubmitOperator` with a `k8s://` master in cluster mode.** `spark-submit` creates the driver pod. The driver creates its executor pods.
- **A `SparkApplication` resource of the Apache Spark Kubernetes Operator.** Airflow creates the resource with `KubernetesCreateResourceOperator`. The Spark operator submits the application and records its state in the resource. Chapter 47 explains the operator and its resource.

`code/ch48/dags/taxi_k8s.py` does the two methods, one after the other, with the same job file. The first task:

```python
submit = SparkSubmitOperator(
    task_id="zone_revenue_submit",
    # master k8s://..., deploy-mode cluster, namespace spark. CH48_SUBMIT_CONN=spark_k8s_jobs uses the
    # namespace spark-jobs, where provider 6.3.4 cannot find the driver pod (see the chapter).
    conn_id=os.environ.get("CH48_SUBMIT_CONN", "spark_k8s"),
    application="local:///opt/ch48/jobs/zone_revenue.py",
    application_args=["/data/nyc-taxi", "-", "{{ run_id }}"],
    name="ch48-submit",
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
```

`YUNIKORN` and `VOLUMES` are two dictionaries at the top of the file. They send the pods to the YuniKorn queue `root.etl` (Step 8) and mount the taxi data and the job file from each node. The second and third tasks create the `SparkApplication` and wait for it:

```python
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
```

The resource is in `code/ch48/k8s/zone-revenue-app.yaml`. Airflow renders the file with Jinja, so that the resource name and the `run_id` change for each DAG run. `resourceRetainPolicy: Always` keeps the driver pod after the application ends, so that the last task can read its log. The DAG run:

```text
10:21:10  running task zone_revenue_submit
10:21:19  Identified spark driver pod: ch48-submit-42aae2a202c6409d-driver
10:21:19  Spark K8s driver pod submitted: spark:ch48-submit-42aae2a202c6409d-driver
10:21:19  Application status for ch48-submit-42aae2a202c6409d-driver (phase: Pending)
10:21:39  Application status for ch48-submit-42aae2a202c6409d-driver (phase: Running)
10:21:59  Application status for ch48-submit-42aae2a202c6409d-driver (phase: Running)
10:22:19  Application status for ch48-submit-42aae2a202c6409d-driver (phase: Succeeded)
10:22:19  Deleting driver pod ch48-submit-42aae2a202c6409d-driver on Kubernetes
10:22:19  Deleted driver pod ch48-submit-42aae2a202c6409d-driver
10:22:19  task state -> success
10:22:19  running task zone_revenue_app
10:22:19  task state -> success
10:22:19  running task wait_for_app
          APP zone-revenue-20261122t102108 state -
          APP zone-revenue-20261122t102108 state DriverRequested
          APP zone-revenue-20261122t102108 state RunningHealthy
          APP zone-revenue-20261122t102108 state RunningHealthy
          APP zone-revenue-20261122t102108 state RunningHealthy
          APP zone-revenue-20261122t102108 state TerminatedWithoutReleaseResources
          APP history: Submitted > DriverRequested > DriverStarted > DriverReady > RunningHealthy > Succeeded > TerminatedWithoutReleaseResources
10:23:10  task state -> success
10:23:10  running task driver_log
          APP driver pod zone-revenue-20261122t102108-0-driver, scheduler yunikorn, yunikorn app-id zone-revenue-20261122t102108-0
          ZONE_REVENUE run=manual__<run> spark=4.3.0 master=k8s://https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT
          ZONE_REVENUE Manhattan       3,374,269 trips  $    90,000,789
          ZONE_REVENUE Queens            319,164 trips  $    22,374,570
          ZONE_REVENUE Brooklyn          106,652 trips  $     4,107,067
          ZONE_REVENUE Bronx              14,955 trips  $       533,102
          ZONE_REVENUE N/A                 2,355 trips  $       258,590
          ZONE_REVENUE Unknown             4,109 trips  $       155,703
          ZONE_REVENUE EWR                   565 trips  $        67,976
          ZONE_REVENUE Staten Island         236 trips  $        12,809
          ZONE_REVENUE 8 rows in 7.9 s
10:23:10  task state -> success
10:23:10  DagRun Finished: run_duration=120.662846, state=success
```

```mermaid
sequenceDiagram
  participant W as Airflow worker
  participant S as spark-submit
  participant K as Kubernetes API
  participant D as driver pod
  W->>S: spark-submit --deploy-mode cluster<br/>waitAppCompletion=false
  S->>K: create driver pod
  S-->>W: "submission ID spark:...-driver", exit 0
  W->>W: keep "spark:...-driver" as the job ID
  loop each 20 s
    W->>K: read pod phase
    K-->>W: Pending, Running, ...
  end
  D->>K: create executor pods
  K-->>W: Succeeded
  W->>K: delete driver pod
```

With `track_driver_via_k8s_api=True`, the Airflow worker does not keep a `spark-submit` JVM for the full job. It polls the phase of the driver pod each 20 seconds (the minimum). On success, the hook deletes the driver pod.

The operator also has the parameter `durable`. Its default is `True`. With Airflow 3.3 or later, the operator saves the job ID (here, `spark:ch48-submit-...-driver`) in the task state store before it polls. If the Airflow worker stops and Airflow tries the task again, the new try finds the saved ID and polls the same driver pod. It does not submit a second copy of the job.

The same mechanism uses the driver ID in standalone cluster mode and the YARN application ID in YARN cluster mode. This chapter did not stop a worker to test this behavior.

!!! warning "Use a namespace named `spark` with `track_driver_via_k8s_api` in provider 6.3.4"
    Provider 6.3.4 finds the driver pod only in the namespace `spark`. In other namespaces, the task fails, and the driver pod continues to run.

The first run of this DAG used the namespace `spark-jobs`. To get this run again, set `CH48_SUBMIT_CONN=spark_k8s_jobs`:

```text
10:14:25  running task zone_revenue_submit
10:14:34  Deployed Spark application ch48-submit with application ID 
spark-c0bad92164d448bdb599ae89696f7c50 and submission ID 
spark-jobs:ch48-submit-ce4807a202c014c2-driver into Kubernetes
          raise RuntimeError("spark-submit did not capture a K8s driver pod name")
          RuntimeError: spark-submit did not capture a K8s driver pod name
10:14:34  task state -> failed
10:14:36  DagRun Finished: run_duration=11.450465, state=failed

$ kubectl -n spark-jobs get pods -l spark-role=driver
NAME                                  READY   STATUS    RESTARTS   AGE
ch48-submit-ce4807a202c014c2-driver   1/1     Running   0          47s
```

The cause is a regular expression in `SparkSubmitHook._process_spark_submit_log`. In cluster mode with `waitAppCompletion=false`, Spark logs only one line about the driver pod: `submission ID <namespace>:<pod name>`. The hook looks for `submission ID spark:(.+?-driver)`. Thus, it matches only when the namespace is the literal text `spark`. The unit test of the provider also uses a namespace named `spark`, so the test cannot find this problem.

This failure has two bad results. The Airflow task fails, but the Spark job continues on the cluster. If the task has retries, each retry submits one more copy of the job. The notes of this chapter record this problem as an open problem. Until a provider release corrects it, use one of these workarounds:

- Run the Spark pods in a namespace named `spark`, as this chapter does.
- Set `track_driver_via_k8s_api=False`. Then `spark-submit` waits for the application, and the hook looks for the `pod name:` lines that Spark logs during the wait. This chapter did not test this mode on Kubernetes.
- Use a `SparkApplication` and a sensor, as in the second half of this DAG.

**Which method to use.** `SparkSubmitOperator` keeps all the Spark settings in the DAG, and it needs only the Spark tarball and access to the Kubernetes API. A `SparkApplication` keeps the settings in a Kubernetes resource that `kubectl` can read and change. The Spark operator can also try a failed application again and remove its pods. Thus, Airflow does less, and the cluster does more. With a `SparkApplication`, the Airflow worker needs no Spark installation.

## Step 6: `SparkKubernetesOperator` is for the Kubeflow operator

The Kubernetes provider has an operator with a name that seems correct for the second method: `SparkKubernetesOperator`. But it is not the correct operator.

The docstring of this operator links to the API of the Kubeflow Spark operator (`sparkoperator.k8s.io/v1beta2`). Its code reads `spec.driver` and `status.applicationState.state`. These fields are in the Kubeflow resource. The resource of the Apache operator (`spark.apache.org/v1`) has `driverSpec` and `status.currentState`. `code/ch48/dags/wrong_operator.py` gives the `pi-python` example of the Apache operator to `SparkKubernetesOperator`:

```text
10:23:49  running task apache_app_with_kubeflow_operator
10:23:53  Creating sparkApplication.
          labels=self.spark_obj_spec["spec"]["driver"].get("labels"),
          KeyError: 'driver'
10:23:53  task state -> failed
10:23:53  DagRun Finished: run_duration=4.00715, state=failed

$ kubectl -n spark-apps get sparkapplications
NAME                 AGE   CURRENT STATE
pi-python-udtf08cz   14s   DriverReady
```

The operator created the resource first, and then it failed when it read `spec.driver`. Thus, the Spark application runs on the cluster after the Airflow task fails. With retries, each retry starts one more application.

There are two Kubernetes operators for Spark. Their resources have the same kind name, `SparkApplication`, but they are not compatible:

| | Kubeflow Spark operator | Apache Spark Kubernetes Operator |
|---|---|---|
| Project | [kubeflow/spark-operator](https://github.com/kubeflow/spark-operator), latest release v2.6.0 (11 November 2026) | [apache/spark-kubernetes-operator](https://github.com/apache/spark-kubernetes-operator), latest release 1.1.0 |
| Resource | `sparkoperator.k8s.io/v1beta2` `SparkApplication`, `ScheduledSparkApplication` | `spark.apache.org/v1` `SparkApplication`, `SparkCluster` |
| How it submits | The operator (in Go) runs `spark-submit` in its own pod. The image of v2.6.0 has Spark 4.2.1. | The operator (in Java) submits through the Spark Kubernetes client in its own JVM (Chapter 47) |
| Pod changes | A mutating admission webhook changes driver and executor pods | Pod templates and `sparkConf` |
| From Airflow | `SparkKubernetesOperator` and `SparkKubernetesSensor` (cncf.kubernetes provider) | `KubernetesCreateResourceOperator` and a sensor that reads `status.currentState` (Step 5) |

For a new installation on Spark 4.3, this book uses the Apache operator. It is a subproject of Apache Spark, and the examples of its 1.1.0 release use Spark 4.3.0. If a cluster already runs the Kubeflow operator, `SparkKubernetesOperator` is the operator to use with it.

## Step 7: Why Spark needs a batch scheduler on Kubernetes

The default Kubernetes scheduler places one pod at a time. It does not know that a driver pod is useless without executor pods. This test sends two Spark jobs at the same time to a queue with a maximum of 2 cores. Each job asks for one driver and two executors, with 1 core each. `code/ch48/yunikorn_demo.sh plain` sends the two jobs and prints each change of pod phase:

```text
t+   0s  etl-a driver                   Pending
t+   0s  etl-b driver                   Pending
t+   3s  etl-a driver                   Running
t+   3s  etl-b driver                   Running
t+   9s  etl-a exec-1                   Pending
t+   9s  etl-a exec-2                   Pending
t+   9s  etl-b exec-1                   Pending
t+   9s  etl-b exec-2                   Pending
yunikorn  etl-b  Running    cores used 1, cores waiting 2
yunikorn  etl-a  Running    cores used 1, cores waiting 2
yunikorn  queue root.etl  cores used 2 of max 2
Request <exec-1> does not fit in queue 'root.etl' (requested map[memory:1502609408 pods:1 
vcore:1000], available map[memory:5584715776 pods:328 vcore:0])
WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI 
to ensure that workers are registered and have sufficient resources
```

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch48-plain.webp">
    <source src="../../media/clips/ch48-plain.mp4" type="video/mp4">
  </video>
  <figcaption>Two drivers start and use the two cores of the queue. Their four executors stay `Pending`, so no job can make progress.</figcaption>
</figure>

Each driver started, and each driver used 1 core. Thus, the queue had no free cores for executors. The two drivers wait for executors that cannot start, and the scheduler waits for cores that the drivers do not release. Neither job can end. The driver log says only that the job "has not accepted any resources". The script stops the two jobs after the time limit.

This test uses a YuniKorn queue without gang scheduling, so that the limit is small and easy to see. The same deadlock can occur with the default scheduler when the cluster or a namespace quota is full of driver pods.

## Step 8: Gang scheduling and queues with YuniKorn

Gang scheduling starts an application only when the scheduler can place a minimum set of its pods together. YuniKorn does this with placeholder pods. Before the real pods start, YuniKorn creates placeholder pods that request the same resources. When it can place all the placeholders, it replaces each placeholder with a real pod.

`code/ch48/yunikorn.sh` installs YuniKorn 1.10.0 with Helm. The admission controller is off, as in the Spark 4.3 docs. Thus, only the pods that ask for the scheduler `yunikorn` go to YuniKorn. The Helm values also define the queues:

```yaml
yunikornDefaults:
  queues.yaml: |
    partitions:
      - name: default
        placementrules:
          - name: provided       # the queue that the pod names in its "queue" label
            create: false
          - name: fixed          # all other pods
            value: root.default
        queues:
          - name: root
            submitacl: "*"
            queues:
              - name: default
              - name: etl        # a small queue: 2 cores in total, for driver and executor pods
                resources:
                  max: {vcore: "2", memory: 8Gi}
```

The `provided` placement rule puts each pod in the queue that its `queue` label names. A Spark job selects YuniKorn with three settings. The `{{APP_ID}}` annotation gives the driver and its executors one application ID in YuniKorn:

```bash
# YuniKorn: the scheduler name, the queue, and one application ID for the driver and its executors
--conf $K.scheduler.name=yunikorn
--conf $K.driver.label.queue="$QUEUE" --conf $K.executor.label.queue="$QUEUE"
--conf $K.driver.annotation.yunikorn.apache.org/app-id='{{APP_ID}}'
--conf $K.executor.annotation.yunikorn.apache.org/app-id='{{APP_ID}}'
```

For gang scheduling, the driver pod also carries the task groups of the application. In this test, the minimum is one driver and one executor:

```bash
# One driver and MIN_EXEC executors, each 1 CPU and 1433Mi (1g of heap plus 40%: the overhead factor for Python jobs).
groups='[{"name":"driver","minMember":1,"minResource":{"cpu":"1","memory":"1433Mi"}},
         {"name":"executor","minMember":'$MIN_EXEC',"minResource":{"cpu":"1","memory":"1433Mi"}}]'
args+=(
  --conf $K.driver.annotation.yunikorn.apache.org/task-group-name=driver
  --conf $K.driver.annotation.yunikorn.apache.org/task-groups="$(echo $groups | tr -d ' ')"
  --conf $K.driver.annotation.yunikorn.apache.org/schedulingPolicyParameters="placeholderTimeoutInSeconds=120 gangSchedulingStyle=Hard"
  --conf $K.executor.annotation.yunikorn.apache.org/task-group-name=executor
)
```

The memory of each task group is 1433Mi: 1 GB of heap plus 40%. 40% is the memory overhead factor that Spark uses for Python applications on Kubernetes. If the task group asks for less than the real pod, the real pod does not fit in the space of its placeholder. `gangSchedulingStyle=Hard` fails the application if YuniKorn cannot place all the placeholders before the timeout. The default style, `Soft`, changes to normal scheduling after the timeout ([YuniKorn gang scheduling guide](https://yunikorn.apache.org/docs/user_guide/gang_scheduling)).

This is one of the placeholder pods that YuniKorn made. It runs the `pause` image and requests the resources of its task group:

```json
{
  "name": "tg-spark-716f3c872a014860b7b3b8-executor-36t62efbg3",
  "labels": {
    "yunikorn.apache.org/app-id": "spark-716f3c872a014860b7b3b88057c9454c",
    "yunikorn.apache.org/queue": "root.etl"
  },
  "scheduler": "yunikorn",
  "annotations": {
    "internal.yunikorn.apache.org/placeholder": "true",
    "yunikorn.apache.org/schedulingPolicyParameters": "placeholderTimeoutInSeconds=120 gangSchedulingStyle=Hard",
    "yunikorn.apache.org/task-group-name": "executor"
  },
  "image": "registry.k8s.io/pause:3.7",
  "requests": {
    "cpu": "1",
    "memory": "1433Mi"
  }
}
```

The same two jobs, now with gang scheduling:

```text
t+   0s  etl-a driver                   Pending
t+   0s  etl-a placeholder (driver)     Pending
t+   0s  etl-a placeholder (executor)   Pending
t+   0s  etl-b driver                   Pending
t+   0s  etl-b placeholder (driver)     Pending
t+   0s  etl-b placeholder (executor)   Pending
t+   2s  etl-a placeholder (executor)   Running
t+   2s  etl-a placeholder (driver)     deleted
t+   4s  etl-a driver                   Running
t+  10s  etl-a exec-1                   Pending
t+  10s  etl-a exec-2                   Pending
t+  10s  etl-a placeholder (executor)   deleted
t+  12s  etl-a exec-1                   Running
t+  52s  etl-a exec-2                   deleted
t+  52s  etl-a exec-1                   deleted
t+  53s  etl-a driver                   Completed
t+  53s  etl-b placeholder (driver)     Running
t+  55s  etl-b driver                   Running
t+  55s  etl-b placeholder (executor)   Running
t+  55s  etl-b placeholder (driver)     deleted
t+  62s  etl-b exec-1                   Running
t+  62s  etl-b exec-2                   Pending
t+  62s  etl-b placeholder (executor)   deleted
t+ 104s  etl-b exec-1                   deleted
t+ 104s  etl-b exec-2                   deleted
t+ 105s  etl-b driver                   Completed
yunikorn  etl-b  Completing cores used 0, cores waiting 0
yunikorn  queue root.etl  cores used 0 of max 2
```

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch48-gang.webp">
    <source src="../../media/clips/ch48-gang.mp4" type="video/mp4">
  </video>
  <figcaption>With gang scheduling, YuniKorn reserves a driver and an executor for one job. The other job waits, and it starts when the first job ends. In this video, etl-b got its gang first. The order of two jobs that arrive at the same time can change from run to run.</figcaption>
</figure>

YuniKorn reserved the two cores of the queue for `etl-a`: one placeholder for the driver and one for an executor. The real driver replaced its placeholder, and `exec-1` replaced the executor placeholder. `exec-2` stayed `Pending`, because the gang reserves the minimum, not all the executors that the job asks for. The job ran with one executor.

The placeholders of `etl-b` waited until `etl-a` ended. Then `etl-b` ran in the same way. The two jobs ran one after the other, and each job completed with the same result:

```text
ZONE_REVENUE run=etl-a spark=4.3.0 master=k8s://https://127.0.0.1:40611
ZONE_REVENUE Manhattan       3,374,269 trips  $    90,000,789
ZONE_REVENUE Queens            319,164 trips  $    22,374,570
ZONE_REVENUE Brooklyn          106,652 trips  $     4,107,067
ZONE_REVENUE Bronx              14,955 trips  $       533,102
ZONE_REVENUE N/A                 2,355 trips  $       258,590
ZONE_REVENUE Unknown             4,109 trips  $       155,703
ZONE_REVENUE EWR                   565 trips  $        67,976
ZONE_REVENUE Staten Island         236 trips  $        12,809
ZONE_REVENUE 8 rows in 9.9 s
```

```mermaid
flowchart LR
  classDef bad fill:#a33,fill-opacity:0.72,stroke:#a33,color:#ffffff
  classDef good fill:#4a7a4a,fill-opacity:0.72,stroke:#4a7a4a,color:#ffffff
  subgraph plain["no gang (2 cores)"]
    a1["etl-a driver: 1 core"]:::bad
    b1["etl-b driver: 1 core"]:::bad
    ax["4 executors: Pending"]:::bad
    a1 --> ax
    b1 --> ax
  end
  subgraph gang["gang (2 cores)"]
    a2["etl-a driver + exec-1<br/>(placeholders, then real pods)"]:::good
    b2["etl-b placeholders<br/>wait"]:::good
    a2 -->|"etl-a ends"| b2
  end
```

If a gang is larger than the maximum of its queue, YuniKorn rejects the application. It does not let the application wait forever. Here, each job asks for a gang of one driver and two executors (3 cores) in a queue of 2 cores:

```text
NAME                            READY   STATUS                RESTARTS   AGE
etl-a-e673f3a202b39895-driver   0/1     ApplicationRejected   0          12s
etl-b-8f32b3a202b39b3f-driver   0/1     ApplicationRejected   0          12s
Application spark-1b24c0d907ca42e0a37bbfc9357e0a15 scheduling failed, reason: 
ApplicationRejected: queue root.etl cannot fit application 
spark-1b24c0d907ca42e0a37bbfc9357e0a15: task group request map[memory:4507828224 pods:3 
vcore:3000] larger than max queue allocation map[memory:8589934592 vcore:2000]
```

YuniKorn queues also give each team a share of the cluster. A queue has a guaranteed and a maximum share of resources, and the queues have a hierarchy (`root.etl`, `root.default`). The scheduler sorts the applications in a queue by a policy, for example FIFO or fair. The gang scheduling guide recommends a FIFO policy with gangs. With a different policy, the scheduler can reserve part of the resources for many applications at the same time.

## Step 9: Volcano

[Volcano](https://volcano.sh/en) is a batch scheduler for Kubernetes from the CNCF. It also gives queues, gang scheduling (with a `PodGroup` resource), and priorities. Spark supports Volcano with a feature step in its Kubernetes client, since Spark 3.3.0 and Volcano 1.7.0 ([running on Kubernetes, Volcano section](https://spark.apache.org/docs/4.3.0/running-on-kubernetes.html#using-volcano-as-customized-scheduler-for-spark-on-kubernetes)). The release tarball of Spark 4.3.0 contains this feature step: `spark-kubernetes_2.13-4.3.0.jar` has the class `VolcanoFeatureStep`, and `jars/` has `volcano-client-7.6.1.jar`. A Spark job selects Volcano with these settings, from the Spark 4.3 docs:

```bash
--conf spark.kubernetes.scheduler.name=volcano
--conf spark.kubernetes.scheduler.volcano.podGroupTemplateFile=/path/to/podgroup-template.yaml
--conf spark.kubernetes.driver.pod.featureSteps=org.apache.spark.deploy.k8s.features.VolcanoFeatureStep
--conf spark.kubernetes.executor.pod.featureSteps=org.apache.spark.deploy.k8s.features.VolcanoFeatureStep
```

The feature step creates a `PodGroup` for each application from the template. The template sets the minimum resources, the queue and the priority. YuniKorn and Volcano solve the same problem with different methods:

- YuniKorn uses placeholder pods and annotations on the driver pod. It does not need a change to the Spark pods beyond labels and annotations.
- Volcano uses its own `PodGroup` resource, which Spark creates through the feature step.

This chapter ran only YuniKorn. The Volcano facts in this section come from the Spark 4.3 docs and the release jars, not from a run.

## Step 10: Remote shuffle with Celeborn

Dynamic allocation adds executors when tasks wait, and it removes executors that have no tasks. But an executor that wrote shuffle files holds data that a later stage needs. Spark cannot remove that executor unless the shuffle data is in a different location. `ExecutorAllocationManager.validateSettings` in Spark 4.3 accepts dynamic allocation only with one of these four conditions:

1. An external shuffle service (`spark.shuffle.service.enabled`).
2. Shuffle tracking (`spark.dynamicAllocation.shuffleTracking.enabled`, `true` by default).
3. Shuffle block decommission, which moves shuffle blocks to other executors before an executor stops.
4. A shuffle plugin (`spark.shuffle.sort.io.plugin.class`) whose driver part tells Spark that its storage is reliable.

On YARN, the usual choice is the external shuffle service, which runs in each NodeManager. The Spark 4.3 docs say that "Kubernetes doesn't support an external shuffle service at this time". Thus, on Kubernetes, dynamic allocation usually uses shuffle tracking. Shuffle tracking keeps an executor alive while it holds shuffle data that a job can need. The default timeout for that data, `spark.dynamicAllocation.shuffleTracking.timeout`, has no limit.

Celeborn is a remote shuffle service. Executors push their shuffle data to Celeborn workers, and reducers read it from them. The executors do not keep the data. Celeborn implements condition 4 with `CelebornShuffleDataIO`. Thus, Spark can remove an executor and keep its shuffle output.

```mermaid
flowchart LR
  classDef spark fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  classDef clb fill:#4a7a4a,fill-opacity:0.72,stroke:#4a7a4a,color:#ffffff
  m1["executor 1<br/>(map tasks)"]:::spark -->|push| w1["Celeborn worker 0"]:::clb
  m2["executor 2<br/>(map tasks)"]:::spark -->|push| w2["Celeborn worker 1"]:::clb
  w1 <-->|replicate| w2
  d["driver<br/>LifecycleManager"]:::spark -->|"request slots"| cm["Celeborn master"]:::clb
  w1 -->|fetch| r["executor 3<br/>(reduce tasks, started later)"]:::spark
  w2 -->|fetch| r
```

`code/ch48/celeborn.sh` installs Celeborn 0.7.0 from the Helm chart in its source release. The cluster has one master and two workers, one worker on each kind node (values in `code/ch48/k8s/celeborn-values.yaml`). The client jar is `org.apache.celeborn:celeborn-client-spark-4-shaded_2.13:0.7.0` from Maven Central. `kind_up.sh` downloads it and checks its SHA-1.

**Celeborn 0.7.0 supports Spark 4.0, 4.1 and 4.2 with Java 17 and Scala 2.13** (the compatibility table of its README). It does not support Spark 4.3. Thus, the run in this step uses Spark 4.2.1, in the image `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu`. This is the only part of the book that does not run on Spark 4.3. Step 11 shows what happens on 4.3.0.

The test job, `code/ch48/jobs/shuffle_idle.py`, runs one aggregation with a shuffle. Then it waits 45 seconds with no tasks, and it runs the same action on the same DataFrame again. Dynamic allocation can use 0 to 2 executors, and an idle executor stops after 15 seconds. `code/ch48/celeborn_job.sh` adds the Celeborn settings from the Celeborn README:

```bash
# the Celeborn client jar on the driver and executor class paths (a hostPath volume on every node)
--conf $K.driver.volumes.hostPath.jars.mount.path=/opt/ch48/jars
--conf $K.driver.volumes.hostPath.jars.options.path=/opt/ch48/jars
--conf $K.executor.volumes.hostPath.jars.mount.path=/opt/ch48/jars
--conf $K.executor.volumes.hostPath.jars.options.path=/opt/ch48/jars
--conf spark.driver.extraClassPath=$JAR --conf spark.executor.extraClassPath=$JAR
# the settings from the Celeborn 0.7.0 README
--conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
--conf spark.celeborn.master.endpoints=celeborn-master-0.celeborn-master-svc.celeborn.svc.cluster.local:9097
--conf spark.shuffle.service.enabled=false
--conf spark.celeborn.client.push.replicate.enabled=true
--conf spark.sql.adaptive.localShuffleReader.enabled=false
--conf spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO
--conf spark.dynamicAllocation.shuffleTracking.enabled=false
```

First, the same job without Celeborn, with Spark's own shuffle and shuffle tracking (the default). On Spark 4.2.1:

```text
SHUFFLE t+  0s  spark 4.2.1, shuffle manager sort, shuffle tracking true
SHUFFLE t+  9s  action 1 done: busiest zone 237, 177,610 trips; executors alive: 2
SHUFFLE t+ 54s  after 45 s with no tasks: executors alive: 2
SHUFFLE t+ 55s  action 2 done: 39 zones over $1M; executors alive: 2
SHUFFLE t+ 55s  stage  0 COMPLETE  tasks=1   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 55s  stage  1 COMPLETE  tasks=2   shuffle write=   10,547 B  read=        0 B
SHUFFLE t+ 55s  stage  2 SKIPPED   tasks=2   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 55s  stage  3 COMPLETE  tasks=1   shuffle write=        0 B  read=   10,547 B
SHUFFLE t+ 55s  stage  4 SKIPPED   tasks=2   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 55s  stage  5 COMPLETE  tasks=1   shuffle write=        0 B  read=   10,547 B
SHUFFLE t+ 55s  executor  1 active=True  removed: -
SHUFFLE t+ 55s  executor  2 active=True  removed: -
```

And on Spark 4.3.0, with the same result:

```text
SHUFFLE t+  0s  spark 4.3.0, shuffle manager sort, shuffle tracking true
SHUFFLE t+  9s  action 1 done: busiest zone 237, 177,610 trips; executors alive: 2
SHUFFLE t+ 54s  after 45 s with no tasks: executors alive: 2
SHUFFLE t+ 54s  action 2 done: 39 zones over $1M; executors alive: 2
SHUFFLE t+ 55s  stage  0 COMPLETE  tasks=1   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 55s  stage  1 COMPLETE  tasks=2   shuffle write=   10,547 B  read=        0 B
SHUFFLE t+ 55s  stage  2 SKIPPED   tasks=2   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 55s  stage  3 COMPLETE  tasks=1   shuffle write=        0 B  read=   10,547 B
SHUFFLE t+ 55s  stage  4 SKIPPED   tasks=2   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 55s  stage  5 COMPLETE  tasks=1   shuffle write=        0 B  read=   10,547 B
SHUFFLE t+ 55s  executor  1 active=True  removed: -
SHUFFLE t+ 55s  executor  2 active=True  removed: -
```

After 45 seconds with no tasks, the 2 executors were still alive. The idle timeout is 15 seconds, but shuffle tracking kept the executors, because they held the shuffle output of stage 1. Stages 2 and 4 are `SKIPPED`. The second action read the shuffle output of the first action, so Spark did not run the map stage again. Thus, the job got the shuffle data, but it kept 2 executors for no work.

Now the same job on Spark 4.2.1 with Celeborn:

```text
SHUFFLE t+  0s  spark 4.2.1, shuffle manager SparkShuffleManager, shuffle tracking false
SHUFFLE t+ 10s  action 1 done: busiest zone 237, 177,610 trips; executors alive: 2
SHUFFLE t+ 55s  after 45 s with no tasks: executors alive: 0
SHUFFLE t+ 63s  action 2 done: 39 zones over $1M; executors alive: 1
SHUFFLE t+ 63s  stage  0 COMPLETE  tasks=1   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 63s  stage  1 COMPLETE  tasks=2   shuffle write=   10,467 B  read=        0 B
SHUFFLE t+ 63s  stage  2 SKIPPED   tasks=2   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 63s  stage  3 COMPLETE  tasks=1   shuffle write=        0 B  read=   10,467 B
SHUFFLE t+ 63s  stage  4 SKIPPED   tasks=2   shuffle write=        0 B  read=        0 B
SHUFFLE t+ 63s  stage  5 COMPLETE  tasks=1   shuffle write=        0 B  read=   10,467 B
SHUFFLE t+ 63s  executor  1 active=False removed: Executor killed by driver.
SHUFFLE t+ 63s  executor  2 active=False removed: Executor killed by driver.
SHUFFLE t+ 63s  executor  3 active=True  removed: -
CELEBORN 10:13:11  Master: Successfully offered slots for 8 reducers of spark-47cdf7fc…-0 on 2 workers, primary types: Map(HDD -> 8).
CELEBORN 10:14:08  Master: Removed application spark-47cdf7fc…
```

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch48-celeborn.webp">
    <source src="../../media/clips/ch48-celeborn.mp4" type="video/mp4">
  </video>
  <figcaption>With Celeborn, the two executors stop when they become idle. A new executor reads the shuffle output from the Celeborn workers, and Spark skips the map stage.</figcaption>
</figure>

These are the results:

- After the gap, 0 executors were alive. Executors 1 and 2 stopped with the reason "Executor killed by driver" (the idle timeout).
- The second action started a new executor, 3. Spark still skipped the map stage (stage 4 is `SKIPPED`), and stage 5 read the same 10,467 bytes that stage 1 wrote. The data came from the Celeborn workers, because executors 1 and 2 no longer existed.
- The Celeborn master gave slots to one shuffle (`-0`) of this application, for 8 reducers on 2 workers. The second action did not register a second shuffle.

The difference matters most on a shared cluster. A job with a long tail, or a pipeline that waits between stages, does not keep idle executors. On Kubernetes, these executors are pods that hold CPU and memory that other jobs cannot use. Celeborn also keeps the shuffle data when an executor pod stops for a different reason. For example, Kubernetes can remove a pod from a node.

The shuffle in this test is small. The test shows the behavior of dynamic allocation. It does not measure the speed of Celeborn compared to Spark's own shuffle, and this chapter does not claim a speed difference.

## Step 11: Celeborn on Spark 4.3.0 (not supported)

The same Celeborn 0.7.0 client, with the same settings, on Spark 4.3.0 (`apache/spark:4.3.0-scala2.13-java17-python3-ubuntu`). The driver stops before the job starts:

```text
java.lang.IllegalArgumentException: spark.shuffle.manager must be a BlockingShuffleManager, 
but org.apache.spark.shuffle.celeborn.SparkShuffleManager is not. A blocking manager serves 
regular, materialized shuffles and resolves blocks by id; configure a pipelined manager via 
spark.shuffle.manager.incremental instead.
```

Spark 4.3 changed the shuffle manager API (SPARK-58185). `ShuffleManager` now has two kinds:

- `BlockingShuffleManager`: materialized shuffle output that the block manager serves. `SortShuffleManager` is this kind. `spark.shuffle.manager` must name a manager of this kind.
- `PipelinedShuffleManager`: output that a later stage can read while the earlier stage still runs. `spark.shuffle.manager.incremental` names this manager.

`SparkEnv.initializeShuffleManager` rejects a `spark.shuffle.manager` that does not extend `BlockingShuffleManager`. The Celeborn 0.7.0 client was built for Spark 4.2 and does not extend this trait. Thus, Celeborn needs a new client release for Spark 4.3. On 22 November 2026, no Celeborn release supports Spark 4.3, and the `pom.xml` of the Celeborn main branch has profiles only up to `spark-4.2`. The core migration guide of Spark 4.3 does not list this change. Other shuffle plugins that set `spark.shuffle.manager` can have the same problem.

!!! warning "Do not upgrade a Celeborn cluster to Spark 4.3 yet"
    Celeborn 0.7.0 does not work with Spark 4.3.0. The driver stops at start. Keep Spark 4.2 for jobs that use Celeborn until a Celeborn release supports Spark 4.3.

## When to use which

| You need | Use | The driver runs |
|---|---|---|
| A batch job on a small cluster, or a laptop, with Spark installed on the Airflow worker | `SparkSubmitOperator`, client mode | in the Airflow worker |
| A SQL file as a task | `SparkSqlOperator` | in the Airflow worker |
| A Spark Declarative Pipeline (`dry-run` in CI, then `run`) | `SparkPipelinesOperator` | in the Airflow worker, or on a Connect server |
| Short queries from many DAGs, no JVM on the workers | `@task.pyspark` with a `spark_connect` connection | on a Connect server that runs all the time |
| A job on Kubernetes, with all the settings in the DAG | `SparkSubmitOperator`, `k8s://` master, cluster mode, `track_driver_via_k8s_api=True` | in a pod |
| A job on Kubernetes, with the settings in a resource and retries in the cluster | `KubernetesCreateResourceOperator` with a `SparkApplication` of the Apache operator, and a sensor | in a pod that the Spark operator creates |
| A cluster that already runs the Kubeflow Spark operator | `SparkKubernetesOperator` | in a pod that the Kubeflow operator creates |

On a Kubernetes cluster that many Spark jobs share, add a batch scheduler. Use YuniKorn or Volcano for queues, and use gang scheduling so that drivers cannot use all the resources. Add Celeborn when you use dynamic allocation and want idle executors to stop. Make sure that the Celeborn release supports your Spark version.

## The whole chapter, on one page

- **Airflow starts or calls a driver, then it waits.** The operator controls the location of the driver:
    - the Airflow worker (`spark-submit`, `spark-sql`, `spark-pipelines`)
    - a Connect server (`@task.pyspark`)
    - a pod (cluster mode, or a `SparkApplication`)
- **Each operator is a hook that builds a command.** `commands.py` prints them. Read them when a task does something that you did not expect.
- **Template fields are Jinja.** Use `{{ '{{APP_ID}}' }}` for Spark's own placeholder.
- **Data moves between tasks through storage.** An XCom is for small, plain values.
- **Over Connect, the task has no JVM.** The 4.3 REST API shows the session, the user and the call site of each task.
- **`track_driver_via_k8s_api` releases the `spark-submit` JVM** and polls the driver pod. In provider 6.3.4, it works only in a namespace named `spark`.
- **`SparkKubernetesOperator` is for the Kubeflow operator.** For the Apache operator, create the `SparkApplication` and poll `status.currentState`.
- **The default Kubernetes scheduler can deadlock Spark.** Drivers use the resources, and executors wait. YuniKorn gang scheduling reserves a driver and its minimum executors together, and it rejects a gang that cannot fit in its queue.
- **On Kubernetes, dynamic allocation keeps executors that hold shuffle data.** Celeborn moves the shuffle data to its own workers, so idle executors stop and Spark still skips the finished map stage.
- **Celeborn 0.7.0 supports Spark up to 4.2.** Spark 4.3.0 rejects its shuffle manager.

The commands, in order:

```bash
cd code
ch48/airflow_env.sh                      # Airflow 3.4.0 + provider 6.3.4 in a separate venv
ch48/af.sh db migrate
ch48/af.sh dags test ch48_taxi_submit    # spark-submit, then spark-sql
ch48/connect_server.sh start
ch48/af.sh dags test ch48_taxi_connect   # @task.pyspark over Spark Connect
SPARK_UI_PORT=4064 uv run python ch48/connect_rest.py
ch48/kind_up.sh                          # kind cluster "ch48", images, Celeborn client jar
ch48/yunikorn.sh && ch48/celeborn.sh && ch48/spark_operator.sh
ch48/af.sh dags test ch48_taxi_k8s       # cluster mode, then a SparkApplication
ch48/yunikorn_demo.sh plain 40           # deadlock without gang scheduling
ch48/yunikorn_demo.sh gang 240           # gang scheduling
ch48/celeborn_job.sh tracking 4.3.0      # dynamic allocation with shuffle tracking
ch48/celeborn_job.sh celeborn 4.2.1      # dynamic allocation with Celeborn
ch48/kind_up.sh delete
```

## Further reading

- [Apache Spark provider for Airflow](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/6.3.4/), the operators, hooks and connection types of the provider version in this chapter.
- [Running Spark on Kubernetes](https://spark.apache.org/docs/4.3.0/running-on-kubernetes.html), with the YuniKorn and Volcano sections and the note about the external shuffle service.
- [Dynamic resource allocation](https://spark.apache.org/docs/4.3.0/job-scheduling.html#dynamic-resource-allocation), the four conditions of Step 10.
- [YuniKorn gang scheduling](https://yunikorn.apache.org/docs/user_guide/gang_scheduling) and [YuniKorn with Spark](https://yunikorn.apache.org/docs/user_guide/workloads/run_spark).
- [Apache Celeborn](https://celeborn.apache.org/), and the compatibility table in its [README](https://github.com/apache/celeborn/blob/v0.7.0/README.md).
- [Kubeflow Spark operator](https://github.com/kubeflow/spark-operator) and the [Apache Spark Kubernetes Operator](https://github.com/apache/spark-kubernetes-operator).
- [Volcano](https://volcano.sh/en).
