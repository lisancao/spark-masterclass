# Chapter 32: Declarative Pipelines: the model

A batch job that builds two tables is easy to write. The job reads a file, writes a table, reads that table and writes a second table.

The hard part comes later. You must run the steps in the correct order. You must select a write mode for each table. A streaming step needs a checkpoint, and somebody must remember where it is. When the job runs a second time, each of these decisions must still be correct.

Spark Declarative Pipelines (SDP) moves these decisions out of your code. SDP came into Apache Spark in 4.1. Spark 4.2 added Auto CDC, and Spark 4.3 added history tables to Auto CDC ([Chapter 34](ch34-change-data-capture.md)).

**A pipeline is a graph of datasets. Your Python and SQL files only declare the datasets and the flows that fill them. The server finds the dependencies and does a check of the full graph. Then it selects the order, the write mode and the checkpoint of each flow.** This chapter shows each part of that sentence with a real project: its files, its graph, its errors and its runs.

This chapter is about the model. The chapters before it show how to use the tools:

- [Chapter 77](ch77-first-etl-pipeline.md) builds and runs a first pipeline, step by step.
- [Chapter 78](ch78-pipelines-cli-project.md) explains the `spark-pipelines` CLI, the spec file, the project layout, `dry-run` and the refresh options.
- [Chapter 79](ch79-configuring-pipelines.md) explains the configuration, the catalog and the database, and one project in more than one environment.

The code is in [`code/ch32`](https://github.com/lisancao/spark-masterclass/tree/main/code/ch32). Each output block comes from `code/ch32/output/`. All runs use `pyspark==4.3.0`. The script `code/common/pipeline.sh` copies a project to a work folder, starts a Connect server there and runs `spark-pipelines` against it.

## Step 1: The imperative job, and its second run

`imperative.py` builds two tables in a Classic session. The first table holds the product list. The second table holds a summary for each category:

```python
def build():
    # The order is ours to get right: products first, then the summary that reads it.
    (spark.read.option("header", True)
     .schema("product_id STRING, name STRING, category STRING, list_price DECIMAL(10,2)")
     .csv(products_csv)
     .write.saveAsTable("products"))
    (spark.table("products").groupBy("category")
     .agg(F.count("*").alias("products"), F.avg("list_price").alias("avg_list_price"))
     .write.saveAsTable("products_by_category"))


build()
spark.table("products_by_category").orderBy("category").show()
try:
    build()   # the second run of the same job
except Exception as e:
    print("second run:", e.getCondition())
```

```text
+--------+--------+--------------+
|category|products|avg_list_price|
+--------+--------+--------------+
|hardware|       2|     17.245000|
|   merch|       2|     61.625000|
+--------+--------+--------------+

second run: TABLE_OR_VIEW_ALREADY_EXISTS
```

The first run is correct. The second run fails, because the default save mode of `saveAsTable` is `errorifexists`.

To make the job safe for a second run, you must select a mode for each table. If one step reads a stream, you must also keep a checkpoint path for it. If you add a third table, you must put it in the correct position in the code. The code that controls these decisions grows with each table. It does not hold any of the business logic.

## Step 2: The same tables, declared

`retail/transformations/products.py` declares the same two tables:

```python
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PRODUCT_SCHEMA = "product_id STRING, name STRING, category STRING, list_price DECIMAL(10,2)"


@dp.materialized_view(comment="The product list, read in full on every run")
def products() -> DataFrame:
    return spark.read.option("header", True).schema(PRODUCT_SCHEMA).csv("data/products.csv")


@dp.materialized_view
def products_by_category() -> DataFrame:
    print("query function products_by_category: called")   # shows when SDP calls it
    return (spark.table("products").groupBy("category")
            .agg(F.count("*").alias("products"), F.avg("list_price").alias("avg_list_price")))
```

Each function returns a DataFrame and does nothing more. It does not write, and it does not select a mode. The name of the function is the name of the table. The `print` line is there only to show when SDP calls the function (Step 3).

SDP has three concepts:

- **A dataset** is an object that the pipeline makes and that a query can read. There are three types: a *materialized view*, a *streaming table* and a *temporary view*. Step 5 compares them.
- **A flow** is a query that reads one or more sources and writes to one dataset. `@dp.materialized_view` makes a dataset and its flow together. A streaming table can have more than one flow (Step 6).
- **A pipeline** is the unit that you run. It is a spec file, `spark-pipeline.yml`, and the source files that the spec names ([Chapter 78](ch78-pipelines-cli-project.md)).

The full retail project has three source files. `orders.py` adds a streaming table of orders, a temporary view of the placed orders and a count by status. `placed_by_customer.sql` adds a materialized view in SQL that reads the temporary view from Python. Python files and SQL files make one graph:

```mermaid
flowchart LR
  classDef src fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  classDef st fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  classDef mv fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  classDef tv fill:#3f7d58,fill-opacity:0.72,stroke:#3f7d58,color:#ffffff
  csv["data/products.csv"]:::src -->|"products"| p["products<br/>materialized view"]:::mv
  p -->|"products_by_category"| pc["products_by_category<br/>materialized view"]:::mv
  json["data/orders/*.json"]:::src -->|"ingest_orders<br/>append flow"| o["orders_raw<br/>streaming table"]:::st
  o --> pl["placed_orders<br/>temporary view"]:::tv
  o -->|"orders_by_status"| os["orders_by_status<br/>materialized view"]:::mv
  pl -->|"placed_by_customer (SQL)"| pb["placed_by_customer<br/>materialized view"]:::mv
```

The boxes are datasets, and the labels on the arrows are flows. Nothing in the code gives this order. SDP finds it from the tables that each query reads.

## Step 3: How SDP builds the graph and runs it

A pipeline always runs on Spark Connect. The CLI is a Connect client, and the server builds and runs the graph. [Chapter 78](ch78-pipelines-cli-project.md) shows how the command sorts its arguments.

The CLI does these steps. Each step prints one line of the log:

```text
2026-11-22 02:17:25: Loading pipeline spec from ~/spark-masterclass/data/work/part06/ch32-retail/spark-pipeline.yml...
2026-11-22 02:17:25: Creating Spark session...
2026-11-22 02:17:26: Creating dataflow graph...
2026-11-22 02:17:28: Registering graph elements...
2026-11-22 02:17:28: Loading definitions. Root directory: '~/spark-masterclass/data/work/part06/ch32-retail'.
2026-11-22 02:17:28: Found 3 files matching glob 'transformations/**/*'
2026-11-22 02:17:28: Registering SQL file ~/spark-masterclass/data/work/part06/ch32-retail/transformations/placed_by_customer.sql...
2026-11-22 02:17:28: Importing ~/spark-masterclass/data/work/part06/ch32-retail/transformations/products.py...
query function products_by_category: called
2026-11-22 02:17:30: Importing ~/spark-masterclass/data/work/part06/ch32-retail/transformations/orders.py...
2026-11-22 02:17:30: Starting run...
2026-11-22 10:17:31: Flow spark_catalog.default.orders_by_status is QUEUED.
2026-11-22 10:17:31: Flow spark_catalog.default.ingest_orders is QUEUED.
2026-11-22 10:17:31: Flow spark_catalog.default.products is QUEUED.
2026-11-22 10:17:31: Flow spark_catalog.default.placed_by_customer is QUEUED.
2026-11-22 10:17:31: Flow spark_catalog.default.products_by_category is QUEUED.
2026-11-22 10:17:31: Flow spark_catalog.default.products is PLANNING.
2026-11-22 10:17:31: Flow spark_catalog.default.products is STARTING.
2026-11-22 10:17:31: Flow spark_catalog.default.products is RUNNING.
2026-11-22 10:17:31: Flow spark_catalog.default.ingest_orders is STARTING.
2026-11-22 10:17:32: Flow spark_catalog.default.ingest_orders is RUNNING.
2026-11-22 10:17:36: Flow spark_catalog.default.products has COMPLETED.
2026-11-22 10:17:36: Flow spark_catalog.default.ingest_orders has COMPLETED.
2026-11-22 10:17:37: Flow spark_catalog.default.products_by_category is PLANNING.
2026-11-22 10:17:37: Flow spark_catalog.default.products_by_category is STARTING.
2026-11-22 10:17:37: Flow spark_catalog.default.products_by_category is RUNNING.
2026-11-22 10:17:37: Flow spark_catalog.default.placed_by_customer is PLANNING.
2026-11-22 10:17:37: Flow spark_catalog.default.placed_by_customer is STARTING.
2026-11-22 10:17:37: Flow spark_catalog.default.placed_by_customer is RUNNING.
2026-11-22 10:17:37: Flow spark_catalog.default.orders_by_status is PLANNING.
2026-11-22 10:17:37: Flow spark_catalog.default.orders_by_status is STARTING.
2026-11-22 10:17:37: Flow spark_catalog.default.orders_by_status is RUNNING.
2026-11-22 10:17:41: Flow spark_catalog.default.orders_by_status has COMPLETED.
2026-11-22 10:17:41: Flow spark_catalog.default.products_by_category has COMPLETED.
2026-11-22 10:17:41: Flow spark_catalog.default.placed_by_customer has COMPLETED.
2026-11-22 10:17:42: Run is COMPLETED.
```

Read the log in three parts:

1. **Registration, in the client.** The CLI creates a session, and asks the server to create an empty dataflow graph. Then it loads each file. It sends a SQL file to the server as text. It imports a Python file as a module. Before the import, it puts the session into the module as the name `spark`. Thus, the file needs no `SparkSession.builder` line. The annotation `spark: SparkSession` is optional and only helps your editor.
2. **The query function runs once, at registration.** The line `query function products_by_category: called` comes between two imports. The decorator calls the function at once, takes the plan of the DataFrame that it returns, and sends the plan to the server. The function does not run again in the run. SDP does not promise this number of calls, so a query function must not have side effects.
3. **The run, on the server.** After `Starting run...`, each line is an event that the server sends back over the same connection. Each flow goes through `QUEUED`, `PLANNING`, `STARTING`, `RUNNING` and `COMPLETED`. The server starts a flow only when its inputs are complete. Thus, `products_by_category` waits for `products`, but `products` and `ingest_orders` run at the same time.

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch32-run.webp">
    <source src="../../media/clips/ch32-run.mp4" type="video/mp4">
  </video>
  <figcaption>A run of the retail pipeline with a Hive metastore. <code>products</code> and <code>ingest_orders</code> run at the same time. Each summary starts only when its input is complete.</figcaption>
</figure>

The CLI prints its own lines in the local time of the machine, and the events of the server in UTC (`handle_pipeline_events` in `spark_connect_pipeline.py`). This difference is why the clock moves by eight hours in the middle of the log.

```mermaid
sequenceDiagram
  participant C as spark-pipelines (client)
  participant S as Spark Connect server
  C->>C: read spark-pipeline.yml
  C->>S: CreateDataflowGraph (catalog, database, confs)
  S-->>C: graph ID
  loop each file that a glob matches
    C->>S: SQL file: DefineSqlGraphElements (the text)
    C->>C: Python file: import it, call each query function
    C->>S: DefineOutput / DefineFlow (name, plan)
  end
  C->>S: StartRun (graph ID, refresh options, dry, storage)
  S->>S: resolve the graph, validate, plan flows
  S-->>C: events: QUEUED, PLANNING, RUNNING, COMPLETED
  S-->>C: Run is COMPLETED
```

Chapter 36 follows these commands into the server source.

## Step 4: What pipeline code must not do

The pipeline owns the session. `session_rules.sh` adds one extra file at a time to a small project, and runs `dry-run` each time:

```text
== variants/conf_set.py
  | spark.conf.set("spark.sql.shuffle.partitions", "8")   # at module scope
pyspark.errors.exceptions.base.PySparkException: [SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.SET_RUNTIME_CONF] Session 
mutation 'RuntimeConf.set' is not allowed in declarative pipelines. Instead set configuration via the pipeline spec or 
use the 'spark_conf' argument in various decorators.

== variants/temp_view.py
  | spark.range(3).createOrReplaceTempView("ids")   # at module scope
pyspark.errors.exceptions.base.PySparkException: [SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.CREATE_OR_REPLACE_TEMP_VIEW] 
Session mutation 'DataFrame.createOrReplaceTempView' is not allowed in declarative pipelines. Instead use the 
@temporary_view decorator to define temporary views.

== variants/builder.py
  | from pyspark.sql import SparkSession
  | mine = SparkSession.builder.config("spark.sql.shuffle.partitions", "3").getOrCreate()
  | print("builder returned the pipeline session:", mine is spark,
  |       "| shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))
2026-11-22 02:42:48: Importing ~/spark-masterclass/data/work/part06/ch32-session/transformations/variant_builder.py...
builder returned the pipeline session: True | shuffle.partitions = 3
2026-11-22 10:42:50: Run is COMPLETED.

== variants/count.py
  | from pyspark import pipelines as dp
  | 
  | @dp.materialized_view
  | def product_count():
  |     n = spark.table("products").count()     # an action inside a query function
  |     return spark.range(1).selectExpr(f"{n} AS products")
pyspark.errors.exceptions.connect.AnalysisException: [ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION] Operations that 
trigger DataFrame analysis or execution are not allowed in pipeline query functions. Move code outside of the pipeline 
query function. SQLSTATE: 0A000

== variants/called.py
  | from pyspark import pipelines as dp
  | print("module scope: runs once, when the CLI imports the file")
  | 
  | @dp.materialized_view
  | def products_by_category():
  |     print("query function: called")
  |     return spark.table("products").groupBy("category").count()
module scope: runs once, when the CLI imports the file
query function: called
2026-11-22 10:43:07: Run is COMPLETED.
```

The results give four rules:

- **Do not change the session.** `spark.conf.set` fails with `SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.SET_RUNTIME_CONF`. `createOrReplaceTempView` fails in the same way. The CLI blocks these calls while it imports your files. Other blocked calls are `setCurrentCatalog`, `setCurrentDatabase`, `dropTempView` and the registration of UDFs. Put configurations in the `configuration` key of the spec ([Chapter 79](ch79-configuring-pipelines.md)).
- **Do not use `SparkSession.builder` in a pipeline file.** It gives no error, but `getOrCreate()` returns the session of the pipeline and applies your configuration to it. Here, `spark.sql.shuffle.partitions` changed to 3 for all the flows of the pipeline.
- **Do not call an action in a query function.** `count()` fails with `ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION`. The same rule applies to `collect()`, `toPandas()`, `save()`, `saveAsTable()`, `start()` and to each operation that analyzes the DataFrame.
- **Module code runs once, when the CLI imports the file.** The same is true for the query function, as Step 3 showed.

!!! warning "Caution: do not put side effects in pipeline files"

    Do not write files, send messages or call external services at module scope or in a query function. The CLI runs this code at registration, also for a `dry-run`, and other SDP implementations can call a query function more than one time.

## Step 5: Three types of dataset

The type of a dataset tells SDP how to update it in each run:

| Type | Python | SQL | Stored | What a run does |
|---|---|---|---|---|
| Materialized view | `@dp.materialized_view` | `CREATE MATERIALIZED VIEW` | yes | runs the query again on all the input, and replaces the data |
| Streaming table | `@dp.table`, or `dp.create_streaming_table` and flows | `CREATE STREAMING TABLE` | yes | reads only the input that is new since the last run, and appends it |
| Temporary view | `@dp.temporary_view` | `CREATE TEMPORARY VIEW` | no | nothing: other datasets of the same run read it as a view |

To select a type, ask how the input grows:

- If the input is a source that only gets new rows (files that arrive, a Kafka topic), use a streaming table. The flow uses `spark.readStream`, and a checkpoint records what it read.
- If the result can change for old rows (an aggregate, a join with a table that changes), use a materialized view. In open source Spark 4.3, a materialized view has no incremental refresh. Each run computes it again from all of its input.
- If the result is a step that only other datasets need, use a temporary view. It costs nothing to store, and the catalog does not show it.

`orders.py` declares a streaming table with an explicit flow, a temporary view and a materialized view:

```python
dp.create_streaming_table("orders_raw", comment="Every order event, as it arrived")


@dp.append_flow(target="orders_raw")
def ingest_orders() -> DataFrame:
    return spark.readStream.schema(ORDER_SCHEMA).json("data/orders")


@dp.temporary_view
def placed_orders() -> DataFrame:
    return spark.table("orders_raw").where(F.col("status") == "placed")


@dp.materialized_view
def orders_by_status() -> DataFrame:
    return spark.table("orders_raw").groupBy("status").agg(F.count("*").alias("order_count"))
```

`spark.table("orders_raw")` in a query function is a batch read of the full table. `spark.readStream.table("orders_raw")` is a streaming read of the rows that are new since the last run. The SQL forms are `FROM orders_raw` and `FROM STREAM orders_raw`. The SQL file of the project reads the temporary view of the Python file:

```sql
CREATE MATERIALIZED VIEW placed_by_customer
COMMENT 'Placed orders per customer'
AS SELECT customer_id, count(*) AS placed
FROM placed_orders
GROUP BY customer_id;
```

The decorators accept these options (`python/pyspark/pipelines/api.py`):

| Option | `materialized_view`, `table` | `create_streaming_table` | `temporary_view` | `append_flow` |
|---|---|---|---|---|
| `name` | yes | yes (required, first argument) | yes | yes |
| `comment` | yes | yes | yes | no |
| `spark_conf` | yes | no | yes | yes |
| `table_properties` | yes | yes | no | no |
| `partition_cols` or `cluster_by` | yes (not both) | yes (not both) | no | no |
| `schema` | yes | yes | no | no |
| `format` | yes | yes | no | no |
| `target` | no | no | no | yes (required) |

`name` is useful when one function makes many datasets in a loop ([Chapter 33](ch33-medallion.md)). `format` selects the table format in a catalog that accepts more than one format. `spark_conf` sets configurations for the flow of that dataset only. In the tests of this book, a `spark_conf` value did not reach the write of a materialized view ([Chapter 79](ch79-configuring-pipelines.md), Step 2).

## Step 6: Flows, and two flows into one table

A streaming table can get rows from more than one flow. Each flow has its own source and its own checkpoint. You can add a flow later, and the other flows continue from their checkpoints. The `flows` project has two Python flows into `orders_raw`, and a SQL file with two flows into a second streaming table:

```python
@dp.append_flow(target="orders_raw")
def ingest_orders() -> DataFrame:
    return spark.readStream.schema(ORDER_SCHEMA).json("data/orders")


@dp.append_flow(target="orders_raw")
def backfill_orders() -> DataFrame:
    return spark.readStream.schema(ORDER_SCHEMA).json("data/backfill")
```

```sql
-- A streaming table with no query of its own, and two SQL flows into it.
CREATE STREAMING TABLE order_events
COMMENT 'One row for each placed or shipped order';

CREATE FLOW placed_events AS INSERT INTO order_events BY NAME
SELECT order_id, 'placed' AS event, order_ts FROM STREAM orders_raw WHERE status = 'placed';

CREATE FLOW shipped_events AS INSERT INTO order_events BY NAME
SELECT order_id, 'shipped' AS event, order_ts FROM STREAM orders_raw WHERE status = 'shipped';
```

`BY NAME` is necessary in a SQL flow. The flow matches columns by name, not by position.

```mermaid
flowchart LR
  classDef src fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  classDef st fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  a["data/orders"]:::src -->|"ingest_orders<br/>checkpoint 1"| o["orders_raw"]:::st
  b["data/backfill"]:::src -->|"backfill_orders<br/>checkpoint 2"| o
  o -->|"placed_events"| e["order_events"]:::st
  o -->|"shipped_events"| e
```

The input has 11 orders in `data/orders` and 2 orders in `data/backfill`. Thus, `orders_raw` must have 13 rows, and `order_events` must have 12 rows (9 placed, 3 shipped). `flows.sh` runs the project in three ways:

```text
== 1. default catalog (Parquet tables), flows run at the same time
2026-11-22 10:06:19: Flow 'spark_catalog.default.backfill_orders' has FAILED.
  Error: [STREAM_FAILED] Query [id = a6e28ccb-42c7-48e3-b357-3f1263a9c2eb, runId = b267e0d0-677b-4689-9273-10d2376d44c7] terminated with exception: Multiple streaming queries are concurrently using file:~/spark-masterclass/data/work/part06...
2026-11-22 10:06:27: Run is COMPLETED.
orders_raw: 11 rows
order_events: 8 rows
  Parquet files on disk in orders_raw: 3

== 2. default catalog, spark.sql.pipelines.execution.maxConcurrentFlows = 1
2026-11-22 10:07:12: Run is COMPLETED.
orders_raw: 11 rows
order_events: 2 rows

== 3. Iceberg catalog, flows run at the same time
namespace lake.retail is ready
2026-11-22 10:08:05: Run is COMPLETED.
orders_raw: 13 rows
order_events: 12 rows
```

Only the Iceberg run is correct. The two runs in the default catalog lose rows:

- **Run 1.** Both flows wrote batch 0 at the same time. One flow failed, SDP tried it again, and the run ended as `COMPLETED`. But `orders_raw` has 11 rows, not 13. The three Parquet files on disk hold all 13 rows. The rows of one flow are in a file that the table does not list.
- **Run 2.** With one flow at a time, no flow failed. But the table has 11 rows again, and `order_events` has only 2 rows. This time, nothing in the log shows a problem.

The cause is the table of the default catalog. A streaming table there is a folder of Parquet files that the file sink of Structured Streaming writes.

The file sink keeps one log of batches, `_spark_metadata`, for the folder. Each flow counts its batches from 0. The second flow then writes its batch 0. The sink finds that batch 0 is already in the log, and skips the write (`FileStreamSink.addBatch`: `if (batchId <= fileLog.getLatestBatchId()...) logInfo("Skipping already committed batch ...")`). An Iceberg table has no shared batch log. Each flow commits a snapshot, so the two flows do not interfere.

!!! warning "Caution: do not write two flows into one streaming table of the default catalog"

    Use one flow for each streaming table in the default catalog, or keep the table in a format such as Iceberg. With two flows, the file sink skips a batch of one flow, and the run ends as `COMPLETED`. The part notes record this result as an open problem.

## Step 7: The server checks the whole graph before a flow runs

The server resolves and checks the full graph before it starts a flow. `spark-pipelines dry-run` stops after these checks ([Chapter 78](ch78-pipelines-cli-project.md)), so it shows them well. `broken/transformations/revenue.py` has three bugs: a cycle, a wrong table name and a wrong column. `dry_run_errors.sh` runs a dry run, fixes one bug, and runs again:

```text
== dry run 1
pyspark.errors.exceptions.connect.AnalysisException: The downstream table 'spark_catalog.default.top_customers' is 
referenced when creating the upstream table or view 'spark_catalog.default.revenue_by_customer'. Circular dependencies 
are not supported in a pipeline. Please remove the dependency between 'spark_catalog.default.revenue_by_customer' and 
'spark_catalog.default.top_customers'.

== fix 1: revenue_by_customer reads orders_enriched. dry run 2
2026-11-22 10:00:47: Failed to resolve flow due to upstream failure: 'spark_catalog.default.top_customers'.
Error: Failed to read dataset 'spark_catalog.default.revenue_by_customer'. Dataset is defined in the pipeline but could 
not be resolved.
2026-11-22 10:00:47: Failed to resolve flow due to upstream failure: 'spark_catalog.default.revenue_by_customer'.
Error: Failed to read dataset 'spark_catalog.default.orders_enriched'. Dataset is defined in the pipeline but could not 
be resolved.
2026-11-22 10:00:47: Failed to resolve flow due to upstream failure: 'spark_catalog.default.revenue_by_city'.
Error: Failed to read dataset 'spark_catalog.default.orders_enriched'. Dataset is defined in the pipeline but could not 
be resolved.
2026-11-22 10:00:47: Failed to resolve flow: 'spark_catalog.default.orders_enriched'.
Error: [TABLE_OR_VIEW_NOT_FOUND] The table or view `spark_catalog`.`default`.`orders_bronz` cannot be found. Verify the 
spelling and correctness of the schema and catalog.
Search path: [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`].
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct 
schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01;
'UnresolvedRelation [spark_catalog, default, orders_bronz], [], false
pyspark.errors.exceptions.connect.AnalysisException: 
Failed to resolve flows in the pipeline.
A flow can fail to resolve because the flow itself contains errors or because it reads
from an upstream flow which failed to resolve.
Flows with errors: spark_catalog.default.orders_enriched
Flows that failed due to upstream errors: spark_catalog.default.revenue_by_city, 
spark_catalog.default.revenue_by_customer, spark_catalog.default.top_customers
To view the exceptions that were raised while resolving these flows, look for flow
failures that precede this log.

== fix 2: orders_bronz -> orders_bronze. dry run 3
2026-11-22 10:01:20: Failed to resolve flow: 'spark_catalog.default.revenue_by_city'.
Error: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `region` cannot be 
resolved. Did you mean one of the following? [`email`, `city`, `name`, `order_id`, `order_ts`]. SQLSTATE: 42703;
'Aggregate ['region], ['region, sum(line_total#65) AS revenue#22]
+- SubqueryAlias spark_catalog.default.orders_enriched
   +- LocalRelation <empty>, [customer_id#54, order_id#55, product_id#56, quantity#57, unit_price#58, status#59, 
order_ts#60, name#61, email#62, city#63, signup_date#64, line_total#65]
pyspark.errors.exceptions.connect.AnalysisException: 
Failed to resolve flows in the pipeline.
A flow can fail to resolve because the flow itself contains errors or because it reads
from an upstream flow which failed to resolve.
Flows with errors: spark_catalog.default.revenue_by_city
Flows that failed due to upstream errors: 
To view the exceptions that were raised while resolving these flows, look for flow
failures that precede this log.

== fix 3: region -> city. dry run 4
2026-11-22 10:02:02: Run is COMPLETED.

== what the dry run wrote
files under spark-warehouse/ and pipeline-storage/: 0
```

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch32-dryrun.webp">
    <source src="../../media/clips/ch32-dryrun.mp4" type="video/mp4">
  </video>
  <figcaption>A dry run of the broken pipeline reports the cycle. After the fix, the next dry run reports the wrong table name. The dry runs write nothing.</figcaption>
</figure>

The errors come in a fixed order, because the server does its checks in a fixed order (`DataflowGraph.validate` and `GraphValidations.validateSuccessfulFlowAnalysis`):

1. **The cycle.** `revenue_by_customer` reads `top_customers`, and `top_customers` reads `revenue_by_customer`. Neither flow can resolve, because each needs the schema of the other. Before the server reports the flows that failed, it looks for a cycle among them. It finds one, and reports only the cycle.
2. **The names.** `orders_bronz` does not exist. Read the log from the bottom: the summary lists one flow with errors and three flows that failed only because of it. The only line that says `Failed to resolve flow:` without `upstream failure` names the real problem, and the `TABLE_OR_VIEW_NOT_FOUND` error follows it.
3. **The columns.** `revenue_by_city` groups by `region`, which `orders_enriched` does not have. The analyzer suggests `city`. The plan in the error shows a `LocalRelation <empty>`: the dry run analyzes each query against the schema of its inputs, with no data.

After the last fix, the dry run completes, and it wrote no file. A dry run does not read data. Thus, it cannot find a value that fails at run time, for example `unit_price` with the value `N/A` in a cast.

Use a dry run in two places. Run it after each change while you write the pipeline. Also run it in continuous integration, before a change goes to production. It needs a Spark Connect server and the catalogs that the spec names, but no data.

## What a run leaves behind

A run writes each table to the catalog and a checkpoint for each streaming flow to the `storage` folder of the spec. The next run reads only new input for each streaming table, and computes each materialized view again. The chapters of the tools show these results:

- [Chapter 77](ch77-first-etl-pipeline.md), Steps 8 and 9: the tables, the files on disk and the checkpoint after two runs.
- [Chapter 78](ch78-pipelines-cli-project.md), Steps 8 and 9: `--refresh`, `--full-refresh`, the new checkpoint folder of a full refresh, and where each output goes.
- [Chapter 79](ch79-configuring-pipelines.md): a catalog that keeps the tables between runs, such as a Hive metastore or Iceberg.

## The whole chapter, on one page

- **Declare, do not order.** A query function returns a DataFrame. SDP finds the order from the tables that each query reads.
- **Python and SQL files make one graph.** A name from one language is a name in the other.
- **The CLI is a Connect client.** It registers the graph, and the server runs it and sends events back.
- **The pipeline owns the session.** Do not change configurations, create temporary views or call actions in pipeline code.
- **Select the dataset type by how the input grows.** Streaming tables append new input. Materialized views compute everything again. Temporary views are not stored.
- **One streaming table in the default catalog takes one flow.** Two flows lose rows there. Iceberg tables take many flows.
- **The server checks the graph before a flow runs.** It finds cycles, wrong names and wrong columns, in that order, and reads no data.

[Chapter 33](ch33-medallion.md) builds a larger pipeline in layers, and shows what a second run reads when new files arrive.

## Further reading

- [Spark Declarative Pipelines guide](https://spark.apache.org/docs/4.3.0/declarative-pipelines-programming-guide.html), the official guide for Spark 4.3.
- `python/pyspark/pipelines/api.py` and `cli.py` in the Spark source (tag `v4.3.0`), the decorator options and the registration of this chapter.
