# Roadmap

The book has fourteen parts. Each part lists its chapters and the subjects that they cover. The map from SparkTutor lessons to chapters is in [coverage](coverage.md).

## Part I: Background

How Spark came to be, how it runs, and how it became a batch and streaming platform.

| # | Chapter | Covers |
|---|---|---|
| 1 | [From MapReduce to Spark](chapters/ch01-mapreduce-to-spark.md) | MapReduce and its disk cost, the RDD papers, lineage; one iterative job MapReduce-style against an RDD cache, measured |
| 2 | [Inside a Spark application](chapters/ch02-inside-a-spark-application.md) | Driver and executors as separate JVMs, cluster managers, DAGScheduler and TaskScheduler, the BlockManager, shuffle files on disk, unified memory, dynamic allocation |
| 3 | [From RDDs to a SQL engine](chapters/ch03-rdds-to-a-sql-engine.md) | The same query as RDDs and as a DataFrame, Catalyst, Tungsten rows, whole-stage codegen, AQE, Arrow, columnar engines, Feather |
| 4 | [One engine for batch and streaming, and Spark's modules](chapters/ch04-one-engine-batch-and-streaming.md) | Releases from 0.7 to 4.3, DStreams to Structured Streaming to Real-time Mode, one function as batch and stream, the module map, GraphX and GraphFrames |

## Part II: Getting started

Your first application, the install, a quick start and configurations.

| # | Chapter | Covers |
|---|---|---|
| 5 | [How Spark runs your code](chapters/ch05-how-spark-runs.md) | Jobs, stages, tasks, Python workers, Classic vs Connect. **4.3:** Feather single-task local execution |
| 6 | [Installing Spark 4.3](chapters/ch06-installing.md) | pip, the tarball, Docker and Spark Connect, each run for real; how to pick; the book's data |
| 7 | [Quick start](chapters/ch07-quick-start.md) | Read the taxi data, aggregate, the same query in SQL, the plan they share |
| 8 | [Setting Spark configurations](chapters/ch08-setting-configurations.md) | Every source of a config and which one wins, static and runtime, SQL and core, how to find configs, Classic and Connect, a reference table |

## Part III: DataFrames

Columns and schemas, cleaning values, joins.

| # | Chapter | Covers |
|---|---|---|
| 9 | [DataFrames, columns and schemas](chapters/ch09-dataframes-columns-schemas.md) | Analyzed plans for `withColumn` chains, nullability. **4.3:** `DataFrame.zip` |
| 10 | [Cleaning values: ANSI, try_ functions, strings and time](chapters/ch10-cleaning-values.md) | Collations, money, dates. **4.3:** nanosecond timestamps |
| 11 | [Joins: strategies, skew and the time-aware joins](chapters/ch11-joins.md) | Broadcast vs sort-merge on real sizes, NEAREST BY. **4.3:** ASOF JOIN |

## Part IV: Spark SQL

The language and its tools, windows and pipe syntax, procedural SQL, time series, geospatial, metric views.

| # | Chapter | Covers |
|---|---|---|
| 12 | [Spark SQL: the language, the catalog and the tools](chapters/ch12-spark-sql.md) | The spark-sql CLI, the Thrift server, the Connect JDBC driver, catalogs and three-part names, tables and views, types, the function library, ANSI mode |
| 13 | [Spark SQL in depth: windows, pipe syntax and QUALIFY](chapters/ch13-spark-sql-in-depth.md) | Window functions get taught properly (LS04 used them untaught) |
| 14 | [Procedural SQL: scripting, SQL UDFs and recursive CTEs](chapters/ch14-procedural-sql.md) | Variables, loops, handlers, `EXECUTE IMMEDIATE` and the SQL path |
| 15 | [Time series](chapters/ch15-time-series.md) | `time_bucket`, windows and sessions, time zones, gaps, percentiles per bucket |
| 16 | [Geospatial data in Spark 4.3](chapters/ch16-geospatial.md) | GEOMETRY and GEOGRAPHY, the five ST functions of 4.3.0, WKB, SRIDs, point in polygon built in SQL on real zones, Parquet geo types, Iceberg v3 |
| 17 | [Metric views: a semantic layer in Spark SQL](chapters/ch17-metric-views.md) | A semantic layer in Spark SQL: dimensions, measures and `measure()` |

## Part V: Spark Connect

Why Connect, the protocol, observability, clients in other languages, building and writing clients, production.

| # | Chapter | Covers |
|---|---|---|
| 18 | [Why Spark Connect should be your default](chapters/ch18-spark-connect.md) | The thin client and the unresolved plan, what Connect fixes and costs, Classic against Connect measured, how to make Connect your default. **4.3:** operation IDs and REST |
| 19 | [Inside the Spark Connect protocol](chapters/ch19-connect-protocol.md) | Relations, expressions and commands as protobuf, the gRPC services, captured traffic, Arrow batches in the response stream, reattach |
| 20 | [Session observability: what does this client run, and how do I stop it?](chapters/ch20-connect-observability.md) | Session, user and operation IDs, tags and interrupts, the Connect tab, REST endpoints, listener events, stopping a client from another process |
| 21 | [Spark Connect clients beyond Python](chapters/ch21-connect-clients.md) | The same query from Python, Scala, Go, Rust and Swift, what each client supports, reattach, start-up time |
| 22 | [Building an application on a Connect client](chapters/ch22-connect-application.md) | A Rust application that keeps small work local in Polars and DuckDB and sends heavy work to Spark, Arrow without conversion, the crossover measured |
| 23 | [Writing a Spark Connect client](chapters/ch23-writing-a-connect-client.md) | A client in about 200 lines of Python without pyspark: compiled protos, ExecutePlan, Arrow decoding, AnalyzePlan, a DataFrame builder, errors and reattach |
| 24 | [Spark Connect in production: deployment patterns](chapters/ch24-connect-in-production.md) | One server per app or shared, sessions and isolation, auth and proxies, reattach, a sandbox per agent. **4.3:** operation IDs, REST endpoints, Feather on the server |

## Part VI: Files, formats and messy data

Files, logs, CSV, JSON, complex types, VARIANT, XML, Avro, Protobuf and blobs.

| # | Chapter | Covers |
|---|---|---|
| 25 | [Files as files, globs and metadata](chapters/ch25-files-as-files.md) | **4.3:** reading zip and tar archives |
| 26 | [Text, logs and CSV](chapters/ch26-text-logs-csv.md) | Corrupt-record modes, layout drift, encodings |
| 27 | [JSON: inference, drift and quarantine](chapters/ch27-json.md) | **4.3:** JSON_TABLE |
| 28 | [Structs, arrays, maps and higher-order functions](chapters/ch28-complex-types.md) | How lambdas are traced into Catalyst |
| 29 | [VARIANT: landing, profiling and editing](chapters/ch29-variant.md) | Shredding and extraction speed. **4.3:** VARIANT edits |
| 30 | [XML, Avro and Protobuf](chapters/ch30-xml-avro-protobuf.md) | The Kafka wire format |
| 31 | [Documents, images, audio and tables for blobs](chapters/ch31-documents-images-audio.md) | Archives as the opener |

## Part VII: Performance

Plans, partitions and memory, Python and Arrow, Parquet, Project Feather, acceleration.

| # | Chapter | Covers |
|---|---|---|
| 32 | [Reading query plans: Catalyst and AQE](chapters/ch32-query-plans.md) | One AQE explanation instead of three; real `explain` output |
| 33 | [Partitions, caching, memory and spill](chapters/ch33-partitions-caching-memory.md) | Recorded Spark UI walkthrough. |
| 34 | [Python on Spark: UDFs, pandas UDFs and Arrow](chapters/ch34-python-udfs-arrow.md) | **4.3:** UDF transpilation; the tuning tables get re-measured |
| 35 | [Python UDTFs](chapters/ch35-python-udtfs.md) | JSON_TABLE as the alternative to parsing UDTFs |
| 36 | [Parquet from the inside](chapters/ch36-parquet-inside.md) | Footer hexdumps, row groups, statistics, pushdown |
| 37 | [Project Feather: small queries without the distributed tax](chapters/ch37-project-feather.md) | **4.3:** when single-task execution applies, the crossover measured across data sizes, what it does to plans, AQE, the UI and Connect |
| 38 | [Acceleration: Gluten and Velox, RAPIDS and cuDF, Comet](chapters/ch38-acceleration.md) | Native and GPU execution engines for Spark: how each one plugs in (plugins, columnar rules, fallbacks), which support Spark 4.3, and the same taxi queries measured on CPU, Velox and the GPU |

## Part VIII: Streaming

Structured Streaming, messy streams, transformWithState, Real-time Mode, checkpoints.

| # | Chapter | Covers |
|---|---|---|
| 39 | [Structured Streaming fundamentals](chapters/ch39-structured-streaming.md) | Sources and sinks, output modes, triggers, windows, watermarks and checkpoints |
| 40 | [Streaming messy data: drift, dead letters, late events](chapters/ch40-streaming-messy-data.md) | The future-dated event that wrecks a watermark |
| 41 | [Stateful processing with transformWithState](chapters/ch41-transform-with-state.md) | Timers, TTL, state inspection |
| 42 | [Real-time Mode](chapters/ch42-real-time-mode.md) | **4.3:** stateful RTM and the pipelined shuffle, latency measured both ways |
| 43 | [Inside a checkpoint and the state store](chapters/ch43-checkpoints-and-state-store.md) | Delta files, RocksDB, the state data source |

## Part IX: Lakehouse tables

Delta and Iceberg, the Iceberg runtime and catalogs.

| # | Chapter | Covers |
|---|---|---|
| 44 | [Tables: Delta and Iceberg, MERGE and time travel](chapters/ch44-tables-delta-iceberg.md) | The transaction log on disk. |
| 45 | [Apache Iceberg with Spark: the runtime and catalog management](chapters/ch45-iceberg.md) | The `iceberg-spark-runtime` jar, Spark catalogs (`SparkCatalog`, `SparkSessionCatalog`), REST, JDBC and Hadoop catalogs, namespaces, Iceberg SQL extensions and procedures, table maintenance |

## Part X: Declarative Pipelines

Your first ETL pipeline, the CLI and project, configuration, the model, medallion, CDC, how SDP runs.

| # | Chapter | Covers |
|---|---|---|
| 46 | [Your first ETL pipeline](chapters/ch46-first-etl-pipeline.md) | From an empty folder to a working pipeline: init, a streaming table, materialized views, a run, a second day of data, a broken flow |
| 47 | [The spark-pipelines CLI and the project](chapters/ch47-pipelines-cli-project.md) | Every subcommand and flag, the spec file, libraries globs, dry-run, refresh selection, full refresh, where tables and checkpoints go, remote servers |
| 48 | [Configuring a pipeline](chapters/ch48-configuring-pipelines.md) | The configuration block, parameters in SQL, catalog and database, dev and prod specs, secrets, an Iceberg catalog, which layer owns which setting |
| 49 | [Declarative Pipelines: the model](chapters/ch49-declarative-pipelines.md) | Datasets, flows and the graph, what pipeline code must not do, dry runs |
| 50 | [Medallion in practice: bronze, silver, gold and quality](chapters/ch50-medallion.md) | Incrementality shown across two runs; the event log |
| 51 | [Change data capture: reading changes and Auto CDC](chapters/ch51-change-data-capture.md) | **4.3:** SCD Type 2 on the Toronto, Ottawa, Hanoi changes |
| 52 | [A pipeline end to end, and how SDP runs it](chapters/ch52-pipeline-end-to-end.md) | Under the hood rewritten from the real SDP source, not the regex toy |

## Part XI: Machine learning and applied algorithms

MLlib and MLflow, text, similarity search, entity resolution, sketches, calling models.

| # | Chapter | Covers |
|---|---|---|
| 53 | [ML pipelines with MLlib, tracked in MLflow](chapters/ch53-mllib-mlflow.md) | ML over Connect; open source MLflow for tracking |
| 54 | [Text analytics without an LLM](chapters/ch54-text-analytics.md) | TF-IDF search and near-duplicates |
| 55 | [Similarity search: vectors, LSH and IVF](chapters/ch55-similarity-search.md) | Recall against work; NEAREST BY top-K |
| 56 | [Entity resolution and connected components](chapters/ch56-entity-resolution.md) | Plan growth in iterative jobs, `localCheckpoint` |
| 57 | [Sketches: HLL, Theta and KLL](chapters/ch57-sketches.md) | Mergeable summaries for profiling |
| 58 | [Calling models and services from Spark](chapters/ch58-calling-models.md) | Retries, batching, request counts |

## Part XII: Running Spark in production

Determinism, dependencies, observability, Kubernetes, orchestration, agents.

| # | Chapter | Covers |
|---|---|---|
| 59 | [Determinism, retries and fault injection](chapters/ch59-determinism-retries.md) | Connect operation IDs for reattach |
| 60 | [Shipping Python dependencies](chapters/ch60-shipping-dependencies.md) | Archives, environments and what actually reaches the workers |
| 61 | [Observability: listeners, event logs, metrics and profilers](chapters/ch61-observability.md) | **4.3:** Connect REST endpoints |
| 62 | [Spark on Kubernetes with the Spark Kubernetes Operator](chapters/ch62-kubernetes-operator.md) | SparkApplication and SparkCluster resources, a Connect server on Kubernetes, run for real on a local cluster |
| 63 | [Orchestrating Spark: Airflow, operators, YuniKorn and Celeborn](chapters/ch63-orchestration.md) | Airflow's Spark operators (submit, Kubernetes, Connect), the Kubernetes operator under Airflow, gang scheduling and queues with Apache YuniKorn, remote shuffle with Apache Celeborn |
| 64 | [Writing Spark pipelines with agents](chapters/ch64-pipelines-with-agents.md) | What agents get wrong in Spark (from the OSA Con 2026 study), and a harness of declare, scope and context: dry-run, a Connect sandbox, version-pinned skills, metric views |

## Part XIII: Extending Spark

Python data sources, DSv2 in Java, Catalyst extensions, Connect plugins, a capstone.

| # | Chapter | Covers |
|---|---|---|
| 65 | [Python data sources I: readers and partition planning](chapters/ch65-python-data-sources-readers.md) | A parallel reader for a custom format |
| 66 | [Python data sources II: APIs, databases and Arrow batches](chapters/ch66-python-data-sources-apis-databases.md) | JDBC ranges vs a Python source |
| 67 | [Python data sources III: filter pushdown](chapters/ch67-python-data-sources-pushdown.md) | Which filters Spark offers a source, and what goes wrong when a source accepts them |
| 68 | [Python data sources IV: writers and streaming](chapters/ch68-python-data-sources-writers-streaming.md) | Commit protocols, offsets, idempotent sinks |
| 69 | [Shipping a source: packaging, testing and debugging](chapters/ch69-shipping-a-python-data-source.md) | Registration, tests, logging inside workers |
| 70 | [A DSv2 connector in Java](chapters/ch70-dsv2-connector-java.md) | Capability table that bridges to the Python API |
| 71 | [Catalyst extensions: rules and native functions](chapters/ch71-catalyst-extensions.md) | **4.3:** the hand-written rule vs built-in UDF transpilation |
| 72 | [Spark Connect plugins](chapters/ch72-connect-plugins.md) | Relation, command and expression plugins |
| 73 | [Capstone: your own connector](chapters/ch73-capstone-connector.md) | Format, batch and pushdown, writer, streaming |

## Part XIV: Internals

Catalyst and Python, the worker protocol, VARIANT bytes, an upstream case study.

| # | Chapter | Covers |
|---|---|---|
| 74 | [How Catalyst plans Python](chapters/ch74-catalyst-plans-python.md) | **4.3:** transpilation as the closing section |
| 75 | [The Python worker protocol](chapters/ch75-python-worker-protocol.md) | Wire format, failure modes, memory limits |
| 76 | [The VARIANT binary format](chapters/ch76-variant-binary-format.md) | The bytes, the metadata dictionary and shredding |
| 77 | [From wrong results to a fix: an upstream case study](chapters/ch77-upstream-case-study.md) | Reproducing, minimizing and reporting a Spark bug |
