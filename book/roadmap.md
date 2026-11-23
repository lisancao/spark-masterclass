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

## Part III: DataFrames and the other APIs

Columns and schemas, cleaning values, joins, RDDs, the pandas API on Spark, typed Datasets.

| # | Chapter | Covers |
|---|---|---|
| 9 | [DataFrames, columns and schemas](chapters/ch09-dataframes-columns-schemas.md) | Analyzed plans for `withColumn` chains, nullability. **4.3:** `DataFrame.zip` |
| 10 | [Cleaning values: ANSI, try_ functions, strings and time](chapters/ch10-cleaning-values.md) | Collations, money, dates. **4.3:** nanosecond timestamps |
| 11 | [Joins: strategies, skew and the time-aware joins](chapters/ch11-joins.md) | Broadcast vs sort-merge on real sizes, NEAREST BY. **4.3:** ASOF JOIN |
| 12 | [RDDs today](chapters/ch12-rdds-today.md) | Transformations and pair operations measured, partitioners, closures, broadcast variables and accumulators, storage levels, checkpoints, what fails over Connect |
| 13 | [pandas API on Spark](chapters/ch13-pandas-api-on-spark.md) | The internal frame, default index types and their cost, pandas against pandas-on-Spark against PySpark, options, conversions, limits |
| 14 | [Datasets and Encoders in Scala](chapters/ch14-datasets-encoders-scala.md) | Encoders, typed operations and their plan nodes, Aggregator, typed code over the Scala Connect client |

## Part IV: Spark SQL

The language and its tools, tables and DDL, Hive, windows and pipe syntax, aggregation, procedural SQL, time series, geospatial, metric views.

| # | Chapter | Covers |
|---|---|---|
| 15 | [Spark SQL: the language, the catalog and the tools](chapters/ch15-spark-sql.md) | The spark-sql CLI, the Thrift server, the Connect JDBC driver, catalogs and three-part names, tables and views, types, the function library, ANSI mode |
| 16 | [Tables, DDL and writes](chapters/ch16-tables-ddl-writes.md) | CREATE TABLE forms on disk, DEFAULT values, constraints, ALTER and TRUNCATE, COMMENT ON, view schema modes, save modes, writeTo, partition overwrite, every INSERT form |
| 17 | [Hive tables and the metastore](chapters/ch17-hive-tables-metastore.md) | Hive support, metastore versions, SerDe and data source tables, MSCK REPAIR, LOAD DATA, Hive UDFs, script TRANSFORM, a standalone metastore |
| 18 | [Spark SQL in depth: windows, pipe syntax and QUALIFY](chapters/ch18-spark-sql-in-depth.md) | Window functions get taught properly (LS04 used them untaught) |
| 19 | [Aggregation: GROUP BY, grouping sets, ROLLUP, CUBE and pivots](chapters/ch19-aggregation.md) | The aggregate operators and when each runs, grouping sets, ROLLUP, CUBE, FILTER and DISTINCT aggregates, PIVOT and UNPIVOT. **4.3:** aggregate plan changes |
| 20 | [Procedural SQL: scripting, SQL UDFs and recursive CTEs](chapters/ch20-procedural-sql.md) | Variables, loops, handlers, `EXECUTE IMMEDIATE` and the SQL path |
| 21 | [Time series](chapters/ch21-time-series.md) | `time_bucket`, windows and sessions, time zones, gaps, percentiles per bucket |
| 22 | [Geospatial data in Spark 4.3](chapters/ch22-geospatial.md) | GEOMETRY and GEOGRAPHY, the five ST functions of 4.3.0, WKB, SRIDs, point in polygon built in SQL on real zones, Parquet geo types, Iceberg v3 |
| 23 | [Metric views: a semantic layer in Spark SQL](chapters/ch23-metric-views.md) | A semantic layer in Spark SQL: dimensions, measures and `measure()` |

## Part V: Spark Connect

Why Connect, the protocol, observability, clients in other languages, building and writing clients, production.

| # | Chapter | Covers |
|---|---|---|
| 24 | [Why Spark Connect should be your default](chapters/ch24-spark-connect.md) | The thin client and the unresolved plan, what Connect fixes and costs, Classic against Connect measured, how to make Connect your default. **4.3:** operation IDs and REST |
| 25 | [Inside the Spark Connect protocol](chapters/ch25-connect-protocol.md) | Relations, expressions and commands as protobuf, the gRPC services, captured traffic, Arrow batches in the response stream, reattach |
| 26 | [Session observability: what does this client run, and how do I stop it?](chapters/ch26-connect-observability.md) | Session, user and operation IDs, tags and interrupts, the Connect tab, REST endpoints, listener events, stopping a client from another process |
| 27 | [Spark Connect clients beyond Python](chapters/ch27-connect-clients.md) | The same query from Python, Scala, Go, Rust and Swift, what each client supports, reattach, start-up time |
| 28 | [Building an application on a Connect client](chapters/ch28-connect-application.md) | A Rust application that keeps small work local in Polars and DuckDB and sends heavy work to Spark, Arrow without conversion, the crossover measured |
| 29 | [Writing a Spark Connect client](chapters/ch29-writing-a-connect-client.md) | A client in about 200 lines of Python without pyspark: compiled protos, ExecutePlan, Arrow decoding, AnalyzePlan, a DataFrame builder, errors and reattach |
| 30 | [Spark Connect in production: deployment patterns](chapters/ch30-connect-in-production.md) | One server per app or shared, sessions and isolation, auth and proxies, reattach, a sandbox per agent. **4.3:** operation IDs, REST endpoints, Feather on the server |

## Part VI: Files, formats and data sources

Files, logs, CSV, JSON, complex types, VARIANT, XML, Avro, Protobuf, blobs, object storage, JDBC.

| # | Chapter | Covers |
|---|---|---|
| 31 | [Files as files, globs and metadata](chapters/ch31-files-as-files.md) | **4.3:** reading zip and tar archives |
| 32 | [Text, logs and CSV](chapters/ch32-text-logs-csv.md) | Corrupt-record modes, layout drift, encodings |
| 33 | [JSON: inference, drift and quarantine](chapters/ch33-json.md) | **4.3:** JSON_TABLE |
| 34 | [Structs, arrays, maps and higher-order functions](chapters/ch34-complex-types.md) | How lambdas are traced into Catalyst |
| 35 | [VARIANT: landing, profiling and editing](chapters/ch35-variant.md) | Shredding and extraction speed. **4.3:** VARIANT edits |
| 36 | [XML, Avro and Protobuf](chapters/ch36-xml-avro-protobuf.md) | The Kafka wire format |
| 37 | [Documents, images, audio and tables for blobs](chapters/ch37-documents-images-audio.md) | Archives as the opener |
| 38 | [Spark on object storage: S3A, ABFS, GCS and the output committers](chapters/ch38-object-storage.md) | S3A on MinIO, credentials, the FileOutputCommitter algorithms, magic and staging committers, the manifest committer, a failed job on each |
| 39 | [Databases over JDBC: partitioned reads, pushdown and writes](chapters/ch39-jdbc.md) | Partitioned reads and skew, fetchsize, DSv1 and catalog pushdown, read and write options, transactions, upserts, batchsize, JDBC catalogs |

## Part VII: Performance

Plans, statistics and the optimizer, partitions and memory, Python and Arrow, Parquet and ORC, Project Feather, acceleration.

| # | Chapter | Covers |
|---|---|---|
| 40 | [Reading query plans: Catalyst and AQE](chapters/ch40-query-plans.md) | One AQE explanation instead of three; real `explain` output |
| 41 | [Statistics, the cost-based optimizer and partition pruning](chapters/ch41-statistics-cbo-pruning.md) | ANALYZE TABLE, histograms, CBO and join reorder, dynamic partition pruning, bucketing, storage-partitioned joins, every hint |
| 42 | [Partitions, caching, memory and spill](chapters/ch42-partitions-caching-memory.md) | Recorded Spark UI walkthrough. |
| 43 | [Python on Spark: UDFs, pandas UDFs and Arrow](chapters/ch43-python-udfs-arrow.md) | **4.3:** UDF transpilation; the tuning tables get re-measured |
| 44 | [Python UDTFs](chapters/ch44-python-udtfs.md) | JSON_TABLE as the alternative to parsing UDTFs |
| 45 | [Parquet and ORC from the inside](chapters/ch45-parquet-inside.md) | Footer hexdumps, row groups, statistics, pushdown |
| 46 | [Project Feather: small queries without the distributed tax](chapters/ch46-project-feather.md) | **4.3:** when single-task execution applies, the crossover measured across data sizes, what it does to plans, AQE, the UI and Connect |
| 47 | [Acceleration: Gluten and Velox, RAPIDS and cuDF, Comet](chapters/ch47-acceleration.md) | Native and GPU execution engines for Spark: how each one plugs in (plugins, columnar rules, fallbacks), which support Spark 4.3, and the same taxi queries measured on CPU, Velox and the GPU |

## Part VIII: Streaming

Structured Streaming, messy streams, transformWithState, Real-time Mode, checkpoints, Kafka.

| # | Chapter | Covers |
|---|---|---|
| 48 | [Structured Streaming fundamentals](chapters/ch48-structured-streaming.md) | Sources and sinks, output modes, triggers, windows, watermarks and checkpoints |
| 49 | [Streaming messy data: drift, dead letters, late events](chapters/ch49-streaming-messy-data.md) | The future-dated event that wrecks a watermark |
| 50 | [Stateful processing with transformWithState](chapters/ch50-transform-with-state.md) | Timers, TTL, state inspection |
| 51 | [Real-time Mode](chapters/ch51-real-time-mode.md) | **4.3:** stateful RTM and the pipelined shuffle, latency measured both ways |
| 52 | [Inside a checkpoint and the state store](chapters/ch52-checkpoints-and-state-store.md) | Delta files, RocksDB, the state data source |
| 53 | [Kafka with Spark in depth](chapters/ch53-kafka-in-depth.md) | Starting offsets, batch reads, batch sizes, headers, failOnDataLoss, the at-least-once sink, consumer groups, SASL |

## Part IX: Lakehouse tables

Delta and Iceberg, the Iceberg runtime and catalogs.

| # | Chapter | Covers |
|---|---|---|
| 54 | [Tables: Delta and Iceberg, MERGE and time travel](chapters/ch54-tables-delta-iceberg.md) | The transaction log on disk. |
| 55 | [Apache Iceberg with Spark: the runtime and catalog management](chapters/ch55-iceberg.md) | The `iceberg-spark-runtime` jar, Spark catalogs (`SparkCatalog`, `SparkSessionCatalog`), REST, JDBC and Hadoop catalogs, namespaces, Iceberg SQL extensions and procedures, table maintenance |

## Part X: Declarative Pipelines

Your first ETL pipeline, the CLI and project, configuration, the model, medallion, CDC, how SDP runs.

| # | Chapter | Covers |
|---|---|---|
| 56 | [Your first ETL pipeline](chapters/ch56-first-etl-pipeline.md) | From an empty folder to a working pipeline: init, a streaming table, materialized views, a run, a second day of data, a broken flow |
| 57 | [The spark-pipelines CLI and the project](chapters/ch57-pipelines-cli-project.md) | Every subcommand and flag, the spec file, libraries globs, dry-run, refresh selection, full refresh, where tables and checkpoints go, remote servers |
| 58 | [Configuring a pipeline](chapters/ch58-configuring-pipelines.md) | The configuration block, parameters in SQL, catalog and database, dev and prod specs, secrets, an Iceberg catalog, which layer owns which setting |
| 59 | [Declarative Pipelines: the model](chapters/ch59-declarative-pipelines.md) | Datasets, flows and the graph, what pipeline code must not do, dry runs |
| 60 | [Medallion in practice: bronze, silver, gold and quality](chapters/ch60-medallion.md) | Incrementality shown across two runs; the event log |
| 61 | [Change data capture: reading changes and Auto CDC](chapters/ch61-change-data-capture.md) | **4.3:** SCD Type 2 on the Toronto, Ottawa, Hanoi changes |
| 62 | [A pipeline end to end, and how SDP runs it](chapters/ch62-pipeline-end-to-end.md) | Under the hood rewritten from the real SDP source, not the regex toy |

## Part XI: Machine learning and applied algorithms

MLlib and MLflow, clustering, recommendation and patterns, text, similarity search, entity resolution, sketches, calling models.

| # | Chapter | Covers |
|---|---|---|
| 63 | [ML pipelines with MLlib, tracked in MLflow](chapters/ch63-mllib-mlflow.md) | ML over Connect; open source MLflow for tracking |
| 64 | [MLlib beyond regression](chapters/ch64-mllib-beyond-regression.md) | Clustering, ALS, FPGrowth and PrefixSpan, statistics, more transformers, model selection, MLlib over Connect |
| 65 | [Text analytics without an LLM](chapters/ch65-text-analytics.md) | TF-IDF search and near-duplicates |
| 66 | [Similarity search: vectors, LSH and IVF](chapters/ch66-similarity-search.md) | Recall against work; NEAREST BY top-K |
| 67 | [Entity resolution and connected components](chapters/ch67-entity-resolution.md) | Plan growth in iterative jobs, `localCheckpoint` |
| 68 | [Sketches: HLL, Theta and KLL](chapters/ch68-sketches.md) | Mergeable summaries for profiling |
| 69 | [Calling models and services from Spark](chapters/ch69-calling-models.md) | Retries, batching, request counts |

## Part XII: Running Spark in production

Submitting applications, security, determinism, dependencies, observability, testing, Kubernetes, orchestration, agents.

| # | Chapter | Covers |
|---|---|---|
| 70 | [Submitting applications: spark-submit, standalone, YARN and Kubernetes](chapters/ch70-submitting-applications.md) | Master URLs, deploy modes, spark-submit, a standalone cluster with HA, YARN, native Kubernetes without the operator |
| 71 | [Securing Spark](chapters/ch71-securing-spark.md) | Authentication and AES-GCM, TLS for UI and RPC, UI ACLs, local disk encryption, redaction, Kerberos |
| 72 | [Determinism, retries and fault injection](chapters/ch72-determinism-retries.md) | Connect operation IDs for reattach |
| 73 | [Shipping Python dependencies](chapters/ch73-shipping-dependencies.md) | Archives, environments and what actually reaches the workers |
| 74 | [Observability: listeners, event logs, metrics and profilers](chapters/ch74-observability.md) | **4.3:** Connect REST endpoints |
| 75 | [Testing and debugging PySpark](chapters/ch75-testing-debugging-pyspark.md) | pyspark.testing, pytest fixtures for Classic and Connect, UDF test traps, executor tracebacks, faulthandler, worker logs, a remote debugger |
| 76 | [Spark on Kubernetes with the Spark Kubernetes Operator](chapters/ch76-kubernetes-operator.md) | SparkApplication and SparkCluster resources, a Connect server on Kubernetes, run for real on a local cluster |
| 77 | [Orchestrating Spark: Airflow, operators, YuniKorn and Celeborn](chapters/ch77-orchestration.md) | Airflow's Spark operators (submit, Kubernetes, Connect), the Kubernetes operator under Airflow, gang scheduling and queues with Apache YuniKorn, remote shuffle with Apache Celeborn |
| 78 | [Writing Spark pipelines with agents](chapters/ch78-pipelines-with-agents.md) | What agents get wrong in Spark (from the OSA Con 2026 study), and a harness of declare, scope and context: dry-run, a Connect sandbox, version-pinned skills, metric views |

## Part XIII: Extending Spark

Python data sources, DSv2 in Java, Catalyst extensions, Connect plugins, a capstone.

| # | Chapter | Covers |
|---|---|---|
| 79 | [Python data sources I: readers and partition planning](chapters/ch79-python-data-sources-readers.md) | A parallel reader for a custom format |
| 80 | [Python data sources II: APIs, databases and Arrow batches](chapters/ch80-python-data-sources-apis-databases.md) | JDBC ranges vs a Python source |
| 81 | [Python data sources III: filter pushdown](chapters/ch81-python-data-sources-pushdown.md) | Which filters Spark offers a source, and what goes wrong when a source accepts them |
| 82 | [Python data sources IV: writers and streaming](chapters/ch82-python-data-sources-writers-streaming.md) | Commit protocols, offsets, idempotent sinks |
| 83 | [Shipping a source: packaging, testing and debugging](chapters/ch83-shipping-a-python-data-source.md) | Registration, tests, logging inside workers |
| 84 | [A DSv2 connector in Java](chapters/ch84-dsv2-connector-java.md) | Capability table that bridges to the Python API |
| 85 | [Catalyst extensions: rules and native functions](chapters/ch85-catalyst-extensions.md) | **4.3:** the hand-written rule vs built-in UDF transpilation |
| 86 | [Spark Connect plugins](chapters/ch86-connect-plugins.md) | Relation, command and expression plugins |
| 87 | [Capstone: your own connector](chapters/ch87-capstone-connector.md) | Format, batch and pushdown, writer, streaming |

## Part XIV: Internals

Catalyst and Python, the worker protocol, VARIANT bytes, an upstream case study, building Spark.

| # | Chapter | Covers |
|---|---|---|
| 88 | [How Catalyst plans Python](chapters/ch88-catalyst-plans-python.md) | **4.3:** transpilation as the closing section |
| 89 | [The Python worker protocol](chapters/ch89-python-worker-protocol.md) | Wire format, failure modes, memory limits |
| 90 | [The VARIANT binary format](chapters/ch90-variant-binary-format.md) | The bytes, the metadata dictionary and shredding |
| 91 | [From wrong results to a fix: an upstream case study](chapters/ch91-upstream-case-study.md) | Reproducing, minimizing and reporting a Spark bug |
| 92 | [Building Spark from source](chapters/ch92-building-spark.md) | Building the v4.3.0 tag in Docker, profiles, hadoop-provided builds and what they need to run |
