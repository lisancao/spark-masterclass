# Roadmap

The author writes the book one part at a time, in sequence. Part I has a first draft. Each later part lists its chapters and the subjects that they cover. The chapter numbers can still change. Each SparkTutor lesson goes into one of these chapters. The map from lessons to chapters is in [coverage](coverage.md).

## Part I: Getting started
*Data: NYC Taxi trips*

| # | Chapter | Covers |
|---|---|---|
| 1 | [How Spark runs your code](chapters/ch01-how-spark-runs.md) | Jobs, stages, tasks, Python workers, Classic vs Connect. **4.3:** Feather single-task local execution |
| 2 | [Installing Spark 4.3](chapters/ch02-installing.md) | pip, the tarball, Docker and Spark Connect, each run for real; how to pick; the book's data |
| 3 | [Quick start](chapters/ch03-quick-start.md) | Read the taxi data, aggregate, the same query in SQL, the plan they share |
| 4 | [Spark Connect](chapters/ch04-spark-connect.md) | The unresolved plan, gRPC, sessions. **4.3:** operation IDs and REST endpoints |

## Part II: DataFrames and SQL
*Data: NYC Taxi, zones lookup*

| # | Chapter | Covers |
|---|---|---|
| 5 | DataFrames, columns and schemas | Analyzed plans for `withColumn` chains, nullability. **4.3:** `DataFrame.zip` |
| 6 | Cleaning values: ANSI, try_ functions, strings, time | Collations, money, dates. **4.3:** nanosecond timestamps |
| 7 | Spark SQL in depth: windows, pipe syntax, QUALIFY | Window functions get taught properly (LS04 used them untaught) |
| 8 | Procedural SQL: scripting, SQL UDFs, recursive CTEs | Variables, loops, handlers, `EXECUTE IMMEDIATE` and the SQL path |
| 9 | Joins: strategies, skew and the time-aware joins | Broadcast vs sort-merge on real sizes, NEAREST BY. **4.3:** ASOF JOIN |
| 10 | Time series and geospatial | `time_bucket`, GEOMETRY and GEOGRAPHY. |

## Part III: Files, formats and messy data
*Data: SparkTutor's seeded messy set*

| # | Chapter | Covers |
|---|---|---|
| 11 | Files as files, globs and metadata | **4.3:** reading zip and tar archives |
| 12 | Text, logs and CSV | Corrupt-record modes, layout drift, encodings |
| 13 | JSON: inference, drift and quarantine | **4.3:** JSON_TABLE |
| 14 | Structs, arrays, maps and higher-order functions | How lambdas are traced into Catalyst |
| 15 | VARIANT: landing, profiling and editing | Shredding and extraction speed. **4.3:** VARIANT edits |
| 16 | XML, Avro and Protobuf | The Kafka wire format |
| 17 | Documents, images, audio and tables for blobs | Archives as the opener |

## Part IV: Performance
*Data: NYC Taxi at full months*

| # | Chapter | Covers |
|---|---|---|
| 18 | Reading query plans: Catalyst and AQE | One AQE explanation instead of three; real `explain` output |
| 19 | Partitions, caching, memory and spill | Recorded Spark UI walkthrough. |
| 20 | Python on Spark: UDFs, pandas UDFs and Arrow | **4.3:** UDF transpilation; the tuning tables get re-measured |
| 21 | Python UDTFs | JSON_TABLE as the alternative to parsing UDTFs |
| 22 | Parquet from the inside | Footer hexdumps, row groups, statistics, pushdown |
| 23 | Project Feather: small queries without the distributed tax | **4.3:** when single-task execution applies, the crossover measured across data sizes, what it does to plans, AQE, the UI and Connect |
| 24 | Acceleration: Gluten and Velox, RAPIDS and cuDF, Comet | Native and GPU execution engines for Spark: how each one plugs in (plugins, columnar rules, fallbacks), which support Spark 4.3, and the same taxi queries measured on CPU, Velox and the GPU |

## Part V: Streaming
*Data: taxi trips replayed to Kafka, messy events*

| # | Chapter | Covers |
|---|---|---|
| 25 | Structured Streaming fundamentals | Sources and sinks, output modes, triggers, windows, watermarks and checkpoints |
| 26 | Streaming messy data: drift, dead letters, late events | The future-dated event that wrecks a watermark |
| 27 | Stateful processing with transformWithState | Timers, TTL, state inspection |
| 28 | Real-time Mode | **4.3:** stateful RTM and the pipelined shuffle, latency measured both ways |
| 29 | Inside a checkpoint and the state store | Delta files, RocksDB, the state data source |

## Part VI: Pipelines and tables
*Data: SparkTutor's dirty retail set*

| # | Chapter | Covers |
|---|---|---|
| 30 | Tables: Delta and Iceberg, MERGE and time travel | The transaction log on disk. |
| 31 | [Apache Iceberg with Spark: the runtime and catalog management](chapters/ch31-iceberg.md) | The `iceberg-spark-runtime` jar, Spark catalogs (`SparkCatalog`, `SparkSessionCatalog`), REST, JDBC and Hadoop catalogs, namespaces, Iceberg SQL extensions and procedures, table maintenance |
| 32 | Declarative Pipelines: the model and the CLI | The project layout and spec the tutor used to hide; dry-run output |
| 33 | Medallion in practice: bronze, silver, gold and quality | Incrementality shown across two runs; the event log |
| 34 | Change data capture: reading changes and Auto CDC | **4.3:** SCD Type 2 on the Toronto, Ottawa, Hanoi changes |
| 35 | Metric views | A semantic layer in Spark SQL: dimensions, measures and `measure()` |
| 36 | A pipeline end to end, and how SDP runs it | Under the hood rewritten from the real SDP source, not the regex toy |

## Part VII: Machine learning and applied algorithms
*Data: taxi fares, messy documents*

| # | Chapter | Covers |
|---|---|---|
| 37 | ML pipelines with MLlib, tracked in MLflow | ML over Connect; open source MLflow for tracking |
| 38 | Text analytics without an LLM | TF-IDF search and near-duplicates |
| 39 | Similarity search: vectors, LSH and IVF | Recall against work; NEAREST BY top-K |
| 40 | Entity resolution and connected components | Plan growth in iterative jobs, `localCheckpoint` |
| 41 | Sketches: HLL, Theta and KLL | Mergeable summaries for profiling |
| 42 | Calling models and services from Spark | Retries, batching, request counts |

## Part VIII: Running Spark in production
| # | Chapter | Covers |
|---|---|---|
| 43 | Determinism, retries and fault injection | Connect operation IDs for reattach |
| 44 | Shipping Python dependencies | Archives, environments and what actually reaches the workers |
| 45 | Observability: listeners, event logs, metrics, profilers | **4.3:** Connect REST endpoints |
| 46 | Spark Connect in production: deployment patterns | One server per app or shared, sessions and isolation, auth and proxies, reattach, a sandbox per agent. **4.3:** operation IDs, REST endpoints, Feather on the server |
| 47 | Spark on Kubernetes with the Spark Kubernetes Operator | SparkApplication and SparkCluster resources, a Connect server on Kubernetes, run for real on a local cluster |
| 48 | [Orchestrating Spark: Airflow, operators, YuniKorn and Celeborn](chapters/ch48-orchestration.md) | Airflow's Spark operators (submit, Kubernetes, Connect), the Kubernetes operator under Airflow, gang scheduling and queues with Apache YuniKorn, remote shuffle with Apache Celeborn |
| 49 | Writing Spark pipelines with agents | What agents get wrong in Spark (from the OSA Con 2026 study), and a harness of declare, scope and context: dry-run, a Connect sandbox, version-pinned skills, metric views |

## Part IX: Extending Spark
*Data: messy set, local HTTP and SQLite*

| # | Chapter | Covers |
|---|---|---|
| 50 | [Python data sources I: readers and partition planning](chapters/ch50-python-data-sources-readers.md) | A parallel reader for a custom format |
| 51 | [Python data sources II: APIs, databases and Arrow batches](chapters/ch51-python-data-sources-apis-databases.md) | JDBC ranges vs a Python source |
| 52 | [Python data sources III: filter pushdown](chapters/ch52-python-data-sources-pushdown.md) | Which filters Spark offers a source, and what goes wrong when a source accepts them |
| 53 | [Python data sources IV: writers and streaming](chapters/ch53-python-data-sources-writers-streaming.md) | Commit protocols, offsets, idempotent sinks |
| 54 | [Shipping a source: packaging, testing and debugging](chapters/ch54-shipping-a-python-data-source.md) | Registration, tests, logging inside workers |
| 55 | [A DSv2 connector in Java](chapters/ch55-dsv2-connector-java.md) | Capability table that bridges to the Python API |
| 56 | [Catalyst extensions: rules and native functions](chapters/ch56-catalyst-extensions.md) | **4.3:** the hand-written rule vs built-in UDF transpilation |
| 57 | [Spark Connect plugins](chapters/ch57-connect-plugins.md) | Relation, command and expression plugins |
| 58 | [Capstone: your own connector](chapters/ch58-capstone-connector.md) | Format, batch and pushdown, writer, streaming |

## Part X: Internals
| # | Chapter | Covers |
|---|---|---|
| 59 | [How Catalyst plans Python](chapters/ch59-catalyst-plans-python.md) | **4.3:** transpilation as the closing section |
| 60 | [The Python worker protocol](chapters/ch60-python-worker-protocol.md) | Wire format, failure modes, memory limits |
| 61 | [The VARIANT binary format](chapters/ch61-variant-binary-format.md) | The bytes, the metadata dictionary and shredding |
| 62 | [From wrong results to a fix: an upstream case study](chapters/ch62-upstream-case-study.md) | Reproducing, minimizing and reporting a Spark bug |
