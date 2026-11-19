# Roadmap

The book is being written a part at a time, in order. Part I is drafted; each later part lists its chapters and what they cover. Chapter numbers may still move.

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

## Part V: Streaming
*Data: taxi trips replayed to Kafka, messy events*

| # | Chapter | Covers |
|---|---|---|
| 23 | Structured Streaming fundamentals | Sources and sinks, output modes, triggers, windows, watermarks and checkpoints |
| 24 | Streaming messy data: drift, dead letters, late events | The future-dated event that wrecks a watermark |
| 25 | Stateful processing with transformWithState | Timers, TTL, state inspection |
| 26 | Real-time Mode | **4.3:** stateful RTM and the pipelined shuffle, latency measured both ways |
| 27 | Inside a checkpoint and the state store | Delta files, RocksDB, the state data source |

## Part VI: Pipelines and tables
*Data: SparkTutor's dirty retail set*

| # | Chapter | Covers |
|---|---|---|
| 28 | Tables: Delta and Iceberg, MERGE and time travel | The transaction log on disk. |
| 29 | Declarative Pipelines: the model and the CLI | The project layout and spec the tutor used to hide; dry-run output |
| 30 | Medallion in practice: bronze, silver, gold and quality | Incrementality shown across two runs; the event log |
| 31 | Change data capture: reading changes and Auto CDC | **4.3:** SCD Type 2 on the Toronto, Ottawa, Hanoi changes |
| 32 | Metric views | A semantic layer in Spark SQL: dimensions, measures and `measure()` |
| 33 | A pipeline end to end, and how SDP runs it | Under the hood rewritten from the real SDP source, not the regex toy |

## Part VII: Machine learning and applied algorithms
*Data: taxi fares, messy documents*

| # | Chapter | Covers |
|---|---|---|
| 34 | ML pipelines with MLlib, tracked in MLflow | ML over Connect; open source MLflow for tracking |
| 35 | Text analytics without an LLM | TF-IDF search and near-duplicates |
| 36 | Similarity search: vectors, LSH and IVF | Recall against work; NEAREST BY top-K |
| 37 | Entity resolution and connected components | Plan growth in iterative jobs, `localCheckpoint` |
| 38 | Sketches: HLL, Theta and KLL | Mergeable summaries for profiling |
| 39 | Calling models and services from Spark | Retries, batching, request counts |

## Part VIII: Running Spark in production
| # | Chapter | Covers |
|---|---|---|
| 40 | Determinism, retries and fault injection | Connect operation IDs for reattach |
| 41 | Shipping Python dependencies | Archives, environments and what actually reaches the workers |
| 42 | Observability: listeners, event logs, metrics, profilers | **4.3:** Connect REST endpoints |

## Part IX: Extending Spark
*Data: messy set, local HTTP and SQLite*

| # | Chapter | Covers |
|---|---|---|
| 43 | Python data sources I: readers and partition planning | A parallel reader for a custom format |
| 44 | Python data sources II: APIs, databases and Arrow batches | JDBC ranges vs a Python source |
| 45 | Python data sources III: filter pushdown | Which filters Spark offers a source, and what goes wrong when a source accepts them |
| 46 | Python data sources IV: writers and streaming | Commit protocols, offsets, idempotent sinks |
| 47 | Shipping a source: packaging, testing and debugging | Registration, tests, logging inside workers |
| 48 | A DSv2 connector in Java | Capability table that bridges to the Python API |
| 49 | Catalyst extensions: rules and native functions | **4.3:** the hand-written rule vs built-in UDF transpilation |
| 50 | Spark Connect plugins | Relation, command and expression plugins |
| 51 | Capstone: your own connector | Format, batch and pushdown, writer, streaming |

## Part X: Internals
| # | Chapter | Covers |
|---|---|---|
| 52 | How Catalyst plans Python | **4.3:** transpilation as the closing section |
| 53 | The Python worker protocol | Wire format, failure modes, memory limits |
| 54 | The VARIANT binary format | The bytes, the metadata dictionary and shredding |
| 55 | From wrong results to a fix: an upstream case study | Reproducing, minimizing and reporting a Spark bug |
