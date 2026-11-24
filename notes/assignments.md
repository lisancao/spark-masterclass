# Writer assignments

Each writer reads `notes/chapter-brief.md` first; this file gives its scope. Chapter numbers here are the numbers in the writer's file names (the old numbering). The integrator renumbers to the final roadmap with `notes/renumber.md`. Cross-references to chapters in other parts: use the final numbers from `book/roadmap.md`.

## part02: Part II
- Worktree `part02`, branch `part-02`. `SPARK_UI_PORT=4022`, Connect port 10220 if needed (UI 4122). Kind cluster name, if needed: `part02`.
- Chapters:
  - 5 DataFrames, columns and schemas: Analyzed plans for `withColumn` chains, nullability. **4.3:** `DataFrame.zip`
  - 6 Cleaning values: ANSI, try_ functions, strings, time: Collations, money, dates. **4.3:** nanosecond timestamps
  - 7 Spark SQL in depth: windows, pipe syntax, QUALIFY: Window functions get taught properly (LS04 used them untaught)
  - 8 Procedural SQL: scripting, SQL UDFs, recursive CTEs: Variables, loops, handlers, `EXECUTE IMMEDIATE` and the SQL path
  - 9 Joins: strategies, skew and the time-aware joins: Broadcast vs sort-merge on real sizes, NEAREST BY. **4.3:** ASOF JOIN
  - 10 Time series and geospatial: `time_bucket`, GEOMETRY and GEOGRAPHY.
- SparkTutor lessons to cover in full: LS02 dataframes_schemas, LS05 spark_sql_deep_dive, LS07 join_strategies, MD03 schemas, MD04 cleaning_values, MD21 whats_new_4x (SQL parts), WN03 pipe_syntax_qualify, WN04 sql_scripting, WN05 geospatial, WN08 cdc_nearest_metrics (NEAREST BY)

## part03: Part III
- Worktree `part03`, branch `part-03`. `SPARK_UI_PORT=4023`, Connect port 10230 if needed (UI 4123). Kind cluster name, if needed: `part03`.
- Chapters:
  - 11 Files as files, globs and metadata: **4.3:** reading zip and tar archives
  - 12 Text, logs and CSV: Corrupt-record modes, layout drift, encodings
  - 13 JSON: inference, drift and quarantine: **4.3:** JSON_TABLE
  - 14 Structs, arrays, maps and higher-order functions: How lambdas are traced into Catalyst
  - 15 VARIANT: landing, profiling and editing: Shredding and extraction speed. **4.3:** VARIANT edits
  - 16 XML, Avro and Protobuf: The Kafka wire format
  - 17 Documents, images, audio and tables for blobs: Archives as the opener
- SparkTutor lessons to cover in full: LS03 data_sources, LS04 external_sources_hof, MD05 files_as_files, MD06 text_and_logs, MD07 csv_and_delimited, MD08 json_drift, MD09 complex_types, MD10 variant_basics, MD11 xml, MD12 avro_and_protobuf, MD13 documents_to_rows, MD14 images_and_audio, MD31 variant_shredding, MD47 blob_table_design, WN02 variant

## part04: Part IV
- Worktree `part04`, branch `part-04`. `SPARK_UI_PORT=4024`, Connect port 10240 if needed (UI 4124). Kind cluster name, if needed: `part04`.
- Chapters:
  - 18 Reading query plans: Catalyst and AQE: One AQE explanation instead of three; real `explain` output
  - 19 Partitions, caching, memory and spill: Recorded Spark UI walkthrough.
  - 20 Python on Spark: UDFs, pandas UDFs and Arrow: **4.3:** UDF transpilation; the tuning tables get re-measured
  - 21 Python UDTFs: JSON_TABLE as the alternative to parsing UDTFs
  - 22 Parquet from the inside: Footer hexdumps, row groups, statistics, pushdown
  - 23 Project Feather: small queries without the distributed tax: **4.3:** when single-task execution applies, the crossover measured across data sizes, what it does to plans, AQE, the UI and Connect
- SparkTutor lessons to cover in full: LS06 optimization_tuning, MD16 plans_partitions_caching, MD24 arrow_performance, MD30 python_udtfs, MD32 tuning_python, MD41 memory_and_blobs, MD45 parquet_internals; Project Feather (new in 4.3)
- Extra: native and GPU engines are a separate chapter (final ch24, another writer); point to it, do not cover them. Project Feather (old 23) must be full depth: the 4.3 source, which queries qualify, the crossover across data sizes, effects on plans, AQE, UI and Connect.

## part05: Part V
- Worktree `part05`, branch `part-05`. `SPARK_UI_PORT=4025`, Connect port 10250 if needed (UI 4125). Kind cluster name, if needed: `part05`.
- Chapters:
  - 24 Structured Streaming fundamentals: Sources and sinks, output modes, triggers, windows, watermarks and checkpoints
  - 25 Streaming messy data: drift, dead letters, late events: The future-dated event that wrecks a watermark
  - 26 Stateful processing with transformWithState: Timers, TTL, state inspection
  - 27 Real-time Mode: **4.3:** stateful RTM and the pipelined shuffle, latency measured both ways
  - 28 Inside a checkpoint and the state store: Delta files, RocksDB, the state data source
- SparkTutor lessons to cover in full: LS08 structured_streaming, MD17 streaming_files, MD33 streaming_messy_data, MD51 streaming_internals, WN07 transform_with_state; Real-time Mode (4.3)

## part06: Part VI
- Worktree `part06`, branch `part-06`. `SPARK_UI_PORT=4026`, Connect port 10260 if needed (UI 4126). Kind cluster name, if needed: `part06`.
- Chapters:
  - 29 Tables: Delta and Iceberg, MERGE and time travel: The transaction log on disk.
  - 30 Declarative Pipelines: the model and the CLI: The project layout and spec the tutor used to hide; dry-run output
  - 31 Medallion in practice: bronze, silver, gold and quality: Incrementality shown across two runs; the event log
  - 32 Change data capture: reading changes and Auto CDC: **4.3:** SCD Type 2 on the Toronto, Ottawa, Hanoi changes
  - 33 Metric views: A semantic layer in Spark SQL: dimensions, measures and `measure()`
  - 34 A pipeline end to end, and how SDP runs it: Under the hood rewritten from the real SDP source, not the regex toy
- SparkTutor lessons to cover in full: LS09 data_lakes, MD18 landing_in_delta, MD34 production_ingestion, SDP01-SDP09 (all nine), WN08 cdc_nearest_metrics (Auto CDC, metric views)
- Extra: the Iceberg catalog deep dive is a separate chapter (final ch31, another writer); your tables chapter stays the Delta vs Iceberg comparison. Metric views (your old ch33) must be full depth: the author's upstream branch (SPARK-60206) and her OSA Con deck (short-12/13, demos/metric_view_agent) show how she presents it; check what shipped in v4.3.0.

## part07: Part VII
- Worktree `part07`, branch `part-07`. `SPARK_UI_PORT=4027`, Connect port 10270 if needed (UI 4127). Kind cluster name, if needed: `part07`.
- Chapters:
  - 35 ML pipelines with MLlib, tracked in MLflow: ML over Connect; open source MLflow for tracking
  - 36 Text analytics without an LLM: TF-IDF search and near-duplicates
  - 37 Similarity search: vectors, LSH and IVF: Recall against work; NEAREST BY top-K
  - 38 Entity resolution and connected components: Plan growth in iterative jobs, `localCheckpoint`
  - 39 Sketches: HLL, Theta and KLL: Mergeable summaries for profiling
  - 40 Calling models and services from Spark: Retries, batching, request counts
- SparkTutor lessons to cover in full: LS10 mllib, MD19 text_analytics, MD20 inference_with_pandas_udfs, MD35 similarity_search, MD36 entity_resolution, MD37 inference_in_production, MD38 capstone_intermediate, MD48 lsh_and_ivf, MD49 connected_components, MD50 sketches

## part08: Part VIII
- Worktree `part08`, branch `part-08`. `SPARK_UI_PORT=4028`, Connect port 10280 if needed (UI 4128). Kind cluster name, if needed: `part08`.
- Chapters:
  - 41 Determinism, retries and fault injection: Connect operation IDs for reattach
  - 42 Shipping Python dependencies: Archives, environments and what actually reaches the workers
  - 43 Observability: listeners, event logs, metrics, profilers: **4.3:** Connect REST endpoints
  - 44 Spark Connect in production: deployment patterns: One server per app or shared, sessions and isolation, auth and proxies, reattach, a sandbox per agent. **4.3:** operation IDs, REST endpoints, Feather on the server
  - 45 Spark on Kubernetes with the Spark Kubernetes Operator: SparkApplication and SparkCluster resources, a Connect server on Kubernetes, run for real on a local cluster
  - 46 Writing Spark pipelines with agents: What agents get wrong in Spark (from the OSA Con 2026 study), and a harness of declare, scope and context: dry-run, a Connect sandbox, version-pinned skills, metric views
- SparkTutor lessons to cover in full: MD52 determinism_retries, MD53 shipping_dependencies, MD54 observability, LS11 spark_connect (server side); new: Connect deployment patterns, the Spark Kubernetes Operator, writing pipelines with agents (OSA Con 2026)
- Extra: orchestration (Airflow, YuniKorn, Celeborn, Volcano) is a separate chapter (final ch48, another writer); keep your Kubernetes chapter on the Apache Spark Kubernetes Operator. The agents chapter (old 46) lifts from the author's OSA Con 2026 deck (index.html, short-*.html, demos/); quote study numbers exactly as the deck states them, and read `~/.claude/projects/-home-lnc-repos/memory/safe-spark-agents-state.md` for which study claims are safe.

## part09a: Part IX (first half)
- Worktree `part09a`, branch `part-09a`. `SPARK_UI_PORT=4029`, Connect port 10290 if needed (UI 4129). Kind cluster name, if needed: `part09a`.
- Chapters:
  - 47 Python data sources I: readers and partition planning: A parallel reader for a custom format
  - 48 Python data sources II: APIs, databases and Arrow batches: JDBC ranges vs a Python source
  - 49 Python data sources III: filter pushdown: Which filters Spark offers a source, and what goes wrong when a source accepts them
  - 50 Python data sources IV: writers and streaming: Commit protocols, offsets, idempotent sinks
  - 51 Shipping a source: packaging, testing and debugging: Registration, tests, logging inside workers
- SparkTutor lessons to cover in full: MD15 first_python_data_source, MD22 custom_file_format, MD23 api_source, MD25 filter_pushdown, MD26 database_source, MD27 writers, MD28 streaming_sources, MD29 packaging_testing, WN06 python_data_sources

## part09b: Part IX (second half)
- Worktree `part09b`, branch `part-09b`. `SPARK_UI_PORT=4030`, Connect port 10300 if needed (UI 4130). Kind cluster name, if needed: `part09b`.
- Chapters:
  - 52 A DSv2 connector in Java: Capability table that bridges to the Python API
  - 53 Catalyst extensions: rules and native functions: **4.3:** the hand-written rule vs built-in UDF transpilation
  - 54 Spark Connect plugins: Relation, command and expression plugins
  - 55 Capstone: your own connector: Format, batch and pushdown, writer, streaming
- SparkTutor lessons to cover in full: MD42 java_dsv2_connector, MD43 session_extensions, MD44 connect_plugins, MD56 final_project

## part10: Part X
- Worktree `part10`, branch `part-10`. `SPARK_UI_PORT=4031`, Connect port 10310 if needed (UI 4131). Kind cluster name, if needed: `part10`.
- Chapters:
  - 56 How Catalyst plans Python: **4.3:** transpilation as the closing section
  - 57 The Python worker protocol: Wire format, failure modes, memory limits
  - 58 The VARIANT binary format: The bytes, the metadata dictionary and shredding
  - 59 From wrong results to a fix: an upstream case study: Reproducing, minimizing and reporting a Spark bug
- SparkTutor lessons to cover in full: MD39 catalyst_python_plans, MD40 python_worker_protocol, MD46 variant_binary_format, MD55 upstream_case_study
