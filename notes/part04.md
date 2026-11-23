# Part IV notes: Performance (chapters 18 to 23, old numbering)

## Coverage table

| Lesson | Topic | Where |
|---|---|---|
| LS06 optimization_tuning | key configs, runtime vs static, `CANNOT_MODIFY_CONFIG` | ch42 Step 1 |
| LS06 | `repartition`, `coalesce`, the `coalesce(1)` trap | ch42 Step 3 |
| LS06 | AQE coalescing, `isFinalPlan` only after `collect()` | ch40 Steps 6, 7 |
| LS06 | `cache`/`persist`/`unpersist`, lazy cache, storage levels, `MEMORY_AND_DISK_DESER`, no `_SER` | ch42 Step 5 |
| LS06 | Arrow by default, `toArrow`, `maxRecordsPerBatch`, `useArrow=False` diagnosis | ch43 Steps 2, 6; ch44 Step 3 |
| LS06 | tuning checklist, Connect-safe `spark_partition_id`, the Spark UI | ch42 Steps 3, 9 |
| LS06 | 4.3: Feather, transpilation | ch46; ch43 Step 4 |
| LS06 | order-independent shuffle checksums | left out: not a performance topic of this part; belongs with determinism (final ch72) |
| MD16 plans_partitions_caching | reading plans, `PushedFilters`, explain modes | ch40 Steps 1 to 3 |
| MD16 | file partitions, small files, splittable codecs | ch42 Step 2 |
| MD16 | caching and file lists, `refreshByPath` | ch42 Step 5 |
| MD16 | `partitionBy` layout and pruning, type inference of folder names | ch42 Step 4 |
| MD24 arrow_performance | Python value to Spark type conversion table (re-measured on 4.3) | ch43 Step 7 (UDF return values), ch44 Step 4 (UDTF yields) |
| MD24 | worker time zone trap, `timestamp_ntz` | ch43 Step 7 |
| MD24 | data source `records_to_arrow_batches`, yielded RecordBatch, schema checks | left out: Python data source specific; Part IX (final ch79 to 54) |
| MD30 python_udtfs | where a UDTF fits | ch44 Step 1 |
| MD30 | lifecycle, `terminate`, `cleanup`, empty partition failure | ch44 Step 2 |
| MD30 | Arrow decided at `udtf()` time, eval types | ch44 Step 3 |
| MD30 | yield conversion table Arrow vs not | ch44 Step 4 |
| MD30 | scalar args, `lateralJoin`, SQL `LATERAL`, `LEFT JOIN LATERAL` row loss and `ON true` | ch44 Step 5 |
| MD30 | `TABLE` args, `PARTITION BY`/`ORDER BY`, `asTable()`, parser trap | ch44 Step 6 |
| MD30 | `analyze()`, `AnalyzeResult`, required partitioning | ch44 Step 7 |
| MD30 | `arrow_udtf`, lateral restriction | ch44 Step 8 |
| MD30 | 4.3 re-checks: transpilation and UDTFs, JSON_TABLE | ch44 Steps 9, 10 |
| MD30 | `SkipRestOfInputTableException`, `asDeterministic()` | left out: one-line API details, in the PySpark docs |
| MD32 tuning_python | where the time goes, the plan-node catalogue with eval types | ch43 Steps 1, 2 |
| MD32 | benchmark built-in vs pandas vs Arrow vs pickled (re-measured, plus transpiled) | ch43 Step 3 |
| MD32 | call counts with an accumulator, double evaluation, `asNondeterministic`, `count()` | ch43 Step 5 |
| MD32 | Python filter blocks pushdown (plus transpiled filter) | ch43 Step 5 |
| MD32 | 4.2 columnar-input hang (SPARK-58241) | left out: fixed in 4.3.0 per the 4.3 git log (commit a84fcaae64d); the columnar path itself is in ch43 Step 6 |
| MD32 | UDF profiler | ch43 Step 8 |
| MD32 | repartition after a gzip read | ch42 Step 2 (gzip makes one task; repartition after the read) |
| MD32 | salting `applyInPandas` | ch43 Step 9 |
| MD32 | settings table, `spark.python.*` static | ch43 Step 10 |
| MD41 memory_and_blobs | unified memory model, read live, off-heap, `INVALID_DRIVER_MEMORY` | ch42 Step 6 |
| MD41 | measured sort spill, two spill metrics, AQE coalescing and spill | ch42 Step 7 |
| MD41 | `binaryFile`, `maxLength`, 2 GiB limit, 300 MiB OOM on 1 GiB | ch42 Step 8 |
| MD41 | Arrow batch sizing into Python | ch43 Step 6 |
| MD41 | byte-range chunking, archives by entry, blob layout table | left out: belongs with blobs and archives (final ch31, ch37) and Python data sources (Part IX); ch42 points to them |
| MD45 parquet_internals | file layout, magic, footer length, hexdump | ch45 Step 1 |
| MD45 | Thrift compact reader, field ids, Spark schema JSON, pyarrow cross-check | ch45 Step 2 |
| MD45 | pages, dictionary fallback, v2 encodings | ch45 Step 3 |
| MD45 | codec size table | ch45 Step 7 |
| MD45 | filter path, row-group/dictionary/bloom/column-index filters, `numOutputRows` measure, layouts and ablation | ch45 Steps 4, 5 |
| MD45 | INT96 not pushable while listed | ch45 Step 6 |
| MD45 | nanosecond timestamps (4.3 re-check) | ch45 Step 8 |
| MD45 | splits keep row groups by midpoint | ch42 Step 2 and Chapter 5 |
| MD45 | RLE/bit-packed hexdump decoder, rep/def levels, shredded VARIANT skipping, `inFilterThreshold`, aggregate pushdown with DSv2, `V1Writes` sort trap | left out: depth beyond the part's scope; VARIANT shredding is final ch35, VARIANT bytes are final ch90 |
| Project Feather (4.3) | the 4.3 source, which queries qualify | ch46 Steps 1, 2 |
| Feather | effects on plans, AQE, the UI | ch46 Steps 3, 4 |
| Feather | crossover across data sizes | ch46 Step 5 |
| Feather | cold start | ch46 Step 6 |
| Feather | Spark Connect | ch46 Step 7 |
| Feather | Arrow cache serializer | ch46 Step 8; ch42 Step 5 note |

## Claim-to-source table

Numbers quoted in the chapters come from the saved outputs below; source claims from the 4.3.0 tag of `~/repos/spark`.

| Chapter | Claim | Source |
|---|---|---|
| ch40 | explain modes, parsed/analyzed/optimized plans, cost estimates | `code/ch40/output/explain_modes.txt` |
| ch40 | 272 rules ran, 9 changed the plan | `code/ch40/output/optimizer.txt` |
| ch40 | nested pruning, JSON pushes nothing | `code/ch40/output/file_scan.txt` |
| ch40 | coalesce, join conversion with `nonEmptyPartitionRatioForBroadcastJoin`, skew split (87 tasks), actions and `isFinalPlan`, AQE defaults | `code/ch40/output/aqe.txt` |
| ch40 | AQE wraps a plan only with an Exchange, a subquery, or a required distribution | `InsertAdaptiveSparkPlan.shouldApplyAQE` |
| ch40 | `maxReducerPartitionsPerTask` (SPARK-58004), `convertSortMergeJoinToShuffledHashJoin` (SPARK-58084) new in 4.3 | earlier writer; `SQLConf.scala` version fields 4.3.0 |
| ch42 | settings and `CANNOT_MODIFY_CONFIG` | `code/ch42/output/config.txt` |
| ch42 | predicted vs actual input partitions; row groups and busy partitions | `code/ch42/output/file_partitions.txt`; `FilePartition.scala` |
| ch42 | repartition variants; coalesce(1) 67.99 s vs repartition(1) 19.81 s vs 17.70 s | `code/ch42/output/repartition.txt` (locked) |
| ch42 | partitionBy layouts, files read per filter, folder type inference | `code/ch42/output/layout.txt` |
| ch42 | cache lazy, 747 MiB, 450/480/531 ms, 3283/27/518 ms, levels, file list | `code/ch42/output/caching.txt` (locked) |
| ch42 | memory pools per heap; 1/(2N) to 1/N | `code/ch42/output/memory.txt`; `UnifiedMemoryManager.scala`, `ExecutionMemoryPool.scala` |
| ch42 | spill table | `code/ch42/output/spill.txt` (locked) |
| ch42 | binaryFile limits and OOM | `code/ch42/output/blobs.txt`; `BinaryFileFormat.scala` |
| ch42 | UI tour: jobs, 3,520 MiB / 971 MiB spill, storage 95 MiB | `code/ch42/output/ui_tour.txt`; Environment highlights non-defaults: `environmentpage.js` |
| ch43 | plan nodes and eval types | `code/ch43/output/plan_nodes.txt`; `pyspark/util.py` `PythonEvalType` |
| ch43 | benchmark | `code/ch43/output/bench.txt` (locked) |
| ch43 | transpiler conditions and accepted constructs | `pyspark/sql/udf.py` lines 200 to 310, `pyspark/sql/transpile.py`; `code/ch43/output/transpile.txt` |
| ch43 | call counts, pushdown, profiler | `code/ch43/output/calls.txt` |
| ch43 | batch sizes, columnar input path, conversion table, worker time zone | `code/ch43/output/arrow_batches.txt`; `ArrowEvalPythonExec.scala`, `ColumnarArrowEvalPythonEvaluatorFactory.scala` |
| ch43 | Arrow default for UDF/UDTF since 4.2; `convertToArrowArraySafely` default since 4.1 | `python/docs/source/migration_guide/pyspark_upgrade.rst` |
| ch43 | salting | `code/ch43/output/salting.txt` (locked) |
| ch44 | lifecycle, Arrow choice, conversion, lateral, TABLE, analyze, arrow_udtf | `code/ch44/output/udtf_core.txt` |
| ch44 | JSON_TABLE syntax and limits | `docs/sql-ref-syntax-qry-select-json-table.md`; `code/ch44/output/json_table.txt` (locked) |
| ch44 | UDTFs excluded from transpiler and Feather | `udf.py` (`SQL_BATCHED_UDF` check), `MarkSingleTaskExecution.unsupportedPatterns` |
| ch45 | hexdump, footer, pages | `code/ch45/output/footer.txt` |
| ch45 | layouts, ablation, timestamps, v2 pages, nanos | `code/ch45/output/skipping.txt` |
| ch45 | codecs | `code/ch45/output/skipping_codecs.txt` (locked) |
| ch45 | parquet-mr 1.17.0, INT96 default | jar names in `pyspark/jars`; `SQLConf.scala` `outputTimestampType` |
| ch46 | the rule, the scan check, the batch position | `MarkSingleTaskExecution.scala`, `DataSourceScanExec.scala` 318 to 352 and 780 to 795, `SparkOptimizer.scala` line 111, `SQLConf.scala` 7950 to 8053 |
| ch46 | eligibility table | `code/ch46/output/eligibility.txt` |
| ch46 | plans, UI, task times | `code/ch46/output/plans_aqe.txt` |
| ch46 | crossover | `code/ch46/output/crossover.txt` (locked) |
| ch46 | cold start | `code/ch46/output/cold.txt` (locked) |
| ch46 | Connect | `code/ch46/output/connect.txt` (locked) |
| ch46 | Arrow cache | `code/ch46/output/arrow_cache.txt` (locked); SPARK-57268 commit 7e43efecc45 |
| ch46 | SPARK-57851 and SPARK-55206 commits | `git log v4.3.0` (0b5312e1b69, 454b5cb0249) |

## Technical names and technical verbs

Technical names (nouns, in code format where they are identifiers): Spark, PySpark, Catalyst, AQE, JVM, executor, driver, task, stage, job, partition, shuffle, `Exchange`, query stage, row group, column chunk, page, footer, dictionary, bloom filter, column index, statistics, codec, Arrow batch, pandas, UDF, UDTF, Python worker, accumulator, cache, storage level, spill, heap, setting, Spark UI, REST API, Spark Connect, session, Project Feather, single-task execution, transpiler, JSON_TABLE, lateral join, `TABLE` argument, plan node names (`HashAggregate`, `SortMergeJoin`, `BroadcastHashJoin`, `ArrowEvalPython`, `BatchEvalPython`, `ArrowEvalPythonUDTF`, `InMemoryTableScan`, `AQEShuffleRead`, `FileScan`), config keys, file formats (Parquet, CSV, JSON, gzip, bzip2, INT96, `TIMESTAMP_MICROS`).

Technical verbs: shuffle, broadcast, cache, spill, coalesce, partition, repartition, prune, push (a filter into a scan), transpile, collect, commit, compile, serialize, deserialize, fork, decode, encode, parse, sort, join, write, read, run, record, profile.

Rules broken on purpose: none. Words allowlisted per chapter with `--allow` (technical nouns that end in `-ing`): `setting`, `caching`, `repartitioning`, `packing`, `timing`, `cleaning`. Final counts are from `scripts/ste-check FILE --allow setting caching repartitioning packing timing cleaning skipping` (the wrapper as fixed by the integrator in this worktree).

## STE checker results

| Chapter | Findings | Extra allow words |
|---|---|---|
| ch40 | 0 | none |
| ch42 | 0 | setting caching repartitioning packing timing cleaning (the same list is passed for ch43 to ch46) |
| ch43 | 0 | setting caching repartitioning packing timing cleaning |
| ch44 | 0 | same |
| ch45 | 0 | same, plus skipping |
| ch46 | 0 | same |

## Environment

- `pyspark[connect]==4.3.0` (uv project in `code/`), Python 3.12.3, pandas 2.3.3, PyArrow 26.0.1, NumPy 2.5.5, OpenJDK 17.0.21.1.
- Parquet writer inside Spark 4.3.0: parquet-mr (parquet-java) 1.17.0 (`pyspark/jars/parquet-*-1.17.0.jar`). ORC 2.3.0.
- `local[*]` on 16 cores, 156 GB RAM. Driver heap: 1g default; scripts that cache or sort six months set `spark_driver_memory="4g"` (noted in each script).
- Ports: `SPARK_UI_PORT=4024` for locked timing runs, 4134 to 4139 for unlocked runs and films. Connect server for chapter 46: `sc://localhost:10240`, UI 4124 (`code/ch46/server.sh`, its own PID and log folders).
- Data: `$BOOK_DATA/nyc-taxi/yellow` (June 2026, 3,837,248 rows), `yellow-h1` (January to June 2026, 22,836,530 rows), `taxi_zone_lookup.csv` (265 rows). SparkTutor `kv.log` copied to `code/data/service_logs/`. Generated data (2,000,000 events, 200,000 JSON orders, size files for the crossover) is written under `/tmp/masterclass-ch2x/` by the scripts.
- No external packages.
- Timings: `flock ~/spark-masterclass/data/.bench.lock` only for runs whose timings the book quotes (bench, salting, crossover, arrow_cache, cold, connect timing, json_table, the codec section of skipping, and in ch42 repartition, caching and spill). The machine ran other writers' unlocked jobs at the same time, so timings have more noise than a quiet machine; the chapters say where the order of close results is not meaningful.

## Benchmark check (shuffle-file reuse)

Every timed loop builds a new DataFrame for each run: `caching.py` (`median_ms(build)`), `spill.py`, `bench.py`, `salting.py`, `json_table.py`, the codec section of `skipping.py`, `crossover.py`, `cold.py` (new process), `connect.py`, `arrow_cache.py` (a new aggregate over the cached DataFrame each run), `demo.py`. The one deliberate reuse ("the same DataFrame object, collect() again", 27 ms) is shown as the trap in ch42 Step 5. `repartition.py` times repeated `noop` writes of one DataFrame; each write plans a new command, and the medians (17.70 s, 67.99 s, 19.81 s) show that the SHA-256 work ran in each run (a reused shuffle would take about a second).

## Open findings (possible Spark bugs or doc gaps, not filed)

1. **`spark.sql.execution.arrow.maxRecordsPerBatch=-1` fails on the columnar Arrow UDF path** with `java.lang.NegativeArraySizeException: -1` at `OnHeapColumnVector.reserveInternal`, from `ColumnarArrowEvalPythonEvaluatorFactory.rowsToColumnarBatches` (it allocates `batchSize` rows). The config doc says "If set to zero or negative there is no limit". Repro: classic 4.3.0, `spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "-1")`, then a `@pandas_udf` over a column of a Parquet scan (`code/ch43/arrow_batches.py`). `0` works.
2. **With columnar input on (4.2+ default), `maxRecordsPerBatch` does not set the Python batch size** for a pandas UDF over a vectorized Parquet scan: batches follow `spark.sql.parquet.columnarReaderBatchSize` (4,096). Documented behaviour of the setting suggests otherwise; a doc gap at least. Output in `code/ch43/output/arrow_batches.txt`.
3. **Arrow `@udf` truncates 7.9 to 7 for an `int` return type** while `spark.sql.execution.pandas.convertToArrowArraySafely=true`, and a `@pandas_udf` with the same value fails with `PySparkValueError`. The PySpark migration guide (4.1) says the setting affects "arrow-enabled UDFs/pandas_udfs". Output: `code/ch43/output/arrow_batches.txt`.
4. **Python UDTF whose `terminate()` yields a row fails with `NullPointerException` when an input partition is empty**, in a lateral join and with a plain `TABLE` argument (`EvalPythonUDTFExec` joins the row to a null `row1`). Minimal repro: `spark.range(6).repartition(3)` (one partition empty in this run) then `df.lateralJoin(T(F.col("id").outer()))` where `T.terminate` yields `(-1,)`. SparkTutor MD30 recorded the same on 4.2. Output: `code/ch44/output/udtf_core.txt`.
5. **`LEFT JOIN LATERAL` with a Python UDTF and no `ON` clause drops the left rows for which the UDTF yields nothing** (0 rows with NULL key; with `ON true`, 3). SparkTutor MD30 cites SPARK-60564 for this; the JIRA number is not verified here. Output: `code/ch44/output/udtf_core.txt`.
6. **Classic PySpark cannot parse `timestamp_ntz(9)`**: with `spark.sql.timestampNanosTypes.enabled=true`, `df.schema` and `df.first()` fail with `CANNOT_PARSE_DATATYPE` (the JVM schema is `struct<t:timestamp_ntz(9)>`). The feature is an internal preview, so this may be expected. Output: `code/ch45/output/skipping.txt`.
7. **INT96 timestamp filters appear in `PushedFilters` but cannot skip** (no statistics). Known Parquet behaviour; the plan text does not show it. Output: `code/ch45/output/skipping.txt`.
8. Not a bug, an observation not explained: in `code/ch42/ui_tour.py` the June sort (2 partitions, AQE off, 1g heap) did not spill, while the same sort in `spill.py` and in a stand-alone test spilled 554 MiB on its first run. The tour sorts six months instead. Not investigated further.
10. **A Connect client hung in `SparkSession.stop()`** after a session and its `newSession()` ran queries (`code/ch46/connect.py brief`, before the change to `os._exit`). One full run of the same script also hung in the middle, with the server idle; a fresh server fixed it. Not reduced to a minimal repro.
9. `SparkEnv.get().memoryManager()` through Py4J failed on 4.3.0 (earlier writer's note in `code/ch42/memory.py`); the script reads the pools from the REST API instead.

## Nav entries

```yaml
  - "Part IV: Performance":
    - chapters/ch40-query-plans.md
    - chapters/ch42-partitions-caching-memory.md
    - chapters/ch43-python-udfs-arrow.md
    - chapters/ch44-python-udtfs.md
    - chapters/ch45-parquet-inside.md
    - chapters/ch46-project-feather.md
```
