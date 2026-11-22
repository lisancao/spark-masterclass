# Part IV notes: Performance (chapters 18 to 23, old numbering)

## Technical names and technical verbs

Technical names (nouns, in code format where they are identifiers): Spark, PySpark, Catalyst, AQE, JVM, executor, driver, task, stage, job, partition, shuffle, `Exchange`, query stage, row group, column chunk, page, footer, dictionary, bloom filter, column index, statistics, codec, Arrow batch, pandas, UDF, UDTF, Python worker, accumulator, cache, storage level, spill, heap, setting, Spark UI, REST API, Spark Connect, session, Project Feather, single-task execution, transpiler, JSON_TABLE, lateral join, `TABLE` argument, plan node names (`HashAggregate`, `SortMergeJoin`, `BroadcastHashJoin`, `ArrowEvalPython`, `BatchEvalPython`, `ArrowEvalPythonUDTF`, `InMemoryTableScan`, `AQEShuffleRead`, `FileScan`), config keys, file formats (Parquet, CSV, JSON, gzip, bzip2, INT96, `TIMESTAMP_MICROS`).

Technical verbs: shuffle, broadcast, cache, spill, coalesce, partition, repartition, prune, push (a filter into a scan), transpile, collect, commit, compile, serialize, deserialize, fork, decode, encode, parse, sort, join, write, read, run, record, profile.

Rules broken on purpose: none. Words allowlisted per chapter with `--allow` (technical nouns that end in `-ing`): `setting`, `caching`, `repartitioning`, `packing`, `timing`, `cleaning`. The shared `scripts/ste-check` wrapper drops extra `--allow` words when a mode is given (its `shift` logic), so the counts below come from `python3 scripts/ste_prep.py FILE | stecheck - --mode description --allow $(cat scripts/ste-allow.txt) <words>`.

## STE checker results

| Chapter | Findings | Extra allow words |
|---|---|---|
| ch18 | 0 | none |
| ch19 | 0 | setting caching repartitioning packing timing cleaning |
| ch20 | see below | |
| ch21 | see below | |
| ch22 | see below | |
| ch23 | see below | |

## Environment

- `pyspark[connect]==4.3.0` (uv project in `code/`), Python 3.12.3, pandas 2.3.3, PyArrow 26.0.1, NumPy 2.5.5, OpenJDK 17.0.21.1.
- Parquet writer inside Spark 4.3.0: parquet-mr (parquet-java) 1.17.0 (`pyspark/jars/parquet-*-1.17.0.jar`). ORC 2.3.0.
- `local[*]` on 16 cores, 156 GB RAM. Driver heap: 1g default; scripts that cache or sort six months set `spark_driver_memory="4g"` (noted in each script).
- Ports: `SPARK_UI_PORT=4024` for locked timing runs, 4134 to 4139 for unlocked runs and films. Connect server for chapter 23: `sc://localhost:10240`, UI 4124 (`code/ch23/server.sh`, its own PID and log folders).
- Data: `$BOOK_DATA/nyc-taxi/yellow` (June 2026, 3,837,248 rows), `yellow-h1` (January to June 2026, 22,836,530 rows), `taxi_zone_lookup.csv` (265 rows). SparkTutor `kv.log` copied to `code/data/service_logs/`. Generated data (2,000,000 events, 200,000 JSON orders, size files for the crossover) is written under `/tmp/masterclass-ch2x/` by the scripts.
- No external packages.
- Timings: `flock ~/spark-masterclass/data/.bench.lock` only for runs whose timings the book quotes (bench, salting, crossover, arrow_cache, cold, connect timing, json_table, the codec section of skipping, and in ch19 repartition, caching and spill). The machine ran other writers' unlocked jobs at the same time, so timings have more noise than a quiet machine; the chapters say where the order of close results is not meaningful.

## Open findings (possible Spark bugs or doc gaps, not filed)

1. **`spark.sql.execution.arrow.maxRecordsPerBatch=-1` fails on the columnar Arrow UDF path** with `java.lang.NegativeArraySizeException: -1` at `OnHeapColumnVector.reserveInternal`, from `ColumnarArrowEvalPythonEvaluatorFactory.rowsToColumnarBatches` (it allocates `batchSize` rows). The config doc says "If set to zero or negative there is no limit". Repro: classic 4.3.0, `spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "-1")`, then a `@pandas_udf` over a column of a Parquet scan (`code/ch20/arrow_batches.py`). `0` works.
2. **With columnar input on (4.2+ default), `maxRecordsPerBatch` does not set the Python batch size** for a pandas UDF over a vectorized Parquet scan: batches follow `spark.sql.parquet.columnarReaderBatchSize` (4,096). Documented behaviour of the setting suggests otherwise; a doc gap at least. Output in `code/ch20/output/arrow_batches.txt`.
3. **Arrow `@udf` truncates 7.9 to 7 for an `int` return type** while `spark.sql.execution.pandas.convertToArrowArraySafely=true`, and a `@pandas_udf` with the same value fails with `PySparkValueError`. The PySpark migration guide (4.1) says the setting affects "arrow-enabled UDFs/pandas_udfs". Output: `code/ch20/output/arrow_batches.txt`.
4. **Python UDTF whose `terminate()` yields a row fails with `NullPointerException` when an input partition is empty**, in a lateral join and with a plain `TABLE` argument (`EvalPythonUDTFExec` joins the row to a null `row1`). Minimal repro: `spark.range(6).repartition(3)` (one partition empty in this run) then `df.lateralJoin(T(F.col("id").outer()))` where `T.terminate` yields `(-1,)`. SparkTutor MD30 recorded the same on 4.2. Output: `code/ch21/output/udtf_core.txt`.
5. **`LEFT JOIN LATERAL` with a Python UDTF and no `ON` clause drops the left rows for which the UDTF yields nothing** (0 rows with NULL key; with `ON true`, 3). SparkTutor MD30 cites SPARK-60564 for this; the JIRA number is not verified here. Output: `code/ch21/output/udtf_core.txt`.
6. **Classic PySpark cannot parse `timestamp_ntz(9)`**: with `spark.sql.timestampNanosTypes.enabled=true`, `df.schema` and `df.first()` fail with `CANNOT_PARSE_DATATYPE` (the JVM schema is `struct<t:timestamp_ntz(9)>`). The feature is an internal preview, so this may be expected. Output: `code/ch22/output/skipping.txt`.
7. **INT96 timestamp filters appear in `PushedFilters` but cannot skip** (no statistics). Known Parquet behaviour; the plan text does not show it. Output: `code/ch22/output/skipping.txt`.
8. Not a bug, an observation not explained: in `code/ch19/ui_tour.py` the June sort (2 partitions, AQE off, 1g heap) did not spill, while the same sort in `spill.py` and in a stand-alone test spilled 554 MiB on its first run. The tour sorts six months instead. Not investigated further.
9. `SparkEnv.get().memoryManager()` through Py4J failed on 4.3.0 (earlier writer's note in `code/ch19/memory.py`); the script reads the pools from the REST API instead.

## Nav entries

```yaml
  - "Part IV: Performance":
    - chapters/ch18-query-plans.md
    - chapters/ch19-partitions-caching-memory.md
    - chapters/ch20-python-udfs-arrow.md
    - chapters/ch21-python-udtfs.md
    - chapters/ch22-parquet-inside.md
    - chapters/ch23-project-feather.md
```
