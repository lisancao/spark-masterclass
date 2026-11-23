# v3 APIs slice: notes

Worktree `~/spark-masterclass-parts/apis`, branch `v3-apis`. `SPARK_UI_PORT=4157` for Classic runs. Connect server on gRPC 15117 with its UI off (`code/ch90/connect_server.sh`), so the Classic scripts that read the REST API keep port 4157. The scripts that read the REST API take the port from `sc.uiWebUrl`, so a port that Spark moved does not send them to another application.

## Scope

Gap audit rows: RDD programming guide (key-value operations, closures, broadcast variables, persistence), Dataset API, DataFrame `stat`/`describe`/`summary`, `DataFrame.plot`, pandas API on Spark, error conditions and SQLSTATE.

New chapters (provisional numbers, all for Part III after ch11):

| File | Title | Thesis in one line |
|---|---|---|
| `book/chapters/ch90-rdds-today.md` | RDDs today | The RDD is the low-level API under the DataFrame. Know what each call costs (shuffle, closure, storage). Connect has no RDDs, so each feature has a DataFrame equivalent that works over Connect. |
| `book/chapters/ch91-pandas-api-on-spark.md` | pandas API on Spark | A pandas-on-Spark DataFrame is a Spark DataFrame, an index column and a column map. The index is the cost. Measured against pandas and PySpark, and run over Connect. |
| `book/chapters/ch92-datasets-encoders-scala.md` | Datasets and Encoders in Scala | A typed Dataset is a DataFrame with an Encoder. Typed operations add object conversion and hide their lambdas from the optimizer. Measured, with an Aggregator, and over the Scala Connect REPL. |

Changes to existing chapters:

- `ch04-one-engine-batch-and-streaming.md`: the module table row for pandas API on Spark now links to Chapter 91 (was "not covered in this edition"). Nothing else changed.
- `ch09-dataframes-columns-schemas.md`: new Step 19 (describe, summary, `df.stat`: corr, cov, crosstab, freqItems, approxQuantile with a measured error/cost table, sampleBy) and new Step 20 (`DataFrame.plot`: the three groups of plot kinds from `pyspark/sql/plot/core.py`, what reaches the driver, plotly only). Two bullets added to "The whole chapter, on one page". Steps 1 to 18 keep their numbers. New scripts `code/ch09/stats.py`, `code/ch09/plot.py`; outputs `stats.txt`, `plot.txt`, `plot-connect.txt`; figures `ch09-plot-bar.svg`, `ch09-plot-scatter.svg` (plotly SVG, transparent background, mid-tone text, one file for both themes).
- `ch10-cleaning-values.md`: new Step 17 "Catch errors by condition and SQLSTATE" (after Step 16): what `PySparkException` carries (condition, SQLSTATE, parameters, query context), errors with no condition, `getErrorClass` deprecated in 4.0.0, the same `pyspark.errors` classes on Classic and Connect, a handler by condition then SQLSTATE class, where the error appears on each side, and the error-conditions catalog read from the pip package. One bullet added to the closing summary. New script `code/ch10/errors.py`, outputs `errors.txt`, `errors-connect.txt`, figure `ch10-sqlstate` from `code/ch10/figures/errors_figure.py`.

Overlap decisions: ch18 already has the Classic-to-Connect replacement table (ch90 links it). ch33 measures DataFrame storage levels (ch90 shows only the Python RDD levels). ch56 has DataFrame checkpoints (ch90 shows RDD checkpoints and links it). ch59 has accumulator retry semantics (ch90 shows the two-actions case and links it). ch03 has the RDD-vs-DataFrame speed measurement (ch90 does not repeat it).

## Technical names and verbs (STE)

- TN: RDD, pair RDD, partitioner, partition function, closure, broadcast variable, accumulator, storage level, checkpoint, local checkpoint, lineage, shuffle write, map task, reduce task, pickled batch, `PipelinedRDD`, `ShuffledRDD`, `PartitionerAwareUnionRDD`, `ReliableCheckpointRDD`, `LocalCheckpointRDD`, internal frame, default index, index column, column map, natural order, option, type hint, Encoder, case class, typed Dataset, typed operation, untyped operation, Aggregator, UDAF, buffer, condition, subcondition, SQLSTATE, SQLSTATE class, query context, error-conditions catalog, contingency table, stratified sample, top-n plot, sampled plot, plotly, kaleido, Connect REPL, operating system, page cache.
- TV: as Part I, plus pickle, unpickle, partition, broadcast, persist, checkpoint, collect, encode, deserialize, serialize, prune, register.

## STE results

`scripts/ste-check <file> --allow programming Programming operating plotting` for the new chapters. The edited chapters use the Part III allow list (`notes/.ste-allow-part03`), as before.

| File | Findings | Note |
|---|---|---|
| ch90 | 0 | |
| ch91 | 0 | |
| ch92 | 0 | |
| ch09 (edited) | 3 | the same 3 as before the edit (`Learning` in a book title, `docstring` two times). The new steps add 0. |
| ch10 (edited) | 21 | the same 21 as before the edit (all in older steps: `cleaning`, `encoding`, `trailing`, `spelling`, ...). The new step adds 0. |
| ch04 (edited) | not changed | one table cell |

Allow words: `programming`/`Programming` (guide title "RDD Programming Guide"), `operating` (TN "operating system"), `plotting` (option and module names `plotting.backend`, `spark.sql.pyspark.plotting.max_rows`).

Deliberate rule breaks: figure alt texts are two or three words, because the lint joins the light and dark alt texts with the caption. Output blocks and plan text are pasted as they are. Three long plan lines in ch92 Step 3 are shortened with `...`, and the text says so.

## Environment

- pyspark 4.3.0 (the `code/` uv project), pandas 2.3.3, PyArrow 26.0.1, OpenJDK 17.0.21.1, Python 3.12, Scala 2.13.18 (the `spark-shell` of the pip package).
- plotly 6.5.0 and kaleido 1.2.0 through `uv run --with` (not added to `pyproject.toml`). Only `ch09/plot.py` and `ch91/api.py` need them; kaleido only writes the SVG figures.
- Scala Classic: `uv run spark-shell --master 'local[*]' --conf spark.ui.port=4157 -I ch92/Datasets.scala`. `-I` (pasted preload), not `-i`; see open finding 3.
- Scala Connect: `spark-shell --remote sc://localhost:15117` (the Ammonite-based Connect REPL in the pip package, `jars/connect-repl/`), driven by `code/ch92/connect_repl.sh` under `script(1)` because the REPL needs a terminal.
- Connect server: `code/ch90/connect_server.sh start|stop` (pip package `sbin/start-connect-server.sh`, gRPC 15117, `spark.ui.enabled=false`, session time zone UTC).
- Data: `BOOK_DATA=$HOME/spark-masterclass/data`, June 2026 yellow taxi month (3,837,248 rows), zone lookup.
- Timed runs under `flock ~/spark-masterclass/data/.bench.lock`: `ch90/keyvalue.py`, `ch90/persist.py`, `ch90/shared.py`, `ch91/internals.py`, `ch91/compare.py`, `ch91/api.py` (Classic), `ch09/stats.py`, `ch92/Datasets.scala`. Other writers ran Spark and films on the same machine without the lock, so the spread is wide. The persist scan times are the noisiest, and the text says so.
- Films: `film render code/film/ch9X.tape --format chapter` (VHS), cut with `code/common/cut_clips.sh book/media/clips/clips-v3-apis.tsv`. Films ran without the lock; captions say so.
- Animations: HyperFrames through `code/common/build_anim.sh` (paper theme, landscape, scaled to 1280x720). Sources in `code/ch9X/anim/<name>/` with `storyboard.md`.
- Figures: `code/ch90/figures.py`, `code/ch91/figures.py`, `code/ch92/figures.py`, `code/ch10/figures/errors_figure.py` (svgfig, light and dark files). Plotly figures (`ch09-plot-bar`, `ch09-plot-scatter`, `ch91-fare-hist`) are single SVG files with a transparent background and mid-tone text. Each figure was rendered to PNG with headless Chrome and checked on white and dark backgrounds.

## Media

| File | What | Source |
|---|---|---|
| `book/media/clips/ch90-rdds.mp4` | film: lazy chain, three pair operations, RDD entry points over Connect | `code/film/ch90.tape` |
| `book/media/clips/ch90-anim-combine.mp4` | animation: map-side combine against groupByKey | `code/ch90/anim/combine` |
| `book/media/clips/ch91-pandas.mp4` | film: layers and index plans, five operations, api.py over Connect | `code/film/ch91.tape` |
| `book/media/clips/ch91-anim-index.mp4` | animation: the three default index types | `code/ch91/anim/index` |
| `book/media/clips/ch92-datasets.mp4` | film: Encoder schema, as[T] checks, typed and untyped plans and times | `code/film/ch92.tape` |
| `book/media/clips/ch92-anim-roundtrip.mp4` | animation: row to Trip object and back, against expressions on the binary row | `code/ch92/anim/roundtrip` |
| `book/media/figures/ch90-keyvalue-*.svg`, `ch90-partitions-*.svg` | shuffle write and time of three pair operations; rows per hash partition | `code/ch90/figures.py` |
| `book/media/figures/ch91-index-cost-*.svg`, `ch91-compare-*.svg`, `ch91-fare-hist.svg` | index cost; five operations in three APIs; `psdf.plot.hist` | `code/ch91/figures.py`, `code/ch91/api.py` |
| `book/media/figures/ch92-plans-*.svg`, `ch92-times-*.svg` | typed and untyped physical plans; typed and untyped times | `code/ch92/figures.py` |
| `book/media/figures/ch09-plot-bar.svg`, `ch09-plot-scatter.svg` | `DataFrame.plot.bar` and `plot.scatter` | `code/ch09/plot.py` |
| `book/media/figures/ch10-sqlstate-*.svg` | conditions, SQLSTATE codes and classes | `code/ch10/figures/errors_figure.py` |

Clip cut list: `book/media/clips/clips-v3-apis.tsv`.

## Claim-to-source table

| Claim | Source |
|---|---|
| reduceByKey 0.59 s / 0.02 MiB, aggregateByKey 0.75 s, groupByKey 1.01 s / 11.76 MiB, 128 JVM records each | `code/ch90/output/keyvalue.txt` (locked) |
| PySpark shuffles pickled batches, groupByKey combines into lists before the shuffle | `python/pyspark/core/rdd.py` (`partitionBy`, `groupByKey`) at v4.3.0 |
| guide rule "will yield much better performance" | `docs/rdd-programming-guide.md` line 1002 at v4.3.0 |
| `lookup` fails with `PYTHON_HASH_SEED_NOT_SET`; executors get the seed or `0` | `basics.txt`; `portable_hash` in `rdd.py`; `context.py:296` |
| portable_hash(int) = int, partitions 4/2/1/5, rows per partition | `code/ch90/output/partitions.txt` |
| co-partitioned join adds no ShuffledRDD (PartitionerAwareUnionRDD) | `partitions.txt` section 4 |
| closure sizes 6,127 and 20,678,098 bytes; threshold 1,048,576; 0.74 vs 0.64 s per job | `code/ch90/output/shared.txt` (locked); `spark.broadcast.UDFCompressionThreshold` in `core/.../config/package.scala:2439`; `_prepare_for_python_RDD` in `rdd.py` |
| `CONTEXT_ONLY_VALID_ON_DRIVER`, `INTERNAL_ERROR_BROADCAST` after destroy | `shared.txt` |
| accumulator counts 16,815 bad fares; map + two actions counts 7,674,496 | `shared.txt`; guide lines 1540-1542 |
| Observation gives the same 16,815 and total | `shared.txt` |
| Python RDD always pickled; levels table; cache() is MEMORY_ONLY; 29.8 MiB at each level | `persist.txt` (locked); guide line 1257 |
| checkpoint lineage 11 lines to ReliableCheckpointRDD; localCheckpoint needs cache() first in PySpark | `persist.txt`; `rdd.py` `checkpoint()` sets `is_checkpointed`, `localCheckpoint()` does not; `_is_pipelinable` |
| Connect: `JVM_ATTRIBUTE_NOT_SUPPORTED`, `NOT_IMPLEMENTED`, equivalents work, `DataFrame.zipWithIndex` works | `code/ch90/output/connect.txt`; `zipWithIndex` versionadded 4.2.0 in `python/pyspark/sql/dataframe.py` |
| internal frame has 21 + 2 columns, index `__index_level_0__`, `__natural_order__` | `code/ch91/output/internals.txt` |
| plans of sequence / distributed-sequence / distributed | `internals.txt` section 2 |
| AttachDistributedSequence persists at `compute.default_index_cache` then `zipWithIndex` | `sql/core/.../python/AttachDistributedSequenceExec.scala` at v4.3.0 |
| `distributed_id` is `MonotonicallyIncreasingID` | `FunctionRegistry.scala:1092` |
| index cost 1.57 / 0.96 / 0.15 / 0.11 / 0.08 s, head(5) 0.68 s | `internals.txt` (locked) |
| five operations in three APIs; pandas 102.5 MiB | `code/ch91/output/compare.txt` (locked) |
| options and defaults; ops_on_diff_frames True since 4.0; ansi_mode_support since 4.1 | `api.txt`; `python/pyspark/pandas/config.py` at v3.5.0, v4.0.0, v4.1.0, v4.3.0; `python/docs/source/migration_guide/pyspark_upgrade.rst` lines 64, 119 |
| removals of append, iteritems, Int64Index in 4.0 | `api.txt`; `pyspark_upgrade.rst` lines 73-77 |
| schema inference on `shortcut_limit + 1` rows | `api.txt`; `python/pyspark/pandas/accessors.py` lines 361-362 |
| JFK RatecodeID 2: 92,624 trips, mode 70.0 | `api.txt` |
| pandas API on Spark over Connect gives the same results | `code/ch91/output/api-connect.txt` |
| Encoder schemas, kryo binary | `code/ch92/output/datasets.txt` section 1 |
| as[T] keeps 21 columns, `UNRESOLVED_COLUMN.WITH_SUGGESTION`, `NOT_NULL_ASSERT_VIOLATION` | `datasets.txt` section 2 |
| typed plan nodes, FileScan 6 columns, DataFilters [] | `datasets.txt` section 3 |
| typed 0.21 s, untyped 0.11 s (1.9x), select-first 0.16 s | `datasets.txt` section 5 (locked) |
| Aggregator plan (ObjectHashAggregate, AppendColumns); mean_of = avg | `datasets.txt` section 7 |
| mean_of 0.39 s, avg 0.10 s, groupByKey+TipShare 0.63 s, groupBy+avg(when) 0.12 s | `datasets.txt` (locked) |
| `UserDefinedAggregateFunction` deprecated in 3.0.0 | `sql/api/.../expressions/udaf.scala:36-38` |
| Connect REPL: `org.apache.spark.sql.connect.Dataset`, map as MapPartitions, Aggregator fails with ReplBridge$ | `code/ch92/output/connect.txt` |
| `spark-shell --remote` documented | `docs/spark-connect-overview.md:222` at v4.3.0 |
| describe/summary values, plan, corr/cov/crosstab/freqItems/approxQuantile/sampleBy | `code/ch09/output/stats.txt` (locked) |
| plot groups top-n / sampled / computed; max_rows 1000; plotly only | `code/ch09/output/plot.txt`, `plot-connect.txt`; `python/pyspark/sql/plot/core.py`; `SQLConf.scala:5113` |
| error conditions, SQLSTATE, params, contexts, Classic vs Connect | `code/ch10/output/errors.txt`, `errors-connect.txt` |
| 1347 conditions, 779 subconditions, 1262 SQLSTATE codes, 223 Python conditions | `errors.txt` section 4 |
| `getErrorClass` deprecated in 4.0.0 with FutureWarning | `python/pyspark/errors/exceptions/base.py` lines 76-90 |

## Open findings

1. **PySpark `RDD.localCheckpoint()` on an uncached `PipelinedRDD` does not take effect.** `checkpoint()` sets `self.is_checkpointed = True`, so `_is_pipelinable()` is false and the next action computes the marked JVM RDD. `localCheckpoint()` only calls `self._jrdd.rdd().localCheckpoint()`, so the next action pipelines past it and the mark is never materialized. Repro (Classic, 4.3.0):
   ```python
   r = sc.parallelize(range(100), 4).map(lambda x: (x % 3, 1)).reduceByKey(lambda a, b: a + b)
   r.localCheckpoint(); r.count(); print(r.isCheckpointed())   # False
   r2 = sc.parallelize(range(100), 4).map(lambda x: (x % 3, 1)).reduceByKey(lambda a, b: a + b)
   r2.cache().localCheckpoint(); r2.count(); print(r2.isCheckpointed())   # True
   ```
   Evidence: `code/ch90/output/persist.txt` section 4. Not checked on master or other releases.
2. **`RDD.toDebugString` prints the storage level of the top RDD on every line.** `debugSelf(rdd)` in `RDD.scala:2071` reads `storageLevel` (this RDD) instead of `rdd.storageLevel`. Visible in `persist.txt` sections 3-4 (an earlier run showed `[Disk Memory Deserialized 1x Replicated]` on every lineage line of a locally checkpointed RDD, including `FileScanRDD`). Repro (run on 4.3.0 Classic): `sc.parallelize(range(10)).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).persist(StorageLevel.DISK_ONLY).toDebugString()` prints `[Disk Serialized 1x Replicated]` on all six lines, including `ParallelCollectionRDD[0]`, which is not persisted.
3. **`spark-shell -i` with case classes fails to decode in 4.3.0 (Scala 2.13.18).** `case class P(id: Long, sq: Long); spark.range(5).map(i => P(i, i*i)).collect()` in a file loaded with `-i` fails with `EXPRESSION_DECODING_FAILED` caused by `NullPointerException: Cannot invoke "Object.getClass()" because "obj" is null`. The same lines typed into the shell, and the same file with `-I`, work. Possibly a REPL outer-scope issue, not checked on other releases.
4. **The Scala Connect REPL cannot run an `Aggregator` that the REPL defines.** `groupByKey(...).agg(new TipShareAgg().toColumn)` and `udaf(...)` fail on the server with `StatusRuntimeException: UNKNOWN: ammonite/repl/ReplBridge$`. Typed `filter`, `map` and `groupByKey(...).count()` work. A compiled Aggregator sent with `addArtifact` was not tested. Evidence: `code/ch92/output/connect.txt`.
5. **Some JVM errors still have no error condition.** `df.sample(fraction=2.0)` raises `IllegalArgumentException: requirement failed: ...` with `getCondition() is None` on Classic and Connect (`errors.txt`). Documentation note, not a bug.
6. **`RDD.lookup` needs `PYTHONHASHSEED` in the driver even for integer keys.** `portable_hash` checks the variable before it looks at the key type. By design (the seed must match the executors), but the error appears only when the driver hashes, so the docs could say it. Evidence: `basics.txt`.

## Not verified

- A compiled Scala Aggregator over Connect (open finding 4).
- Any cluster run: all measurements are `local[*]` on one 16-core machine, so the shuffle stays on a local disk.
- The pandas API on Spark plotting over Connect writes no image (the script writes the figure only on Classic), but the figure objects were built over Connect.

## Nav and roadmap rows for the integrator

`mkdocs.yml`, in "Part III: DataFrames" after ch11:

```yaml
      - "90. RDDs today": chapters/ch90-rdds-today.md
      - "91. pandas API on Spark": chapters/ch91-pandas-api-on-spark.md
      - "92. Datasets and Encoders in Scala": chapters/ch92-datasets-encoders-scala.md
```

`book/roadmap.md`, Part III table:

```markdown
| 90 | [RDDs today](chapters/ch90-rdds-today.md) | Transformations and actions, pair operations (reduceByKey against groupByKey measured), partitioners, closures, broadcast variables and accumulators and their DataFrame equivalents, persistence levels, checkpoints, no RDDs over Connect |
| 91 | [pandas API on Spark](chapters/ch91-pandas-api-on-spark.md) | The internal frame, the three default index types measured, pandas against pandas API on Spark against PySpark, options, interop, type hints, plotting, limits, Connect support |
| 92 | [Datasets and Encoders in Scala](chapters/ch92-datasets-encoders-scala.md) | Encoders, `as[T]`, typed against untyped plans and times, `Aggregator` as a typed UDAF and through `udaf()`, Datasets over the Scala Connect REPL |
```

Update the Part III row texts for ch09 ("**4.0+:** `DataFrame.plot`; describe, summary and `df.stat`") and ch10 ("catching errors by condition and SQLSTATE"). The Part III intro line "Columns and schemas, cleaning values, joins." could add "RDDs, pandas API on Spark and typed Datasets".
