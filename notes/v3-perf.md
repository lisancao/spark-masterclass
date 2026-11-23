# v3-perf notes: performance slice (ch86 new, ch33 and ch36 extended)

Branch `v3-perf`, worktree `~/spark-masterclass-parts/perf`. Ports: Spark UI 4154, Connect 15114 (the in-process Connect plugin of `ch33/ui_tabs.py`).

## Scope

Gap-audit rows covered: "Leveraging statistics / CBO", "Partitioning hints", "Storage-partitioned join", "Bucketing", "Spark tuning guide" (serialization, memory, GC, data structures, locality), "Web UI" (tab by tab), "ORC", Parquet columnar encryption and datetime rebase (items 4, 11, 13, 22, 23 of the recommendations), and the `lz4raw` line (removed in 4.0).

### New chapter

- `book/chapters/ch86-statistics-cbo-pruning.md`: "Statistics, the cost-based optimizer and partition pruning". Provisional number 86. Goes in Part VII (Performance) directly after Chapter 32 (query plans). Code `code/ch86/`, figures `book/media/figures/ch86-*`, animation `book/media/clips/ch86-anim-dpp.*`, film tape `code/film/ch86.tape`.
  - Steps: 1 what the optimizer knows without statistics, 2 ANALYZE TABLE (all forms), 3 how statistics reach the plan (`explain("cost")`, CBO off/on, histograms, estimates against real rows), 4 the estimate selects the join strategy, 5 join reorder (and a histogram case where CBO keeps the written order), 6 dynamic partition pruning (plan, measured, the selective-predicate rule, broadcast reuse, static pruning, new in 4.3 materialized filtering side), 7 bucketed tables (layout, joins, coalesce, aggregates, pruning and `autoBucketedScan`), 8 storage-partitioned joins on Iceberg, 9 join and partitioning hints, whole chapter.

### Changes to existing chapters

- **ch33** (`ch33-partitions-caching-memory.md`): Step 9 renamed "The Spark UI, tab by tab" and extended with a subsection "The tabs, one by one" (Jobs, Stages, Storage, Environment, Executors, SQL / DataFrame, Structured Streaming, Connect; one screenshot each, a table of tabs, URLs and REST endpoints). The old two clips and their text stay. The last paragraph of the old Step 9 ("The other tabs complete the picture...") was replaced by the new subsection. New Steps 10 (serialization: Java and Kryo, registration), 11 (object size with `SizeEstimator`, GC and memory tuning, GC logs), 12 (data locality and `spark.locality.wait`, measured on `local-cluster`), 13 (hardware provisioning from the docs). The intro paragraph, "The whole chapter, on one page" and "Further reading" were extended. Steps 1 to 8 are unchanged.
- **ch36** (`ch36-parquet-inside.md`): title changed to **"Parquet and ORC from the inside"**; opening paragraph and thesis mention ORC, encryption and calendars. New Steps 9 (ORC file structure with a real dump), 10 (native and hive ORC readers), 11 (Parquet against ORC: pushdown and vectorized reads on the taxi data), 12 (Parquet modular encryption), 13 (datetime rebase modes). The old Step 9 "Rules for a Parquet layout" is now Step 14 "Rules for a Parquet or ORC layout", with three more rules. One line added in Step 7 about `lz4raw`. No other chapter refers to "Chapter 36, Step 9" (checked with grep).

## Nav and roadmap rows for the integrator

`mkdocs.yml` (Part VII):

```yaml
      - "32. Reading query plans: Catalyst and AQE": chapters/ch32-query-plans.md
      - "86. Statistics, the cost-based optimizer and partition pruning": chapters/ch86-statistics-cbo-pruning.md   # new, after ch32
      - "33. Partitions, caching, memory and spill": chapters/ch33-partitions-caching-memory.md
      ...
      - "36. Parquet and ORC from the inside": chapters/ch36-parquet-inside.md     # title change
```

`book/roadmap.md`, Part VII:

| # | Chapter | Covers |
|---|---|---|
| 86 (after 32) | Statistics, the cost-based optimizer and partition pruning | ANALYZE TABLE and histograms, `explain("cost")`, CBO join selection and join reorder, dynamic partition pruning measured, bucketed tables, storage-partitioned joins on Iceberg, join and partitioning hints. **4.3:** DPP for a materialized filtering side |
| 33 | Partitions, caching, memory and spill | add: the Spark UI tab by tab with screenshots, Java and Kryo serialization, object size and GC, `spark.locality.wait` measured, hardware provisioning |
| 36 | Parquet and ORC from the inside | add: ORC stripes, row index and postscript from a real file, native and hive readers, Parquet against ORC on the taxi data, modular encryption, datetime rebase modes |

`book/coverage.md`: no SparkTutor lesson maps to the new topics beyond what ch32/ch33/ch36 already list.

## Technical names (TN) and technical verbs (TV)

- TN, optimizer: optimizer, cost-based optimizer (CBO), estimate, statistics (table statistics, column statistics), row count, distinct count, histogram, bin, equi-height histogram, selectivity, cost, join order, join reorder, star schema, fact table, dimension table, broadcast, broadcast threshold, build side, partition filter, dynamic partition pruning (DPP), static pruning, subquery, bucket, bucketed table, bucket pruning, storage-partitioned join (SPJ), partition transform, reducer, hint, join hint, partitioning hint, advisory size, plan, logical plan, physical plan, exchange, shuffle.
- TN, ORC and Parquet: stripe, stream, row index, row group, postscript, footer, metadata section, stripe footer, protobuf, varint, magic bytes, codec, dictionary, vectorized reader, SearchArgument, SerDe, modular encryption, envelope encryption, data key, master key, key management service (KMS), KMS client, plaintext footer, encrypted footer, calendar (hybrid, proleptic Gregorian), rebase, rebase mode, day number.
- TN, tuning: serializer, serialization, registration, object header, boxed value, primitive array, garbage collector (GC), GC log, pause, young collection, full collection, young generation, old generation, region, task time, locality level, delay scheduling, locality wait, timer, executor, local-cluster mode, hardware provisioning, local disk, mount point.
- TN, UI: tab, Jobs tab, Stages tab, Storage tab, Environment tab, Executors tab, SQL / DataFrame tab, Structured Streaming tab, Connect tab, event timeline, summary metrics, screenshot.
- Code identifiers in code format: config keys, class names (`KryoSerializer`, `SizeEstimator`, `SizeInBytesOnlyStatsPlanVisitor`, `FilterEstimation`, `CostBasedJoinReorder`, `PartitionPruning`, `PlanAdaptiveDynamicPruningFilters`, `UnsafeRowSerializer`, `BucketFunction`, `ReducibleFunction`, `InMemoryKMS`, `PropertiesDrivenCryptoFactory`, `OrcFileFormat`), plan nodes, error conditions, file names.
- TV: analyze, estimate, reorder, prune, broadcast, shuffle, bucket (only as `bucketBy`), hash, partition, coalesce, rebalance, serialize, deserialize, register, cache, spill, encrypt, decrypt, sign, rebase, decode, vectorize (not used as a verb in the end), schedule, launch, collect (GC).

## STE check

`scripts/ste-check <file> description --allow <words>` (prepared text, shared allowlist plus the words below). Final counts:

| File | Findings | Chapter allowlist |
|---|---|---|
| `ch86-statistics-cbo-pruning.md` | 0 | setting settings caching planning scheduling partitioning bucketing pruning cleaning timing |
| `ch33-partitions-caching-memory.md` | 0 | setting settings caching planning scheduling provisioning logging repartitioning packing timing cleaning skipping |
| `ch36-parquet-inside.md` | 0 | setting settings caching repartitioning packing timing cleaning skipping |

The ch33 and ch36 lists keep the words that the Part IV writer allowlisted (`notes/part04.md`), plus the `-ing` technical names of the new steps (`scheduling` in "delay scheduling", `provisioning` in "hardware provisioning", `logging` in "unified logging", `pruning` in "partition pruning", `bucketing` in config names).

Rules broken on purpose:

- Parentheses hold config keys, defaults, JIRA numbers and short labels, as in Part I.
- The thesis of ch86 is five sentences in bold, each 25 words or fewer.
- Some sentences start with a code identifier (`gc.sh` caches ...), as elsewhere in the book.
- Note about the checker: a sentence that starts with a markdown link (`[Chapter 20](...) uses ...`) is merged with the sentence before it by `ste_prep.py`, so the checker reports a long sentence. I rewrote those sentences so that they do not start with a link.

## Claim-to-source

### ch86

| Claim | Source |
|---|---|
| Sizes, rows, column stats before/after ANALYZE; "0 rows" before ANALYZE in the Hive catalog without a plan rowCount | `code/ch86/output/stats.txt` sections 1, 2, 5 |
| CBO off/on estimates (30.6 MiB; 169.8 MiB, 3.71E+6; filter 2.74E+6) | `stats.txt` section 3 |
| Estimated vs real rows, histogram bins (254, height 14,602.9) | `stats.txt` section 4 |
| Without CBO, Filter/Project keep the child size scaled by row width; aggregates without keys and limits give smaller estimates | `SizeInBytesOnlyStatsPlanVisitor.scala` v4.3.0 (`visitUnaryNode`, `visitAggregate`, `visitGlobalLimit`) |
| `spark.sql.cbo.planStats.enabled` gives relations rowCount/column stats with CBO off | `catalog/interface.scala` line 1212, `LogicalRelation.scala` line 58, v4.3.0 |
| CBO and statistics config defaults | `SQLConf.scala` v4.3.0 lines 4599-4705 |
| ndv error 0.05 (HyperLogLog++) | `SQLConf.scala` `spark.sql.statistics.ndv.maxError` |
| CBO picks BHJ for the 82-row side before any shuffle; with CBO off, AQE converts to BHJ only after both shuffles ran | `cbo_join.txt` section 1 (final plan read from the executed DataFrame) |
| Join reorder plan and rows (3,709,141 and 210 vs 210 and 210), times | `cbo_join.txt` section 2 |
| Histogram join estimate 7.63E+6 rows, CBO keeps written order; real 210 | `cbo_join.txt` section 3 |
| DPP plan, 26/181 partitions, rows, task time, wall time | `dpp.txt` sections 1, 2 |
| Broadcast off: filter planned but replaced by `true` at run time when there is no broadcast to reuse | `dpp.txt` section 2; `PlanAdaptiveDynamicPruningFilters.scala` v4.3.0 (`case None if onlyInBroadcast => DynamicPruningExpression(Literal.TrueLiteral)`); `reuseBroadcastOnly` default true in `SQLConf.scala` |
| `IS NOT NULL` is not "likely selective"; the list of selective predicates | `predicates.scala` v4.3.0 `isLikelySelective`; `PartitionPruning.scala` `hasPartitionPruningFilter`, `pruningHasBenefit` |
| `holiday <> ''` gives DPP, 5 partitions | `dpp.txt` section 3 |
| fallbackFilterRatio 0.5, useStats | `SQLConf.scala` lines 799-820 |
| New in 4.3: DPP for LocalRelation / checkpointed side (SPARK-54593) | commits 6a75953e151, fb74f7c0e57, 0c69b35d69d, 402cdba79b1 in `git log v4.2.0..v4.3.0`; `dpp.txt` section 5. Not run on 4.2 |
| Bucket file names, 272 vs 16 files, DESC EXTENDED bucket spec | `bucketing.txt` section 1 |
| Bucketed joins, exchanges and times; coalesce buckets; maxBucketRatio 4 | `bucketing.txt` section 2; `SQLConf.scala` line 7201 |
| autoBucketedScan disables the bucketed scan; pruning selects 1 of 16 but 17 tasks; files metric counts all files | `bucketing.txt` section 4; `DataSourceScanExec.scala` v4.3.0 lines 345, 621-627, 855-870 |
| SPJ: two Exchanges by default; none with preserve-data-grouping; 16 x 8 with allowCompatibleTransforms | `spj.txt` |
| Iceberg `preserve-data-grouping` default false; `BucketFunction` reducer to the gcd | `~/repos/iceberg-spark43` (PR 18749 build) `SparkSQLProperties.java` line 39-41, `functions/BucketFunction.java` lines 142-153 |
| SPJ settings and defaults; `allowKeysSubsetOfPartitionKeys` with alternative `allowJoinKeysSubsetOfPartitionKeys` | `SQLConf.scala` v4.3.0 lines 2440-2530; `docs/sql-performance-tuning.md` v4.3.0 (Storage Partition Join) |
| Join hint results, build sides, precedence, ignored hints and warnings | `hints.txt` sections 1-4 (warnings from stderr, appended to the file) |
| Partitioning hint exchanges, file counts and sizes; parallelismFirst behaviour | `hints.txt` sections 5, 6 |

### ch33

| Claim | Source |
|---|---|
| Kryo 4 (`kryo-shaded-4.0.3.jar`) | jars in pyspark 4.3.0 |
| Cache/shuffle sizes for each serializer; DataFrame shuffle 149.4 KiB unchanged; registrationRequired error | `code/ch33/output/serialization.txt` (medians of 3 after a warmup, run under `flock` on the bench lock) |
| Kryo auto-pick for primitive types and strings in RDD shuffles | `SerializerManager.scala` v4.3.0 `canUseKryo` |
| Kryo setting defaults | `core/.../internal/config/Kryo.scala` v4.3.0 |
| Object sizes | `sizes.txt` (`SizeEstimator.estimate`) |
| GC: cache sizes, GC time, pauses, full collections; GC log lines | `gc.txt` (single runs of 5 passes, under the bench lock; a first run, not saved, showed the same cache sizes and full-GC counts) |
| `-XX:+PrintGCTimeStamps` fails on Java 17 | `gc.txt` last block |
| GC tuning advice (-Xmn 4/3, NewRatio, G1HeapRegionSize, G1 default since 4.0) | `docs/tuning.md` v4.3.0 |
| Locality levels; delay scheduling launch times and job times | `locality.txt` (job 2 single runs under the bench lock); `docs/tuning.md` v4.3.0 Data Locality |
| `spark.locality.wait` 3s, `.process/.node/.rack` fall back to it, `legacyResetOnTaskLaunch` false since 3.1.0 | `core/.../internal/config/package.scala` v4.3.0 lines 827-839, 2383-2393 |
| Hardware recommendations | `docs/hardware-provisioning.md` v4.3.0 (not measured) |
| `spark.local.dir` default `/tmp`, overridden by SPARK_LOCAL_DIRS / LOCAL_DIRS | `docs/configuration.md` v4.3.0 line 393 |
| UI tab contents, REST numbers | screenshots `book/media/figures/ch33-ui-*.png`, `ui_tabs.txt` |
| Environment page marks non-default values (Default Value column) | screenshot; `environmentpage.js` v4.3.0 line 178 (also present in v4.2.0, so not new in 4.3) |

### ch36

| Claim | Source |
|---|---|
| ORC file bytes, postscript fields, layout sum to 3,328 bytes | `code/ch36/output/orc_dump.txt` section 1 |
| ORC 2.3.0 in Spark 4.3.0, Parquet 1.17.0 | `pom.xml` v4.3.0 lines 148-149; jars |
| Stripes of the June file, row index entries of 10,000 rows | `orc_dump.txt` section 2 |
| native/hive reader classes, Batched, PushedFilters | `orc_dump.txt` section 3 |
| ORC default codec zstd since 4.0 | `docs/sql-migration-guide.md` v4.3.0 |
| `convertMetastoreOrc` uses the native reader for Hive ORC tables | `docs/sql-data-sources-orc.md` v4.3.0 |
| Parquet vs ORC rows from the scan and times | `orc_compare.txt` (medians of 5 after a warmup, bench lock) |
| ORC settings defaults | `SQLConf.scala` v4.3.0 lines 2068-2126 |
| Encryption magic `PARE`, sizes, reads with and without keys, PyArrow errors | `crypto_rebase.txt` sections 1-3 |
| Docs link the 1.17.1 test jar; Spark 4.3.0 ships 1.17.0 | `docs/sql-data-sources-parquet.md` v4.3.0; `pom.xml` |
| Rebase defaults CORRECTED; the docs table says EXCEPTION | `crypto_rebase.txt` section 4; `SQLConf.scala` lines 7095-7151; `docs/sql-data-sources-parquet.md` lines 632-660 |
| Spark ignores the read mode for its own files (version key, legacyDateTime key) | `crypto_rebase.txt`; `DataSourceUtils.getRebaseSpec` v4.3.0 lines 200-221 |
| `spark.sql.legacy.*` rebase names removed in 4.0; `lz4raw` removed in 4.0 | `docs/sql-migration-guide.md` v4.3.0 |

## Environment

- pyspark 4.3.0 (uv project in `code/`), Java 17.0.21.1, Scala 2.13.18 (spark-shell from the pyspark package), 16 cores, Linux.
- ch86 tables: Hive metastore in Derby at `data/work/ch86-warehouse/metastore_db` (gitignored). Embedded Derby allows one JVM at a time, so the ch86 scripts run one after the other.
- Iceberg runtime: `$BOOK_DATA/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar` (apache/iceberg PR #18749, commit `b1eab64`; not a release), sha256 `f915c7b075e58012f6a9ee2e34dffcccac9a5715816e01738ff5560efe6dc4d5`. Same jar as Chapter 45.
- orc-tools: `org.apache.orc:orc-tools:2.3.0:uber` from Maven Central, at `$BOOK_DATA/jars/dl-orc/orc-tools-2.3.0-uber.jar`, sha256 `6ea7e5d0ad410efc8b69f7d28dc6eaba8403185c709053fff24c6fa884c63a4a`.
- Parquet mock KMS: `org.apache.parquet:parquet-hadoop:1.17.0:tests` from Maven Central, at `$BOOK_DATA/jars/dl-parquet/parquet-hadoop-1.17.0-tests.jar`, sha256 `a5477c09f0d1346efbd1cff03250d7a822fe0ff7abf8c9201a456904c433bf1e`.
- `code/ch33/trip.jar`: one Java class (`book.Trip`, source in `code/ch33/java/`), compiled with javac 17 for the Scala jobs. Small and checked in.
- Playwright (system Python) for the UI screenshots; pngquant to shrink them.
- Timings: medians after a warm-up under the bench lock, but other writers ran unlocked work on the same machine. Run-to-run differences of 0.1 to 0.3 s were common, and some earlier runs of the same scripts gave other times. The text says where a difference is within the noise, and quotes task time (sum of executor run times) for DPP because it is less sensitive to the load.

## Open findings (not filed; not called bugs in the book)

1. **Doc gap: Parquet rebase defaults.** `docs/sql-data-sources-parquet.md` (v4.3.0) lists `EXCEPTION` as the default of `spark.sql.parquet.datetimeRebaseModeInRead/InWrite` and `int96RebaseModeInRead/InWrite`. `SQLConf.scala` v4.3.0 and `spark.conf.get` say `CORRECTED`. Repro: `spark.conf.get("spark.sql.parquet.datetimeRebaseModeInRead")` on 4.3.0.
2. **Doc gap: GC flags.** `docs/tuning.md` (v4.3.0) recommends `-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps`. Java 17 (the minimum for Spark 4.x) rejects `PrintGCTimeStamps`: `java -XX:+PrintGCTimeStamps -version` gives "Unrecognized VM option". An executor with these options does not start. The guide could say `-Xlog:gc*`.
3. **Doc gap: SPJ setting name.** The 4.3.0 tuning guide names `spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled`. In `SQLConf.scala` v4.3.0 the main key is `allowKeysSubsetOfPartitionKeys.enabled`, with the old name as an alternative.
4. **Doc gap: Parquet test jar version.** The columnar encryption section links the 1.17.1 `parquet-hadoop` test jar, but Spark 4.3.0 ships Parquet 1.17.0.
5. **Log message typo.** `HintErrorLogger` prints "Count not find relation 'x' specified in hint 'BROADCAST(x)'" ("Count" for "Could"). Repro: `SELECT /*+ BROADCAST(x) */ ... FROM trips t JOIN zones z ...` with log level WARN (`hints.txt`).
6. **Estimate question (not a bug claim): join estimate with histograms after a filter on another column.** With histograms on `trips.PULocationID` and `zones.LocationID`, the estimate for `trips JOIN zones WHERE zones.Borough = 'Staten Island'` (38 zones) is 7.63E+6 rows, more than either input (3.71E+6 trips), real 210. Without histograms the estimate is 5.42E+5 and CBO reorders. Repro: `cbo_join.py` section 3. Worth a look at `JoinEstimation.computeByHistogram` and how `FilterEstimation` updates (or does not update) the histogram of `LocationID`.
7. **Metric note: `number of files read` with bucket pruning** counts all files of the selected partitions, not the files that remain after bucket pruning (`DataSourceScanExec` sets it from `selectedPartitions`). A user who checks pruning with this metric sees no change. Maybe a doc note.

## Not verified

- DPP for a `LocalRelation` was not run on Spark 4.2; "new in 4.3" rests on the commit history.
- The Iceberg side of SPJ uses a pre-release Iceberg build.
- Hardware provisioning numbers are from the docs, not measured.
- The Connect tab has no REST endpoint in the core monitoring API; the text points to the Connect REST endpoints of Chapter 20 (another writer's chapter) without a new check.

## Clips and media

Film clips (VHS, `film render <tape> --format chapter --demo code`, cut list `book/media/clips/clips-v3-perf.tsv`, cut with `code/common/cut_clips.sh`; renders were moved out after each cut because the three tapes share `code/film/renders/chapter/`):

| Clip | Tape | Shows | VO draft |
|---|---|---|---|
| `ch86-estimates` | `code/film/ch86.tape` | `stats.py`: CBO off/on, estimated against real rows | Without a histogram, the estimates miss by orders of magnitude. With one, they land close. |
| `ch86-dpp` | `ch86.tape` | `dpp.py` with `BOOK_RUNS=1 BOOK_NO_LOCK=1` | Twenty-six of 181 partitions with DPP. All 181 without it, or without a broadcast. |
| `ch86-hints` | `ch86.tape` | `hints.py` section 5 | Each partitioning hint, its exchange, and the files that the write made. |
| `ch33-locality` | `code/film/ch33.tape` | `locality.py`, first two settings | The idle executor waits for the locality timer, or takes a task at once. |
| `ch36-orc` | `code/film/ch36-orc.tape` | `orc.py dump` | The last byte, the postscript, the layout of an ORC file. |
| `ch36-crypto` | `ch36-orc.tape` | `crypto_rebase.py crypto` | PARE at both ends; without the keys, only the plaintext columns of a plaintext footer can be read. |

The film runs had no bench lock. Their on-screen times differ from the saved outputs, and the captions say so.

Animations (HyperFrames, paper theme, 1280x720, `code/common/build_anim.sh`): `ch86-anim-dpp` (storyboard `code/ch86/anim/dpp/`), `ch33-anim-locality` (storyboard `code/ch33/anim/locality/`). Numbers from `dpp.txt` and `locality.txt`; layouts labelled schematic.

Figures (SVG light/dark from `code/ch86/figures.py`, `code/ch33/figures_tuning.py`, `code/ch36/figures_orc.py`, each rendered to PNG with headless Chrome and checked): `ch86-estimates`, `ch86-dpp`, `ch86-joins`, `ch33-serialization`, `ch33-locality`, `ch36-orc-bytemap`, `ch36-orc-compare`. Screenshots (PNG, pngquant): `ch33-ui-{jobs,stages,stage-detail,storage,environment,executors,sql,sql-detail,streaming,streaming-detail,connect}.png` from `code/film/ch33_ui_tabs.py`; `ch33-ui-stages.png`, `ch33-ui-sql.png` and `ch33-ui-streaming.png` are made but not embedded.

## Ports

Final runs of the ch86 scripts, `ui_tabs.py` and `orc.py compare` used 4154 and 15114. Before I noticed that other writers use the neighbouring ports, some runs used 4155 to 4159: `serialization.sh` (4157), `gc.sh` and `sizes.scala` (4158), `locality.py` (4156), `crypto_rebase.py` and `orc.py dump` (4157). Spark moves to the next free port if a port is taken, and these scripts read no REST data except `locality.py`, which reads its own application. One run of `cbo_join.py` read the REST API of the film's application on 4154 and failed; it was run again.
