# v2 Background part: notes

Worktree `~/spark-masterclass-parts/background`, branch `v2-background`. `SPARK_UI_PORT=4141` (4142 for side runs while a film held 4141), Connect port 15101 not used (no chapter here needs a Connect server).

## Scope

A new part, "Background", that opens the book. Four new chapters (provisional numbers, the integrator renumbers):

| File | Title | Thesis in one line |
|---|---|---|
| `book/chapters/ch01-mapreduce-to-spark.md` | From MapReduce to Spark | MapReduce kept data on disk between steps; the RDD keeps the working set in memory and recovers with lineage. Measured: one iterative job, MapReduce style against `cache()`. |
| `book/chapters/ch02-inside-a-spark-application.md` | Inside a Spark application | Driver services (DAGScheduler, TaskScheduler, backend) and executor services (BlockManager, memory manager, shuffle manager), each with its evidence: log lines, REST, shuffle files, event log. Source refs at `v4.3.0`. |
| `book/chapters/ch03-rdds-to-a-sql-engine.md` | From RDDs to a SQL engine | Catalyst, Tungsten (UnsafeRow bytes), whole-stage codegen (generated Java), AQE, then Arrow, columnar plugins, Feather. Same query as Python RDD, Scala RDD, DataFrame, measured. |
| `book/chapters/ch04-one-engine-batch-and-streaming.md` | One engine for batch and streaming, and Spark's modules | Release timeline 0.7 to 4.3 from release notes; DStreams to Structured Streaming to Real-time Mode; one function as batch and stream; module map with chapters; GraphX and GraphFrames 0.12.3 tested on 4.3. |

### Changes to existing chapters

- `ch05-how-spark-runs.md`: two sentences added. In the intro, a pointer to the Background part (Chapter 1). After "The pieces of a Spark application", a pointer to Chapter 2 for the scheduler classes, block manager, shuffle files and memory model. Nothing removed: ch05's section on the pieces is two short paragraphs and a diagram that Part I needs as the hands-on base; ch02 does not repeat it, it goes deeper. STE: 0 findings after the edit.

### Overlap decisions

- ch02 memory: ch33 already measures pool sizes and spill. ch02 reads `maybeGrowExecutionPool` and shows execution evicting storage (new measurement, `memory.py`), links ch33 for the rest.
- ch03 codegen and AQE: ch32 has the full treatment. ch03 shows one generated loop and one final AQE plan, and links ch32 three times.
- ch03 Arrow, plugins, Feather: one measurement (`toPandas`) and a table that links ch34, ch36, ch37, ch38.
- ch04 streaming: links ch39, ch42, ch43 for depth.

## Technical names and verbs (STE)

- TN: driver, executor, cluster manager, scheduler backend, master URL, DAGScheduler, TaskScheduler, TaskSetManager, BlockManager, BlockManagerMaster, MapOutputTracker, LiveListenerBus, map task, reduce task, map output, shuffle file, index file, task attempt ID, locality level (`PROCESS_LOCAL`, `NODE_LOCAL`, `NO_PREF`, `RACK_LOCAL`, `ANY`), offer, task set, unified pool, storage memory, execution memory, dynamic allocation, shuffle tracking, external shuffle service, push-based shuffle, event log, working set (from the HotCloud paper), lineage, narrow dependency, wide dependency, coarse-grained transformation, RDD, DStream, micro-batch, state store, Real-time Mode, continuous processing, Catalyst, Tungsten, UnsafeRow, null bitset, whole-stage code generation (codegen), adaptive query execution (AQE), column vector, columnar plugin, operating system, Project Feather, GraphX, GraphFrames, pandas API on Spark.
- TV: as Part I, plus cache, evict, fetch, spill, recompute, serialize, fuse, compile, materialize, prune.

## STE results

`scripts/ste-check <file> --allow working operating learning Processing Computing programming missing scheduling tracking attempt`

| File | Findings |
|---|---|
| ch01 | 0 |
| ch02 | 0 |
| ch03 | 0 |
| ch04 | 1, false positive: `ste_prep.py` does not remove fenced code that is indented inside a `=== "Stream"` tab, so a code line counts as a 32-word sentence |
| ch05 (edited) | 0 |

Allow words and why: `working` (TN "working set"), `operating` (TN "operating system"), `learning` (TN "machine learning"), `Processing`, `Computing`, `programming` (paper and guide titles), `missing` (DAGScheduler term "missing parents"), `scheduling` ("scheduling pool", "job scheduling" doc title), `tracking` (TN "shuffle tracking"), and `attempt` (TN "task attempt ID").

Deliberate rule breaks: short quotes from release notes and papers keep their own wording (some have `-ing` forms or passive voice: "enabled by default", "graduates", "is now in maintenance mode"); figure alt texts are three words because the lint joins the light and dark alt texts into one sentence. No semicolons or em dashes in prose (the only semicolons are inside pasted output blocks).

## Environment

- pyspark 4.3.0 (uv project in `code/`), OpenJDK 17.0.21.1, Python 3.12, Scala via `uv run spark-shell` (the pip package's `bin/spark-shell`, Scala 2.13).
- Cluster for ch02: `local-cluster[2,2,1024]` and `local-cluster[3,2,1024]` (standalone master and workers inside the driver JVM, one executor JVM per worker). Works from the pip package; no Docker needed.
- GraphFrames: `io.graphframes:graphframes-spark4_2.13:0.12.3` (Maven Central, latest, lastUpdated 2026-10-05) plus `graphframes-py==0.12.3` from PyPI through `uv run --with`. Not added to `pyproject.toml`. The project does not list Spark 4.3; it ran on 4.3.0 Classic in this test (inDegrees, pageRank, connectedComponents).
- Data: `BOOK_DATA=$HOME/spark-masterclass/data`, June 2026 yellow taxi month (3,837,248 rows, 4 row groups), zone lookup.
- Timed runs under `flock ~/spark-masterclass/data/.bench.lock`: `ch01/iterative.py` (3 runs), `ch03/rdd_vs_df.py` and `ch03/RddVsDf.scala` (3 runs each), `ch03/tungsten.py` and `ch03/arrow.py` (3 runs each). Other writers ran Spark on the same machine at the same time without the lock (films, setup), so the spread between runs is wide; the text quotes the saved run and the range of the three runs.
- Films: `film render code/film/ch7X.tape --format chapter` (VHS v0.12.1), cut with ffmpeg to 1280x720, CRF 28, plus a `.webp` poster. Films ran without the lock; captions say their times differ from the text.
- Animations: HyperFrames 0.9.19 through `anim` (create-animation skill), theme `ide`, landscape 1920x1080 rendered, scaled to 1280x720. Sources in `code/ch7X/anim/<name>/` (`index.html`, `storyboard.md`, `data.js` built by `code/ch7X/anim/build_data.py` from the saved output; renders ignored).
- Figures: hand-built SVG from `code/ch7X/figures.py`, which read the saved output. Each figure has a `-light.svg` and a `-dark.svg`, embedded with `#only-light` / `#only-dark`. Shared helper `code/common/svgfig.py` (new; any chapter can use it). Each figure was rendered to PNG with headless Chrome and checked by eye.

## Media

| File | What | Source |
|---|---|---|
| `book/media/clips/ch01-passes.mp4` | film: ten passes each style | `code/film/ch01.tape` |
| `book/media/clips/ch01-anim-passes.mp4` | animation: passes in real measured time | `code/ch01/anim/passes` |
| `book/media/clips/ch02-tasks.mp4` | film: executors, stages, tasks, shuffle index | `code/film/ch02.tape` |
| `book/media/clips/ch02-dynamic.mp4` | film: dynamic allocation events | `code/film/ch02.tape` |
| `book/media/clips/ch02-anim-scheduler.mp4` | animation: job through DAGScheduler and TaskScheduler to executors | `code/ch02/anim/scheduler` |
| `book/media/clips/ch02-anim-shuffle.mp4` | animation: shuffle write, files, fetch | `code/ch02/anim/shuffle` |
| `book/media/clips/ch03-codegen.mp4` | film: timings, UnsafeRow words, generated loop | `code/film/ch03.tape` |
| `book/media/clips/ch03-anim-catalyst.mp4` | animation: the four plans, then the fused stage | `code/ch03/anim/catalyst` |
| `book/media/clips/ch04-graphs.mp4` | film: batch and stream, DStreams, GraphFrames | `code/film/ch04.tape` |
| `book/media/clips/ch04-anim-stream.mp4` | animation: one function as batch and as stream | `code/ch04/anim/stream` |
| `book/media/figures/ch01-dataflow-*.svg`, `ch01-pass-times-*.svg` | MapReduce vs RDD data flow (schematic layout); measured pass times | `code/ch01/figures.py` |
| `book/media/figures/ch02-parts-*.svg`, `ch02-shuffle-files-*.svg`, `ch02-eviction-*.svg` | classes in each process; real shuffle segment sizes; measured eviction | `code/ch02/figures.py` |
| `book/media/figures/ch03-times-*.svg`, `ch03-unsaferow-*.svg` | medians and ranges of the three runs; the 48 bytes of one row | `code/ch03/figures.py` |
| `book/media/figures/ch04-timeline-*.svg`, `ch04-modules-*.svg` | release timeline; module layers with chapters | `code/ch04/figures.py` |

Clip cut list: `book/media/clips/clips-background.tsv`. All mp4 files are under 450 KB.

## Claim-to-source table

| Claim | Source |
|---|---|
| MapReduce model, map output to local disk, reduce output to GFS/HDFS | Dean and Ghemawat, OSDI 2004 (linked) |
| "each job must reload the data from disk", "tens of seconds", "10x in iterative machine learning", RDD definition quote | HotCloud 2010 PDF text (pdftotext of the USENIX PDF) |
| "an order of magnitude", authors | NSDI 2012 abstract page (usenix.org) |
| 16 text files, 40,062,050 bytes; per-pass seconds; 40,375,322 input bytes; 17,393,058 cached; weights equal; median 1.02 vs 0.26 s (3.9x) | `code/ch01/output/iterative.txt` |
| ratio 3.5 to 4.1 over three runs | `iterative.txt`, `iterative-run2.txt` (4.1x), `iterative-run3.txt` (3.5x) |
| lineage, `Memory Serialized 1x Replicated`, 16.6 MiB, weights folders | `iterative.txt` |
| master URL patterns, scheduler backends | `SparkContext.scala:3300-3473` (`createTaskScheduler`, `SparkMasterRegex`), `YarnClusterManager.scala:28`, `KubernetesClusterManager.scala:38` |
| SparkContext init order | `SparkContext.scala` lines 488, 493, 497, 515, 589, 603, 606, 632, 661, 673, 682 |
| local-cluster runs master and workers in the driver JVM | `LocalSparkCluster.scala:53-77` |
| executors, cores, 434.4 MiB | `code/ch02/output/anatomy.txt` |
| DAGScheduler log lines, task placement, broadcast at `DAGScheduler.scala:2796`, 2074 ms job | `code/ch02/output/scheduler_log.txt` |
| `handleJobSubmitted` 2100, `getOrCreateShuffleMapStage` 654, `getMissingParentStages` 1074 (code quoted), `submitStage` 2333, `submitMissingTasks` 2661, event loop 4756, `getPreferredLocs` 4677 | `DAGScheduler.scala` at v4.3.0 |
| Pipelined shuffle co-scheduling in `submitStage`; SPARK-58185 | `DAGScheduler.scala:2333-2400`; 4.3 release notes |
| `submitTasks` 299, `resourceOffers` 573 | `TaskSchedulerImpl.scala` |
| `computeValidLocalityLevels` 1434, `getAllowedLocalityLevel` 631, noPrefs returned as `PROCESS_LOCAL` 422 | `TaskSetManager.scala` |
| `spark.locality.wait` 3s, `spark.shuffle.reduceLocality.enabled` true, `spark.reducer.maxSizeInFlight` 48m, `spark.shuffle.sort.bypassMergeThreshold` 200, `spark.shuffle.service.enabled` false, `spark.shuffle.push.enabled` false, `spark.diskStore.subDirectories` 64, event log rolling/compress true, codec zstd, `spark.dynamicAllocation.schedulerBacklogTimeout` 1s, `shuffleTracking.enabled` true (line 804), `spark.memory.fraction` 0.6, `storageFraction` 0.5 | `core/.../internal/config/package.scala` at v4.3.0 |
| block ID names | `BlockId.scala` |
| cached block placement and PROCESS_LOCAL reads | `anatomy.txt` |
| writer selection rules; 16,777,216 partitions | `SortShuffleManager.scala:91-110, 205, 228`; `PackedRecordPointer.java:43`; `SortShuffleWriter.scala:118` |
| DataFrame shuffle has no aggregator (no map-side combine) | `ShuffleExchangeExec.scala:600-615` |
| map ID = task attempt ID | `ShuffleMapTask.scala:101` |
| shuffle files, sizes, index offsets, hex, local/remote fetch bytes | `anatomy.txt` |
| `getBlockData` 615 | `IndexShuffleBlockResolver.scala` |
| event log folder, 95 events, TaskEnd JSON | `anatomy.txt` |
| `getMaxMemory` 466, reserved 300 MiB line 264, `maybeGrowExecutionPool` 160 (quoted) | `UnifiedMemoryManager.scala` |
| eviction 325.6 to 213.6 MiB, 21/32 partitions, floor 217.2 MiB, spill 560 MiB | `code/ch02/output/memory.txt` |
| executors added 6.8/8.6/8.8 s, removed 31.1-31.2 s | `code/ch02/output/dynamic.txt` |
| doubling ramp-up 457; timeout = max(idle, storage, shuffle) 567-577 | `ExecutorAllocationManager.scala`, `ExecutorMonitor.scala` |
| release facts for 0.7, 1.0, 1.3, 1.5, 1.6, 2.0, 2.2, 2.3, 3.0, 3.2, 3.4, 4.0, 4.1, 4.2, 4.3; announcement dates; incubator June 2013; top-level Feb 2014; no 3.1.0 announcement | spark.apache.org news page and each `releases/spark-release-*.html` page (fetched 2026-11-22). Note: the 4.1 page is `spark-release-4.1.0.html` (dots), the others use dashes. |
| Python RDD 12.71 s, DataFrame 0.28 s (45.7x), wholeStage off 0.29 s; range(400M) codegen 0.14 vs 1.52 s | `code/ch03/output/rdd_vs_df.txt` (= run1) |
| ranges: RDD/DF 20.4 to 45.7x; codegen CPU 4.5 to 11.0x; taxi codegen 0.8 to 1.5x | `rdd_vs_df-run1..3.txt` |
| Scala 1.28 / 0.47 / 0.37 s; ranges 1.1 to 1.3x and 2.3 to 3.5x | `RddVsDf.txt` (= run1), `RddVsDf-run1..3.txt` |
| plans (parsed, analyzed, optimized, physical) | `rdd_vs_df.txt` (the chapter shortens the `Relation` column list and the `FileScan` tail, and says so) |
| UnsafeRow class, 48 bytes, six words | `code/ch03/output/tungsten.txt` |
| cache sizes 72.1 / 122.1 / 62.4 MiB; scan 0.28 vs 0.05 s (5x in all three runs) | `tungsten.txt`, `tungsten-run2..3.txt` |
| 0 then 3 codegen subtrees; generated lines 136-154; final AQE plan | `code/ch03/output/codegen.txt` |
| toPandas 3.91 vs 0.22 s; ranges 3.91-4.05 and 0.18-0.23 | `arrow.txt`, `arrow-run1..3.txt` |
| `spark.sql.execution.arrow.pyspark.enabled` falls back to `spark.sql.execution.arrow.enabled` (true, line 4969); `pythonUDF.arrow.enabled` true; AQE true; wholeStage true | `SQLConf.scala` at v4.3.0 |
| `injectColumnar` 168, `ColumnarRule` | `SparkSessionExtensions.scala`, `Columnar.scala:47` |
| DStream deprecated as of 3.4.0 (FutureWarning text) | `python/pyspark/streaming/context.py:46-75`; `dstream.txt` |
| "previous generation", "legacy project" | `docs/streaming-programming-guide.md:28` |
| batch vs stream results, micro-batch rows, 249 state rows, offsets, both plans | `code/ch04/output/same_code.txt` |
| continuous processing is experimental in the 4.3 docs | `docs/streaming/performance-tips.md:83` |
| RTM 4.1 and 4.3 quotes | 4.1 and 4.3 release notes |
| module jars and Python packages | `code/ch04/output/modules.txt` |
| `spark.mllib` "is now in maintenance mode" | `docs/ml-guide.md:34` |
| SparkR deprecated from 4.0.0 | `docs/sparkr.md:25` |
| GraphFrames 0.12.3 latest, artifacts for spark4_2.13 and connect, pyspark-client support in 0.12.1, Spark 4.2 fix in 0.12.3 | Maven Central `io/graphframes/*/maven-metadata.xml`, GitHub releases page, PyPI `graphframes-py` |
| GraphFrames and GraphX results | `graphs.txt`, `GraphX.txt` |

## Open findings

1. **local-cluster stop prints executor launch errors (not filed, not called a bug in the book).** On `spark.stop()` with `local-cluster[2,2,1024]`, the workers log `ERROR Worker: Failed to launch executor app-.../N ... java.lang.IllegalStateException: Shutdown hooks cannot be modified during shutdown.` and `ERROR TaskSchedulerImpl: Lost executor 0 ... Command exited with code 143`. The job results are correct; only the shutdown is noisy. It looks like the master tries to replace executors that the shutdown killed. Repro: `uv run python -c "from pyspark.sql import SparkSession as S; s=S.builder.master('local-cluster[2,2,1024]').getOrCreate(); s.range(10).count(); s.stop()"` and read stderr. local-cluster is documented as a test mode, so this may be expected.
2. **Dynamic allocation removed idle executors about 15 s after the job, not 10 s.** `ExecutorMonitor.updateTimeout` takes max(idle 5 s, shuffle tracking 10 s) from the idle start. The extra 5 s is not explained. Possibly the idle start is later than the JobEnd event, or the shuffle stays "active" for a while after the job. Repro: `code/ch02/dynamic.py`. The chapter says the cause was not found.
3. **`memory.py` second count after eviction can OOM with a 1 GiB driver.** A recount that re-caches the 11 evicted partitions under the same heap failed (Python side lost the gateway); the step was removed from the script. Not a Spark bug (heap too small for the experiment), recorded so nobody adds it back.
4. **Doc gap, not Spark:** `scripts/ste_prep.py` keeps fenced code blocks that are indented inside `===` tabs, so stecheck counts code as prose (1 false positive in ch04). Fix is in the shared script; I did not edit it.
5. **`PROCESS_LOCAL` for no-preference tasks** (`TaskSetManager.scala:422`) is documented in the chapter as behaviour, not a bug. Users read it as a cache hit; a docs note in `monitoring.md` or `tuning.md` could help.

## Not verified

- Spark 0.7's exact release date (the 0.7 release page has no date; the news page did not list it). The table says 2013.
- GraphFrames on Spark 4.3 over Spark Connect (only Classic was run).
- Which shuffle writer actually ran: the chapter states which writer the 4.3 source rules select for 6 partitions without map-side combine (`BypassMergeSortShuffleWriter`); no log at DEBUG was captured to confirm it at run time.
- The HotCloud "10x" and NSDI "order of magnitude" are the papers' own claims on their clusters; the chapter measures only a laptop analogue and says so.

## Nav and roadmap rows for the integrator

Proposed `mkdocs.yml` nav, as the first part (before "Part I: Getting started"):

```yaml
  - "Background":
    - "70. From MapReduce to Spark": chapters/ch01-mapreduce-to-spark.md
    - "71. Inside a Spark application": chapters/ch02-inside-a-spark-application.md
    - "72. From RDDs to a SQL engine": chapters/ch03-rdds-to-a-sql-engine.md
    - "73. One engine for batch and streaming, and Spark's modules": chapters/ch04-one-engine-batch-and-streaming.md
```

(Use the label format of the existing nav; the numbers become the final ones after `scripts/renumber.py`.)

Roadmap section:

```markdown
## Background
*Data: NYC Taxi trips*

| # | Chapter | Covers |
|---|---|---|
| 70 | [From MapReduce to Spark](chapters/ch01-mapreduce-to-spark.md) | MapReduce and its cost, the AMPLab papers, RDDs and lineage; one iterative job MapReduce-style against `cache()`, measured |
| 71 | [Inside a Spark application](chapters/ch02-inside-a-spark-application.md) | Driver, executors, cluster managers, DAGScheduler, TaskScheduler and locality, BlockManager, the shuffle on disk, the unified memory model, dynamic allocation; source at v4.3.0, REST, event logs. **4.3:** pipelined shuffle dependencies |
| 72 | [From RDDs to a SQL engine](chapters/ch03-rdds-to-a-sql-engine.md) | Catalyst, Tungsten and UnsafeRow bytes, whole-stage codegen, AQE, Arrow, columnar plugins; the same query as RDD and DataFrame, measured. **4.3:** Project Feather |
| 73 | [One engine for batch and streaming, and Spark's modules](chapters/ch04-one-engine-batch-and-streaming.md) | Release timeline 0.7 to 4.3, DStreams to Structured Streaming to Real-time Mode, one function as batch and stream, the module map, GraphX and GraphFrames |
```

Cross-references to fix at renumbering: ch01 to ch04 link each other and link ch05, ch18, ch32-ch39, ch42, ch43, ch49-ch53, ch56, ch61-ch63, ch72, ch75 by current file names. ch05 now links ch01 and ch02.

## Commits

`c13593f`, `b441ce0` (code WIP), `22f2b4a` (ch01), `a8312d9` (WIP), `e794841` (ch02), `5eb0eba` (ch05 edit), `da0b61b` (WIP), `584e9ad` (ch03), `eaa4449` (WIP), `2c0a77a` (ch04), plus the commit of these notes and the last mermaid additions.
