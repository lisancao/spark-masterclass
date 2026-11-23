# Part X notes: Internals (chapters 56 to 59)

Chapters: 56 How Catalyst plans Python, 57 The Python worker protocol, 58 The VARIANT binary format, 59 From wrong results to a fix: an upstream case study. Branch `part-10`. Code in `code/ch74` to `code/ch77`, tapes in `code/film/ch74.tape` to `ch77.tape`, storyboard `code/film/storyboard-part10.md`, clips listed in `book/media/clips/clips-part10.tsv`.

## Technical names and verbs

- **Products and formats:** Spark, PySpark, Spark Connect, Catalyst, JVM, Py4J, gRPC, protobuf, Arrow, Arrow IPC, pyarrow, pandas, Parquet, VARIANT, JSON, YAML, Pyrolite, cloudpickle, Scala, Java, sbt, Git, JIRA, GitHub, VHS, Linux, YARN, Kubernetes.
- **Spark concepts:** query execution phase (parsed, analyzed, optimized, physical), expression ID, rule, batch (`Once`, `FixedPoint`), plan change log, eval type, Python operator, fusion (chain), whole-stage code generation, columnar input, transpilation, worker, daemon, worker reuse, wire tap, init message, data section, runner conf, eval conf, special length (marker), faulthandler, traceback dump, idle timeout, address space, metadata dictionary, header byte, basic type, type info, primitive ID, short string, shredding, shredding schema, typed column, partially shredded object, metric view, measure, dimension, analysis context, namespace, boundary table, minimal reproduction, regression test, shallow clone, pickaxe search.
- **Technical verbs:** analyze, optimize, plan, resolve, fuse, push (a filter), prune, transpile, pickle, unpickle, serialize, decode, encode, fork, tap, broadcast, spill, shred, rebuild, compile, patch, reproduce, minimize, triage, commit, merge.
- **Code identifiers** stay in code format: config keys, class, method and operator names (`ArrowEvalPython`, `ExtractPythonUDFs`, `PushPredicateThroughNonJoin`, `SpecialLengths`, `VariantBuilder`, `MetricViewPlaceholder`, ...), file paths, commands.

## Coverage

### MD39 catalyst_python_plans -> Chapter 74

| Lesson topic | Where |
|---|---|
| `QueryExecution` phases with `explain("extended")`, expression IDs | Step 1 |
| Phases as JVM objects, `TreeNode` walk, `AdaptiveSparkPlan.executedPlan()`, `Scan parquet ` vs `FileScan parquet`, `qe.tracker().phases()` | Step 2 |
| `RuleExecutor`, `Once`/`FixedPoint`, `spark.sql.optimizer.maxIterations`, `planChangeLog.{level,rules,batches}`, `dumpTimeSpent()` | Step 3 |
| `PythonUDF` expression (`Unevaluable`), `PythonEvalType` table (100/101/200/204/205/207/250), Arrow default since 4.2 (SPARK-54555) | Step 4 |
| "Extract Python UDFs" batch quoted, `ExtractPythonUDFs`, fusion rules (nested, parallel, same eval type, iterator UDFs, JVM expression between, identical calls) | Step 5, diagram |
| Filters around Python: AND split, determinism guard (SPARK-13473), UDF predicate, `canPushThrough` | Step 6 |
| UDF runs twice: call-count table (129, 270, 0), `avoidDoubleFilterEval` (SPARK-47672), SPARK-59693 (fix 4.4.0), three rewrites, cache-after-first-action trap | Step 7, clip `ch74-twice` |
| Map and grouped operators, new expression IDs, `isBarrier`, required distribution | Step 8 |
| Python data source planning classes, `PythonPlannerRunner`, formatted mode for pushed filters | Step 8 |
| Whole-stage codegen, Python nodes not `CodegenSupport`, `explain("codegen")` | Step 9 |
| SPARK-58241 hang with columnar input (fixed 4.2.1/4.3.0), `columnarInput.enabled` | Step 9 (history), replayed in ch75 Step 7 |
| Connect protobuf plan, `SparkConnectPlanner.transformPythonFunction`, lazy analysis on Connect | Step 10 |
| 4.3 Python UDF transpilation (new closing section), with benchmark | "New in 4.3", clip `ch74-transpile` |
| `PythonEvals` strategy | Step 1 diagram only |
| `nonExcludableRules`, `toRdd` phase | left out: no behavior that a reader can observe beyond what Steps 1 to 3 show |
| The 4.2.0 Python data source plan-reuse wrong results (`setReadInfo`), Connect plan cache confs | left out on purpose: the bug is not filed upstream (see "Open findings") |

### MD40 python_worker_protocol -> Chapter 75

| Lesson topic | Where |
|---|---|
| Process model (driver, JVM, daemon, forked workers), import after fork, `PythonWorkerFactory` | Step 1, diagram |
| Worker reuse proven by PIDs, `reuse=false`, `use.daemon=false`, no daemon on Windows, reuse decided by `END_OF_STREAM` | Step 2 |
| What reuse keeps (module state, memory, TaskContext reset) | Step 2, Step 11 (address space kept) |
| Runner environment (`OMP_NUM_THREADS`, `PYTHON_UDF_BATCH_SIZE`, `PYSPARK_EXECUTOR_MEMORY_MB`) | Step 11 excerpt (memory). `OMP_NUM_THREADS` and batch size: left out (no run shows them) |
| Wire tap via `spark.python.worker.module` (pyspark prefix, `read1` vs `read`, `spark.executorEnv.PYTHONPATH`) | Step 3 |
| Request byte by byte, `Writer.open`, `writeUDFs`, Pyrolite protocol 2 frames | Step 4, diagram |
| Arrow request (runner conf, `input_type` eval conf, IPC stream) | Step 5 |
| Response (pickle protocol 5, `TIMING_DATA`, spill, accumulators, `END_OF_STREAM`), `SpecialLengths` table | Step 6, sequence diagram, clip `ch75-tap` |
| SPARK-58241 desync walk-through (SPARK-56340, fix quoted) | Step 7 |
| Debugging confs with since-versions (idle timeout, kill on idle, traceback dump, faulthandler both keys) | Steps 8 and 9 |
| Failure taxonomy table | Step 8, clip `ch75-failures` |
| Closure pickling, `_prepare_for_python_RDD`, `UDFCompressionThreshold`, large task binary warning | Step 10 |
| `spark.executor.pyspark.memory` as `RLIMIT_AS`, Arrow worker address space, reused worker keeps it, per-task division, YARN `pysparkWorkerMemory`, overhead 0.10 / 0.40 / 384m | Step 11 |
| 4.3: length-prefixed init message; transpiled UDFs start no worker | "New in 4.3" |
| `spark.python.factory.idleWorkerMaxPoolSize`, broadcast diffs (`getWorkerBroadcasts`), single-threaded runner with `Selector` (SPARK-44705), `killWorkerOnFlushFailure`, `spark.sql.pyspark.worker.logging.enabled`, `spark.sql.planner.pythonExecution.memory`, planning-worker protocol | left out: no script in this part runs them, and the chapter only states what it ran or quoted |

### MD46 variant_binary_format -> Chapter 76

| Lesson topic | Where |
|---|---|
| Source map, README (spec commit, no UUID/Time/nanos, no public shredding API) | intro, Step 3 |
| `VariantVal` two blobs, per-row metadata | Step 1 |
| Metadata dictionary decoded, first-seen order, `sorted_strings` never set | Step 2 |
| Value header, primitive ID table, short string limit | Step 3 |
| Object byte by byte (widths per object, sorted IDs, data in input order) | Step 3, diagram |
| Arrays and nesting, one dictionary for all levels | Step 4 |
| `parse_json` number encodings table, `schema_of_variant` hiding widths, int+decimal vs int+double | Step 5 |
| Typed casts to VARIANT, TIMESTAMP vs NTZ, `to_json` lossy | Step 5 |
| Hand-made bytes accepted, bounds checks only, `MALFORMED_VARIANT` from `checkIndex` | Step 6 |
| Unsorted field IDs return NULL from 32 fields (`BINARY_SEARCH_THRESHOLD`), UTF-16 `compareTo` | Step 6, clip `ch76-handmade` |
| Duplicate keys, `try_parse_json`, `allowDuplicateKeys`, `SIZE_LIMIT` 128 MiB | Step 6 |
| Shredding at byte level with pyarrow, value/typed_value/both/neither, JSON null vs missing, rebuild | Step 7, diagram |
| `pushVariantIntoScan` | Step 7 (ReadSchema) |
| Benchmark (`variant_get` vs `get_json_object`, `to_json` cost) | Step 8 |
| 4.3: VARIANT edit functions at the byte level | "New in 4.3" |
| 4.3: inferred shredding on by default (`inferShreddingSchema` since 4.1.0, `writeShredding.enabled` true) | Step 7 |
| Stale `VariantUtil.java` class comment and stale 16 MiB in `error-conditions.json` | left out of the book (open findings below) |
| Shredded field filters in PushedFilters but not applied by ParquetFilters | left out: unverified on 4.3 and not filed |

### MD55 upstream_case_study -> Chapter 77

The lesson's case was the Python data source pushdown leak (`setReadInfo`). That bug is not filed upstream, so the brief rules it out. Chapter 77 teaches the same method on SPARK-60206 (filed by Lisa, public, open PR apache/spark#59416), a wrong-results bug that reproduces on 4.3.0.

| Lesson topic | Where |
|---|---|
| The investigation loop | Step 1, diagram |
| Minimal reproduction rules and a short repro with expected values | Step 2, clip `ch77-repro` |
| Boundary tables | Step 3 |
| Hypothesis with predictions | Step 4 |
| Experiments through Py4J (catalog metadata, analyzed plan) | Step 4 evidence |
| Reading the source (object ownership, the code path that skips the context) | Step 5, diagram |
| History: which tags, the shallow-clone pickaxe trap | Step 6 |
| Master's fix and a narrow patch for the maintenance line | Step 7 |
| Testing the fix without building Spark (Scala compiler from `pyspark/jars`, `spark.driver.extraClassPath`, limits incl. `NoSuchMethodError`) | Step 8, clip `ch77-fix` |
| Failing regression tests, fail for the right reason | Step 9 (the PR's Scala test, quoted; not run here) |
| Building Spark for real | Step 9 (command named, not run) |
| Triage: JIRA REST, affects versions only reproduced, correctness label, Blocker guidance, target branches | Step 10 |
| JIRA template | Step 10 (the six parts of a good report, checked against SPARK-60206) |
| Workarounds | Step 3 row 4, Step 10 |
| Review lessons | Step 10 (two public review comments on #59416) |
| Connect plan cache, `localCheckpoint` workaround, PySpark mixin test style | left out: specific to the pushdown leak |

## Claim-to-source table

| Claim | Source |
|---|---|
| ch74: 98 status-200 requests, 31 api among them, 70 api in all | `code/ch74/output/twice.txt` |
| ch74: phase times 3/5/3 ms and 8/10/7 ms | `phases.txt` (first run), the second run was observed while the earlier writer ran the script; not saved |
| ch74: rule metrics, 1947 runs, `ResolveDataSource` 461 ms | `rules.txt` |
| ch74: eval types and fusion plans | `eval_types.txt`; `PythonRunner.scala`, `ExtractPythonUDFs.scala` at v4.3.0 |
| ch74: 270 = 200 + 70, 129 = 98 + 31, 0 for count | `twice.txt` |
| ch74: SPARK-59693 fix 4.4.0, SPARK-58241 fix 4.2.1/4.3.0, SPARK-54555 4.2.0, SPARK-47672 4.2.0, SPARK-55214 4.3.0, SPARK-56340 4.2.0, SPARK-56350 4.2.0 | Apache JIRA REST, checked 2026-11-22 |
| ch74: columnar input table and times | `codegen.txt` |
| ch74: Connect plan 1,571 bytes, UDF 776 bytes | `connect_plan.txt` |
| ch74: transpiler results, overflow difference, 78 rows | `transpile.txt`; `python/pyspark/sql/transpile.py`, `udf.py` at v4.3.0 |
| ch74: benchmark 0.51 / 0.54 / 0.14 / 0.18 s, 3,837,248 trips | `bench_transpile.txt` |
| ch75: process IDs, reuse counts 4/8/12, 12 distinct PIDs | `processes.txt` |
| ch75: request 4633 B / init 4582 B, Arrow 5740 / 5376 B, response 102 / 388 B, timings 75/0 and 170/129 ms | `tap.txt` |
| ch75: 4.3 init message framing not in 4.2.0 | `git grep START_OF_INIT_MESSAGE` at v4.2.0 (0 hits) and v4.3.0; commit 6bc27159eb1 |
| ch75: desync numbers (88, 2,065,855,609, 1140.1 ms) | `desync.txt` |
| ch75: failure classes and times | `failures.txt`, `segfault.txt` |
| ch75: config since-versions | `SQLConf.scala`, `core/.../internal/config/*.scala` at v4.3.0 |
| ch75: closure sizes 3,901,147 B, 56 B, 592 B, 780,235 B | `closures.txt` |
| ch75: memory table (VmSize 68/297/470/1366 MiB) | `memory.txt` |
| ch75: memory divided by concurrent tasks | `BasePythonRunner.getWorkerMemoryMb`, `PythonRunner.scala` at v4.3.0 |
| ch75: overhead 0.10, 0.40, 384m | `core/.../config/package.scala`, k8s `Constants.scala` at v4.3.0 |
| ch76: 61 rows, 1 NULL, sizes, 3 distinct metadata blobs | `code/ch76/output/layout.txt` |
| ch76: byte layouts | `layout.txt`; `VariantUtil.java`, `VariantBuilder.java` at v4.3.0 |
| ch76: encodings table, agg types, casts, round trip | `encodings.txt` |
| ch76: 60 of 60 byte-identical, broken bytes, unsorted IDs at 5/31/32/40, duplicate keys | `handmade.txt`; `Variant.java` (`BINARY_SEARCH_THRESHOLD = 32`) |
| ch76: 128 MiB limit | `VariantUtil.java` `SIZE_LIMIT` (read, not run) |
| ch76: shredding schema, row states, 60 of 60 rebuilt, mixes, ReadSchema, 7,403 vs 2,564 B | `shredding.txt`; `ShreddingUtils.java` |
| ch76: benchmark numbers | `bench.txt` (the second locked run). A first locked run, before a label fix in the script (it printed "20 fields"; the trips have 21 columns), gave 3.55 / 5.41 / 1.10 / 0.22 / 4.36 s, the same order and similar ratios; it was overwritten and is not quoted |
| ch76: edit functions since 4.3.0 and their byte changes | `edits.txt`; `DESCRIBE FUNCTION EXTENDED variant_set` |
| ch77: wrong rows on 4.3.0 | `code/ch77/output/repro.txt` |
| ch77: boundary rows and evidence | `boundaries.txt` |
| ch77: metric views in v4.2.0 and v4.3.0, not v4.1.0, shallow clone boundary | `history.txt` |
| ch77: fix works on 4.3.0 classes, 169 class files, 9 s | `test_fix.txt` |
| ch77: JIRA and PR fields | `jira.txt` (snapshot 2026-11-22) |
| ch77: review quotes | apache/spark#59416 review comments by HyukjinKwon (2026-10-14), also in `~/repos/LESSONS/spark.md` |
| ch77: correctness label and Blocker guidance | https://spark.apache.org/contributing.html |
| ch77: "fail on master without this change" | the description of apache/spark#59416 |

## Environment

- `pyspark[connect]==4.3.0` from `code/pyproject.toml` (uv), Python 3.12, OpenJDK 17.0.21, Linux, 16 cores. `local[*]`, `local[1]` or `local[2]` as each script says.
- Spark UI port 4031 where a script uses `common.book.session`; most Part X scripts turn the UI off. Connect server (ch74 Step 10) on gRPC port 15061 by `code/ch74/server.sh` (the earlier writer chose it; the assignment's 10310 was not used and nothing else listens on 15061 here).
- Spark source: `~/repos/spark` at tag `v4.3.0` (a shallow clone; see ch77 Step 6). PR branch `origin/SPARK-60206-metric-view-namespace` for the ch77 excerpts.
- Scala compiler for ch77: `scala-compiler-2.13.18.jar` from the pyspark 4.3.0 wheel.
- No external packages (no Delta, Iceberg, Kafka, Avro) in this part.
- Benchmarks ran under `flock $HOME/spark-masterclass/data/.bench.lock`. The ch74 and ch75 films also ran under it (the rule at the time). After the integrator's rule change, the ch76 and ch77 films ran without the lock; no film timing is quoted in the text. ch74 benchmark: one warm-up, median of five fresh DataFrames. ch76 benchmark: one warm-up, median of five runs of each SQL query.
- Data: `code/data/logs/access.log` (ch74), `code/data/events/` (ch76, three days, the last line of day 3 is broken on purpose), the June 2026 taxi month under `$BOOK_DATA` (ch74 and ch76 benchmarks). ch75 and ch77 use synthetic rows only.
- `code/common/book.py` was not changed.

## STE check

`scripts/ste-check book/chapters/<file>` (prepared, shared allowlist), with the chapter allowlist where given.

| Chapter | Findings, shared allowlist only | With chapter allowlist | Chapter allowlist |
|---|---|---|---|
| ch74 | 0 | 0 | none |
| ch75 | 11 | 0 | `setting settings Broadcasting Introducing debugging processing warning forking` |
| ch76 | 15 | 0 | `shredding Shredding VariantEncoding VariantShredding containing missing setting` |
| ch77 | 7 | 4 | `Contributing warning` |

Rules broken on purpose:

- **Quotations stay as written.** ch77 Step 10 quotes two review comments and one sentence of the contributing guide. They are longer than 25 words or passive ("were ignored", "be considered"); they are the 4 remaining ch77 findings. ch75 quotes "forking processes from Java is expensive" and a commit title ("Introducing ..."). ch76 quotes the README ("containing").
- **`-ing` words as technical names:** shredding (the Parquet feature), setting (noun for a config), processing (a field name in the output), Broadcasting (log text), debugging and Contributing (titles of linked pages), warning (admonition noun), missing (the state name of a shredded field).
- **Parentheses** hold config keys, JIRA numbers, byte values or short labels.

## Open findings (not in the book as bugs)

1. **Python data source filter pushdown leaks into a reused base scan (4.1.0 to 4.3.0, unfiled).** The MD55 lesson's case. Not used, because it is not filed. Lisa's draft: `~/spark-contributor/drafts/OPP-2026-10-26-PYDS-PUSHDOWN-LEAK/jira.md`.
2. **SPARK-60206 has no `correctness` label and lists only 4.2.0.** ch77 shows it reproduces on 4.3.0 (pip `pyspark==4.3.0`). Lisa may want to add "4.3.0" to Affects Version/s and the `correctness` label (the chapter says the label is missing; it does not say anything else about the report). If she adds them, update Step 10's snapshot and its paragraph on the label.
3. **Stale comment in `VariantUtil.java` (v4.3.0):** the class comment says the dictionary size is a 4-byte integer and the offsets are 4-byte; `VariantBuilder.result()` writes `offset_size` bytes for both. Doc gap. Repro: decode any `parse_json` metadata (header `0x01`, size 1 byte). Unfiled.
4. **`VARIANT_CONSTRUCTOR_SIZE_LIMIT` message says 16 MiB** in `error-conditions.json` at v4.3.0; `SIZE_LIMIT` is 128 MiB outside tests. Doc gap, unfiled.
5. **No validation of the field-ID sort on read** (ch76 Step 6). Spark's own writer is correct; a foreign writer that breaks the spec gets NULL from `variant_get` from 32 fields up, with no error. The book states this as behavior for spec-breaking input, not as a bug. Possible improvement: validate in `MALFORMED_VARIANT` checks, or document it.
6. **`schema_of_variant` drops trailing zeros of the scale** (`1.50` -> `DECIMAL(2,1)` while the bytes keep scale 2). Shown in ch76 as behavior. Not clearly a bug.
7. **SPARK-56324 JIRA vs commit:** commit 6bc27159eb1 ("[SPARK-56324] Introducing message-based communication ...") is in v4.3.0 and not v4.2.0, but JIRA SPARK-56324 has the summary "Add worker specification & gRPC UDF service as possible code paths to PySpark" and fix version 4.2.0. ch75 cites the commit, not the JIRA. Probably an umbrella issue; not checked further.

## Not verified

- ch77 Step 9: the Scala regression tests of #59416 were not run here (no Spark build). The chapter says so and cites the PR description.
- ch75 runner environment variables other than `PYSPARK_EXECUTOR_MEMORY_MB` are not shown by a run.
- ch74 Step 2: the second-run phase times (8, 10, 7 ms) were in the earlier draft; the saved `phases.txt` holds one run.
- The film clips show live runs, so their timings differ from the saved outputs (captions quote no numbers).
- Pacing of the clips needs a person to watch them at 1x.

## Clips

| Clip | Chapter, place |
|---|---|
| `ch74-twice` | ch74 Step 7 |
| `ch74-transpile` | ch74 "New in 4.3" |
| `ch75-tap` | ch75 Step 6 |
| `ch75-failures` | ch75 Step 8 |
| `ch76-handmade` | ch76 Step 6 |
| `ch77-repro` | ch77 Step 2 |
| `ch77-fix` | ch77 Step 8 |

`ch77-fix` is the `fix` beat with its wait for the compile and the two runs (seconds 8 to 90 of the beat) played at 6x speed, so the clip is 36 s, not 104 s; the start and the final output are at normal speed. The `start`/`end` columns of `clips-part10.tsv` give the beat in the full cut.

`ch76.tape` also renders a `layout` beat, which is not embedded (kept for a narrated cut).

## Nav entries

```yaml
  - "Part X: Internals":
    - "How Catalyst plans Python": chapters/ch74-catalyst-plans-python.md
    - "The Python worker protocol": chapters/ch75-python-worker-protocol.md
    - "The VARIANT binary format": chapters/ch76-variant-binary-format.md
    - "From wrong results to a fix": chapters/ch77-upstream-case-study.md
```
