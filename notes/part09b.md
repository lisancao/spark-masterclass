# Part IX, second half (chapters 52 to 55): writer notes

Chapter numbers here are the old numbers of `notes/assignments.md`. At integration, `notes/renumber.md` maps 52-55 to 55-58, and the cross-references "Chapter 47" to "Chapter 51" (the Python data source chapters) to 50-54. Chapter 15 (VARIANT) keeps its number. "Chapter 59" (the upstream case study, cited in ch55) becomes 62.

## Technical names and verbs (STE)

Technical names used with one meaning in all four chapters: DSv2, `ScanBuilder`, `Scan`, `Batch`, `InputPartition`, `PartitionReader`, `InternalRow`, `UTF8String`, predicate, pushdown (noun), push (verb: give a filter to a source), plan (noun and verb), optimizer, analyzer, rule, check rule, extension, injection point, expression, whole-stage code generation, `CodegenFallback`, `StaticInvoke`, Python UDF, Arrow-optimized Python UDF, pickled UDF, transpiler, transpile (verb), eval type, plugin, relation plugin, expression plugin, command plugin, type URL, wire format, wire type, varint, key (protobuf), artifact, class loader, registry, session, Connect server, operation, manifest, job folder, offset, micro-batch, admission control, checkpoint, oracle, mutation testing, first fit decreasing, VARIANT, BOM (byte order mark), CRLF, shade/relocate (verbs for jar packages), shuffle, partition (noun and verb), commit, abort.

## STE checker results

`scripts/ste-check` (wraps `stecheck --mode description`), final counts:

| Chapter | Findings | Extra `--allow` words |
|---|---|---|
| 52 | 0 | none |
| 53 | 0 | none |
| 54 | 0 | none |
| 55 | 0 | `decreasing`, `testing` (in the technical names "first fit decreasing" and "mutation testing") |

`scripts/ste-check FILE description --allow WORDS` replaces the shared allowlist instead of adding to it (the second `--allow` wins in argparse). For ch55 I ran `scripts/ste_prep.py FILE | stecheck - --mode description --allow $(cat scripts/ste-allow.txt) decreasing testing`. Worth a fix in `scripts/ste-check` (not my file).

Rules broken on purpose: none flagged. Quotes from Spark docs that broke STE (ch53 javadoc of `SparkSessionExtensions`, the doc of `transpilePyUDFs`, ch52 javadoc of `SupportsPushDownV2Filters`) are paraphrased in STE instead of quoted. Literal log text (`Data source write support ... is aborting`) is in code format.

## Coverage

### MD42 java_dsv2_connector -> Chapter 52

Written by the earlier writer (commit 0225fc2); I fixed its STE findings (14 to 0) and made its clips. Coverage checked against the inventory:

| Topic | Section |
|---|---|
| What `PythonScanBuilder` leaves out, capability table | The capability table |
| javac against pyspark jars; Maven `pom.xml`, compiler plugin pin, shading | Step 1 (`build.sh`, `nopin.sh`) |
| Py4J back door `spark._jvm.jdocs.Audit` | Steps 4, 6 |
| Read path class table, driver vs executor | Step 2 (+ mermaid) |
| `META-INF/services`, `DATA_SOURCE_NOT_FOUND`, `DefaultSource`, `CREATE TEMPORARY VIEW ... USING` | Step 3 |
| When methods run | Step 4 |
| `InternalRow` types table, `ClassCastException` trap | Step 5 |
| V2 `Predicate` table as received | Step 6 |
| `description()` in explain, pushed filters in plans | Step 7 |
| `V2ScanRelationPushDown` order, LIMIT, aggregates complete/partial, `LocalScan` | Step 8 (+ mermaid) |
| Statistics, broadcast | Step 9 |
| `KeyGroupedPartitioning`, `HasPartitionKey`, removed Exchange | Step 10 |
| `SupportsRuntimeV2Filtering`, DPP | Step 11 (+ mermaid) |
| Custom metrics | Step 12 |
| Rest of the API (TopN, Offset, TableSample, PushDownJoin, VariantExtractions, columnar, V1 filters, iterative pushdown) | Step 13 |
| `explain("cost")` | left out (statistics are shown through the plan's `Statistics(...)` instead) |

### MD43 session_extensions -> Chapter 53

| Topic | Section |
|---|---|
| Injection point table (4.3, incl. new `injectPreOperatorOptimizationRule`), stability annotations, loading paths | Step 1 (+ mermaid) |
| Java/Scala interop (`AbstractFunction1`, `BoxedUnit`, lambdas, `AbstractPartialFunction`) | Steps 2, 7 |
| Hand-written `Expression`, the five traps with real errors | Steps 3, 4 (`traps.py`) |
| `injectFunction`, `ExpressionInfo`, `DESCRIBE FUNCTION`, analysis errors | Step 5 |
| `F.call_function`, `F.expr`, wrapper, `recursiveFileLookup` trap | Step 5 |
| `CodegenFallback` vs `StaticInvoke`, `supportCodegen`, `RuntimeReplaceable`, nine-argument constructor | Step 6 |
| Expressions run on the driver (constant folding) | Step 6 |
| `PngRule` replaces a Python UDF; `PythonUDF` fields; `DataFilters` | Step 7 |
| Rewrite safety: name, determinism, types, NULL via `coalesce`, fixed point, batch order | Step 8 |
| Plan change log | Step 8 (`planchange.py`) |
| **4.3:** Python UDF transpilation (eval type 100 only, ANSI, Classic only, refusals, flag read twice, `ConvertToCatalyst` first batch) | Step 9 (+ mermaid) |
| Check rules, eager in Classic | Step 10 |
| Benchmark table and the reused-DataFrame trap | Step 11 |

### MD44 connect_plugins -> Chapter 54

| Topic | Section |
|---|---|
| What works over Connect without a plugin; `JVM_ATTRIBUTE_NOT_SUPPORTED`; when you need a plugin | Step 1 |
| Extension points 998/999 (verified at v4.3.0), interfaces, settings, 3.4 Scala traits -> 4.0 Java, `GetStatusPlugin` (4.1), `MLBackendPlugin` | Step 2 |
| Registry: in sequence, first non-empty, `NO_HANDLER_FOR_EXTENSION`, no-arg constructor, `PLUGIN_CTOR_MISSING`, `PLUGIN_RUNTIME_ERROR`, one instance, lazy and global | Step 2 (+ mermaid) |
| Why `byte[]` (shaded protobuf, class name checked in the 4.3.0 jar); wire format by hand; `Any` byte table; bytes vs embedded message; `parseRelationWithRecursionLimit` | Step 3 |
| `PlanNodesPlugin` code; `transformRelation`, `session()` (`@Since 4.0.0`), `Dataset.ofRows`, AQE descent | Step 4 |
| `remote("local[2]")` in-process server, warnings, port 15002 trap, `start-connect-server.sh` (in the 4.3.0 pip package) | Step 5 |
| **4.3:** `spark.local.connect.reuse` persistent local server (new, experimental) | Step 5 (`reuse.py`) |
| Python client: `descriptor_pb2` messages, `LogicalPlan` subclass, `_create_proto_relation`, `plan_id`; no public extension API in 4.3 (checked) | Step 6 |
| `plan_nodes` tables; plan assertions (PushedFilters, `ArrowEvalPython`, `PngRule` over Connect); no job, planned many times | Step 7 |
| Unknown message, `Optional.empty()` | Step 8 |
| Expression plugin | Step 9 |
| Command plugin, `execute_command` | Step 10 |
| **4.3:** operations REST endpoint records each plugin call | Step 11 |
| Artifacts, session class loader, the artifact-installs-a-plugin experiment and the registry source | Step 12 (+ sequence diagram) |
| Maven with `spark-connect_2.13` provided, protobuf relocation | Step 13 |
| Operation IDs: reattach, interrupt | left out here; Chapter 4 covers operation IDs and REST, Step 11 links the plugin calls to it |
| Built-in structured plans in 4.3 that would replace `plan_nodes` | checked: none found in `python/pyspark/sql/connect/` at v4.3.0 |

### MD56 final_project -> Chapter 55

| Topic | Section |
|---|---|
| Project framing, capability table mapped to chapters | opening table |
| The casefiles format, planted problems table, BOM | Step 1 |
| `list_case_folders` rules, `name@job` exclusion | Step 1 |
| The contract: three tables, options, write and stream entry points | Step 2 (+ mermaid) |
| Parsing: v1/v2 map, `utf-8-sig`, bad index as a row, NULL + problem, aware UTC, `\r`, unparsed lines, `attachment_status` | Step 3 |
| Planning by bytes, FFD, item per table, file-less rows owner | Step 4 |
| `partitions()` raising `NotImplementedError` -> `InputPartition(None)` (verified on 4.3.0) | Step 4 (`gotchas.py`) |
| Batch output of all three tables | Step 5 |
| VARIANT via `VariantVal.parseJson`, `schema_of_variant`, DECIMAL for `2.5` | Step 6 |
| Pushdown: exact semantics, aware UTC datetimes, `PushedFilters` in JVM time zone, two reader classes | Step 7 |
| The leak on a reused DataFrame (present in 4.3.0), `CaseFiles` | Step 8 |
| Writer: manifest pointer swap, commit rejects, abort cleanup, Delta-like; POSIX-only atomicity | Step 9 (+ sequence diagram) |
| `raise_error` does not exercise `abort()` (verified on 4.3.0) | Step 9 (`gotchas.py`) |
| Streaming: byte offsets, complete lines, `ReadMaxBytes`, `ReadAllAvailable`, AvailableNow, oracle, byte budget | Step 10 (+ mermaid) |
| Logs via `python_worker_logs()`; Logger pickles by name (verified) | Step 11 |
| Offline tests and mutation testing | Step 12 |
| Staged grader with `ONLY` | Step 12 |
| Design review template and answers | Step 13 |
| Java path mapping table (mapped to the `jdocs` classes of Chapter 52); why DSv2 scan builders do not share state | Step 14 |
| `CasesScanBuilder` Java sketch | left out: the table maps each part to the working `jdocs` class of Chapter 52 instead of an uncompiled sketch |
| Taking it further | Taking it further |
| Course closing ("what you can do now", SparkTutor courses) | left out: tutor framing; the book continues into Part X |
| 4.3: reading archives, RTM for Python sources | left out: not verified for Python data sources |

## Claim-to-source table

| Ch | Claim | Source |
|---|---|---|
| 52 | 26 files; filter + two columns, limit, count(*) plans; 8.0 EiB vs 3.0 KiB statistics | `ch52/output/capability.txt` |
| 52 | timing range over four runs (pydocs vs jdocs) | `ch52/output/capability_runs.txt` (rerun this session, see below) |
| 52 | Maven 3.8.7, javac 17.0.21.1; compiler plugin 3.1 fails without the pin | `ch52/output/build.txt`, `nopin.txt` |
| 52 | rows equal Python vs Java (26/26/0), predicates as received, pushdown plans, COUNT(*) LocalTableScan, partitioning, DPP, metric | `ch52/output/jdocs.txt` |
| 53 | all extension, trap, rule, NULL and check-rule outputs | `ch53/output/extensions.txt`, `traps.txt`, `planchange.txt` |
| 53 | `injectPreOperatorOptimizationRule` exists in v4.3.0, not in v4.2.0 | `git grep` on both tags |
| 53 | transpiler: eval type 100 only, refusals, ANSI warning, same counts (2192451/16815/777470/850512) | `ch53/output/transpile.txt`; `python/pyspark/sql/udf.py` v4.3.0 |
| 53 | flag read twice (UDF build and `ConvertToCatalyst`) | `ch53/output/transpile.txt` section 4; `Optimizer.scala` v4.3.0 lines 1304-1320 |
| 53 | benchmark: 20,000,000 values, part A 0.309-0.384 s native, 2.062 s pandas, 3.356 s Arrow UDF; part B 0.451 s transpiled, 0.633 s built-in, 1.052 s Arrow, 1.133 s pickled | `ch53/output/bench.txt`, medians of 5 after 1 warm-up, under the bench lock, 16 cores, `local[*]` |
| 54 | `Any` bytes, `JVM_ATTRIBUTE_NOT_SUPPORTED`, plan tables, pushed filters, `NO_HANDLER_FOR_EXTENSION`, `has_magic`, `jplug.note`, operations record | `ch54/output/plugins.txt` |
| 54 | proto fields 998/999, plugin interfaces, registry code, error classes, settings static since 3.4.0, getStatus since 4.1.0 | v4.3.0 source: `relations.proto`, `expressions.proto`, `commands.proto`, `SparkConnectPluginRegistry.scala`, `Connect.scala`, `error-conditions.json` |
| 54 | `org/sparkproject/connect/protobuf/Any.class` in `spark-connect_2.13-4.3.0.jar` | `unzip -l` of the jar in the venv |
| 54 | 3.4.0 Scala traits, 4.0.0 Java interfaces | `git ls-tree` at v3.4.0 and v4.0.0 |
| 54 | in-process server warnings, port check | `ch54/output/local_server.txt` |
| 54 | reuse: first run starts a server, second connects in well under a second, discovery keys | `ch54/output/reuse.txt`; `python/pyspark/sql/connect/local_server.py` v4.3.0 |
| 54 | artifact experiment: session B uses the plugin from session A's artifact | `ch54/output/artifacts.txt`; `SparkConnectPluginRegistry.withInitialize` |
| 55 | all tables, plans, manifest, stream batches, warnings | `ch55/output/demo.txt` |
| 55 | `NotImplementedError` fallback, Logger pickling, `raise_error` vs abort | `ch55/output/gotchas.txt`; `plan_data_source_read.py` v4.3.0 lines 241-246 |
| 55 | `DATA_SOURCE_PUSHDOWN_DISABLED` check examines the reader object | `plan_data_source_read.py` v4.3.0 lines 363-370 |
| 55 | grader 10 of 10, 8 tests, 9 mutants caught, 6+1+1 batches, 48 rows | `ch55/output/grade.txt` |
| 55 | `call.wav` 40,044 bytes | `stat` on the sample |

## Environment

- Linux 7.0.0, 16 cores, `local[*]`; OpenJDK 17.0.21.1 (javac 17.0.21.1); Apache Maven 3.8.7
- Python 3.12.3, `pyspark[connect]==4.3.0` (the uv project in `code/`)
- Maven coordinates (all `provided`): `org.apache.spark:spark-sql_2.13:4.3.0` (ch52, ch53, ch54), `org.apache.spark:spark-connect_2.13:4.3.0` (ch54); `maven-compiler-plugin` 3.13.0 pinned
- Connect server for ch54: `sbin/start-connect-server.sh` from the pip package, gRPC 10300, UI 4130; reuse experiment on 10301 / UI 4131; in-process server on the default 15002 (the script checks the port first)
- `SPARK_UI_PORT=4030` for Classic runs
- Timings (ch53 bench, ch54 reuse, ch52 capability runs) ran under `flock $HOME/spark-masterclass/data/.bench.lock`. The ch53 film also ran under it; the ch52, ch54 and ch55 films ran without it, after the integrator changed the lock rule (films take no lock)

## Open findings (not yet checked with the spark-bug routine)

1. **A Connect artifact jar installs a plugin for every session.** Server started with `spark.connect.extensions.relation.classes=jplug.PlanNodesPlugin` but without the jar. Session A's first plugin call fails (`ClassNotFoundException`), the registry stays uninitialized; after `spark.addArtifacts(jar)`, the next call loads the class through session A's class loader and caches it in the process-wide `SparkConnectPluginRegistry`; a new session B that never uploaded the jar then uses it. Repro: `ch54/server.sh bare && uv run python ch54/artifacts.py`. Possibly a security concern on shared servers (one user's code runs for all sessions). Check master and 4.2, and whether artifacts are meant to be able to reach the registry at all.
2. **Python data source pushdown leaks into a reused DataFrame on 4.3.0.** With `spark.sql.python.filterPushdown.enabled=true` and a reader that implements `pushFilters`, `df.count(), df.filter("status = 'open'").count(), df.count()` gives `[10, 5, 5]`. Not yet run through the spark-bug routine and not filed; the chapter describes it as observed behavior, not as a Spark bug. (The Part X case study uses SPARK-60206, not this.) Repro: `ch55/demo.py` section 7.
3. **`transpilePyUDFs` is read at UDF build time and again in `ConvertToCatalyst`.** Not a bug, but the config doc does not say it. A UDF built with the flag on and planned with it off logs "is disabled but we still got TranspiledPythonUDFs in our plan" and runs in Python. Doc gap candidate.
4. **`scripts/ste-check --allow` replaces the shared allowlist** (book tooling, not Spark).

## Not verified

- ch52's earlier timing sentence ("in four runs, 74 to 131 ms ...") came from the earlier writer's runs, which were not saved. I reran four runs into `ch52/output/capability_runs.txt` (pydocs 194-278 ms, jdocs 59-120 ms; under the lock, but other writers' unlocked jobs used the machine) and updated the sentence to those numbers. `capability.txt` itself (74.2 / 15.1 ms) is from the earlier, quieter run.
- ch53 part B: the built-in `when` chain (0.633 s) measured slower than the transpiled UDF (0.451 s). It ran first in its table; I did not measure run-to-run variation, and the chapter says so instead of reading a result into it.
- The ch54 operations table shows execution times from one run while other writers used the machine; the chapter says they are not a measurement.
- Clips: rendered with VHS in the chapter format; pacing not reviewed at 1x by a person.

## Film storyboard (VO drafts)

| Tape | Beat | On screen | VO (draft) |
|---|---|---|---|
| ch52 | capability | `capability.py`: three plans per source | The same three queries go to a Python source and a Java source. Only the Java scan reads two columns, takes the limit, and answers the count with no scan. |
| ch52 | pushdown | `jdocs.py`: pruning, a pushed filter, COUNT(*) | The Java source opens no file for a pruned scan, five files for the pushed filter, and none for a count. |
| ch53 | rule | `extensions.py` sections 7 and 8 | The same Python UDF under two names. The rule replaces only is_png, and the NULL row keeps the Python answer. |
| ch53 | transpile | `transpile.py` sections 1 to 3 | With the 4.3 flag on, only the non-Arrow UDF loses its Python node, and the transpiler says why it refuses the others. |
| ch54 | plugins | `plugins.py` section 4 | A Connect client has no JVM. A server plugin sends the plan of any DataFrame back as rows. |
| ch54 | artifacts | `artifacts.py` | One session uploads the plugin jar as an artifact. A second session, which never uploaded it, can use the plugin too. |
| ch55 | stream | `demo.py` section 9 | The change logs stream at 400 bytes per batch, and the line with no newline waits. |
| ch55 | grade | `grade.py` | Ten stages: parsers and tests with no Spark, then batch, pushdown, writer, stream and logs. |

## Nav entries

```yaml
  - "Part IX: Extending Spark":
      - chapters/ch52-dsv2-connector-java.md
      - chapters/ch53-catalyst-extensions.md
      - chapters/ch54-connect-plugins.md
      - chapters/ch55-capstone-connector.md
```

(After renumbering: ch55-dsv2-connector-java.md, ch56-catalyst-extensions.md, ch57-connect-plugins.md, ch58-capstone-connector.md.)
