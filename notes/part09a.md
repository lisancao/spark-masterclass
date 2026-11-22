# Part IX (first half) notes: Python data sources, chapters 47 to 51

Branch `part-09a`. Chapters:

- `book/chapters/ch50-python-data-sources-readers.md`
- `book/chapters/ch51-python-data-sources-apis-databases.md`
- `book/chapters/ch52-python-data-sources-pushdown.md`
- `book/chapters/ch53-python-data-sources-writers-streaming.md`
- `book/chapters/ch54-shipping-a-python-data-source.md`

Code in `code/ch50` to `code/ch54`, outputs in `code/chNN/output/`, tapes `code/film/ch50.tape` to `ch54.tape`, storyboard `code/film/storyboard-part09a.md`, clip cut list `book/media/clips/clips-part09a.tsv` (named `part09a`, not `part09`, so it does not collide with the part-09b branch).

## Technical names and verbs (STE)

- Technical names: Python Data Source API, data source, source, reader, writer, stream reader, stream writer, sink, partition (an `InputPartition`), planned read, planning (the phase, used as a noun and as a modifier: "planning worker", "during planning"), reading (the phase, in "planning and reading"), task attempt (`attempt` as a noun), file listing (`listing` as a noun), staging folder (`staging` as a modifier), processing-time trigger, real-time mode, micro-batch, offset, checkpoint, manifest, commit message, commit protocol, filter object, pushdown, push and return, affinity (SQLite), key range, stride, quantile, snapshot mark, cursor, page, worker logs, docstring, packaging (in a doc title), VARIANT, Arrow batch, `RecordBatch`.
- Technical verbs: register, plan, pickle, push (a filter), commit, abort, stage (a file), publish, replay, parse, convert, serialize, stream, partition, shuffle, query, log, install, ship, cast.
- `scripts/ste-check` command used (the second `--allow` replaces the shared list, so the shared list is repeated):
  `scripts/ste-check FILE description --allow $(cat scripts/ste-allow.txt) planning reading attempt listing docstring staging processing programming packaging`

## STE finding counts

| Chapter | Findings after the pass |
|---|---|
| ch50 | 0 |
| ch51 | 0 |
| ch52 | 0 |
| ch53 | 0 |
| ch54 | 0 |

Deliberate rule breaks:

- `-ing` words kept as technical names or modifiers: planning, reading (the two phases), staging (folder), listing (file listing), processing (processing-time trigger), programming (title of a Spark guide), packaging (title of a PySpark guide), pruning, partitioning, streaming, warning (admonition).
- `attempt` (a Spark task attempt, `TaskContext.attemptNumber()`) kept as a technical noun. STE prefers `try`.
- Admonition titles and step headings keep code identifiers.
- Parentheses hold config keys, JIRA numbers, file names and short labels, as in Part I.

## Coverage

| Lesson | Topic | Covered in |
|---|---|---|
| MD15 first_python_data_source | `DataSource`, `DataSourceReader`, `InputPartition`, `register`, name and case | ch50 Steps 1, 5 |
| MD15 | schema as DDL or `StructType`, `read()` yields tuples, `Row`, batches | ch50 Step 1, ch51 Steps 7 to 8 |
| MD15 | options are strings, `CaseInsensitiveDict` | ch50 Step 3 |
| MD15 | partitions, `spark_partition_id()` | ch50 Step 4 |
| MD15 | errors, `DATA_SOURCE_RETURN_SCHEMA_MISMATCH` | ch50 Step 6 |
| MD15 | SQL `USING` | ch50 Step 5 |
| MD22 custom_file_format | why a source vs `binaryFile` + UDF | ch50 Step 8 |
| MD22 | planning vs reading table, paths in partitions | ch50 Steps 2, 8 |
| MD22 | cluster storage, `pyarrow.fs` | ch50 Step 8 |
| MD22 | relative paths on Connect, `abspath` in the client | ch50 Step 12 |
| MD22 | sorted listing, `resolve()`, `stat()` | ch50 Step 8 |
| MD22 | byte-balanced greedy planning, bound, partition count rules | ch50 Step 9 |
| MD22 | PNG and WAV header parsing, forgiving parsers | ch50 Step 10 |
| MD22 | open only what the schema asks, no column pruning | ch50 Step 10 |
| MD22 | aware datetimes | ch50 Step 10, ch51 Step 9 |
| MD22 | empty folder, `InputPartition(None)` | ch50 Steps 4, 11 |
| MD22 | option validation, error at first action | ch50 Steps 6, 10 |
| MD22 | tests without Spark | ch50 Step 7, ch54 Step 6 |
| MD22 | files changing between plan and read | ch50 Step 11 |
| MD23 api_source | driver loop anti-pattern | ch51 Step 1 |
| MD23 | lifecycle table, call accounting, plan reuse, cache | ch51 Step 2 |
| MD23 | pagination: pages vs cursors | ch51 Step 2 |
| MD23 | executors must reach the API, imports in methods, sockets | ch51 Step 2 |
| MD23 | timeout, retries with backoff, 4xx vs 5xx, `HTTPError` order, `Retry-After`, idempotent GET | ch51 Step 3 |
| MD23 | local mode no task retries, `local[4,3]`, `spark.task.maxFailures` | ch51 Step 3 |
| MD23 | VARIANT with `VariantVal.parseJson`, `MALFORMED_VARIANT`, `try_variant_get` | ch51 Step 6 |
| MD23 | validation in `DataSource.__init__`, user schema delays it, Connect | ch51 Step 4 |
| MD23 | land to bronze | ch51 Step 2 |
| MD23 | good citizen: concurrency, `pages_per_partition`, secrets, big pages, stable ordering and duplicates | ch51 Step 5 |
| MD25 filter_pushdown | flag, `DATA_SOURCE_PUSHDOWN_DISABLED` | ch52 Step 1 |
| MD25 | measured table of offered filters, classes, attribute tuples | ch52 Step 2 |
| MD25 | contract: unreturned filters not checked | ch52 Steps 3, 5 |
| MD25 | apply in planning, mutate self, return by reference | ch52 Step 3 |
| MD25 | audit + plan proof, `explain("formatted")`, `redirect_stdout` | ch52 Step 4 |
| MD25 | push and return | ch52 Step 5 |
| MD25 | plan-reuse correctness trap, self-union, views | ch52 Step 8 |
| MD25 | design notes (types, intersect, Not and NULL, planning cost, streaming no filters) | ch52 Step 9 |
| MD25 | SPARK-59909 error message | ch52 note and Step 3 |
| MD26 database_source | when JDBC vs Python source, dialect list (re-checked for 4.3.0) | ch51 note, Step 10 |
| MD26 | SQLite affinity demo, `FLOATING POINT`, `PRAGMA table_info`, Decimal bind, connections do not pickle | ch51 Step 10 |
| MD26 | `to_spark_value`, NULL for misfits | ch51 Step 10 |
| MD26 | JDBC with `sqlite-jdbc` comparison | ch51 Step 10 |
| MD26 | read-only URI, `fetchmany`, close in `finally` | ch51 Step 10 |
| MD26 | JDBC key ranges with Derby, `whereClause()`, bounds not a filter, NULLs, `columnPartition`, `predicates` | ch51 Step 11 |
| MD26 | ranges with `?` params, rowid alias, quantile bounds, alternatives | ch51 Step 11 |
| MD26 | SQL injection via pushdown, `quote_ident`, `sqlite_master` check | ch52 Step 6 |
| MD26 | semantic mismatch table and `typeof` guard | ch52 Step 7 |
| MD26 | filter value types (Decimal, `id > 1.5`, UTC datetimes, `NOT`, `LIKE 'a%b'`) | ch52 Step 2 |
| MD26 | what not to push | ch52 Step 7 |
| MD26 | no column/limit/aggregate pushdown vs JDBC | ch52 Step 7 and note |
| MD26 | plan reuse through a temp view | ch52 Step 8 |
| MD26 | consistency across partitions, snapshot mark, backup, `pg_export_snapshot`, replica | ch51 Step 12 |
| MD26 | production notes (credentials pickled, Arrow batches) | ch51 Steps 5, 8 |
| MD27 writers | write path table, `WriterCommitMessage`, `TaskContext` | ch53 Step 1 |
| MD27 | write-only source | ch53 Step 1 |
| MD27 | `write()` must return a message, `NOT_IMPLEMENTED` | ch53 Step 2 |
| MD27 | only append/overwrite, overwrite is a boolean, `.save(path)` | ch53 Step 2 |
| MD27 | `abort()` gets `None`, job id from `__init__`, hidden files | ch53 Steps 3, 4 |
| MD27 | manifest publish and its order | ch53 Step 3 |
| MD27 | caveats: concurrency, S3 rename, table formats | ch53 Step 7 |
| MD27 | `DataSourceArrowWriter`, `maxRecordsPerBatch` 50 -> 50 + 17 | ch53 Step 6 |
| MD27 | task attempts, speculative execution (lesson weak spot, added) | ch53 Step 5 |
| MD28 streaming_sources | micro-batch loop table and diagram | ch53 Step 8 |
| MD28 | offsets as JSON, `latestOffset == start` | ch53 Step 8 |
| MD28 | checkpoint layout, offset design alternatives, `"v": 1` | ch53 Step 9 |
| MD28 | admission control, limit classes, `ReadAllAvailable` | ch53 Steps 8, 10 |
| MD28 | `SupportsTriggerAvailableNow`, with vs without | ch53 Step 10 |
| MD28 | restart continues at the next batch id | ch53 Step 9 |
| MD28 | `DataSourceStreamWriter`, `DataSourceStreamArrowWriter`, same class as source and sink | ch53 Step 9 |
| MD28 | replay by deleting `commits/N`, idempotent commit | ch53 Step 9 |
| MD28 | crash-safe commit (lesson weak spot, fixed with a folder rename) | ch53 Step 9 |
| MD28 | stateless `read()`, cleanup in `commit(end)` | ch53 Step 8 |
| MD28 | `SimpleDataSourceStreamReader`, `SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE` | ch53 Step 11 |
| MD28 | progress via attributes | ch53 (scripts use `p.batchId`, `p.numInputRows`); the Connect dict-access difference is left out, not re-tested |
| MD28 | memory sink refuses recovery (`_LEGACY_ERROR_TEMP_1299`) | left out: not re-tested on 4.3.0 |
| MD28 | `reportLatestOffset()` | left out: named in the lesson only, not exercised |
| MD28 | Real-time mode (4.3 re-check) | ch53 Step 10 |
| MD29 packaging_testing | `pyspark_*` + `DefaultSource` lookup, src layout, `pip install -e` | ch54 Step 1 (a real package, installed and tested; the lesson only described it) |
| MD29 | workers' Python, `DATA_SOURCE_NOT_FOUND`, `ModuleNotFoundError`, by value vs by reference | ch54 Step 2 |
| MD29 | install on executors and Connect server, lookup once per application | ch54 Steps 2, 5 |
| MD29 | one broken import disables all | ch54 Step 3 |
| MD29 | registration rules, SPARK-51919 override, `DATA_SOURCE_ALREADY_EXISTS` | ch54 Step 4 |
| MD29 | unit tests with Spark's checks, error text table, pytest | ch54 Step 6 |
| MD29 | two error surfaces, `getCondition()` gaps by mode, regex fallback | ch54 Step 7 |
| MD29 | worker logs, columns, `print` as `stdout`, levels, asynchronous logs | ch54 Step 8 |
| MD29 | ship checklist | ch54 Step 9 |
| MD29 | Python sources in Declarative Pipelines (SPARK-59135) | left out: not reproduced on 4.3.0; see the open findings |
| WN06 python_data_sources | batch skeleton, pickling rule, options, batches | ch50, ch51 |
| WN06 | pushdown contract, classes, no OR, 4.2 flag traps | ch52 |
| WN06 | streaming reader, admission control, `ReadMaxRows` etc., AvailableNow | ch53 Steps 8, 10 |
| WN06 | `SimpleDataSourceStreamReader` | ch53 Step 11 |
| WN06 | writers, Arrow writers, version offsets, override registration | ch53, ch54 Step 4 |
| WN06 | docstring bug `limit.maxRows` vs `max_rows` | left out: a 4.2.0 doc detail; the book uses `limit.max_files` from the 4.3.0 class |

## Claim-to-source table

| Claim | Source |
|---|---|
| Python DS code changed between v4.2.0 and v4.3.0 only by SPARK-58019 follow-up and a docstring fix | `git diff --stat v4.2.0 v4.3.0 -- python/pyspark/sql/datasource.py python/pyspark/sql/worker/ sql/.../v2/python/` and `git log` |
| Single-task execution does not apply to Python sources | `code/ch50/output/feather_check.txt` |
| Methods run in workers, PIDs, plan reuse | `code/ch50/output/where_it_runs.txt` |
| Error moments and conditions | `code/ch50/output/counter.txt` |
| Byte balancing 21,266 / 21,261 / 21,248 | `code/ch50/output/docs_source.txt` |
| No column pruning for Python sources | `docs_source.txt`; `PythonScanBuilder.scala` (no `SupportsPushDownRequiredColumns`) at v4.3.0 |
| Connect: `schema()` at `df.columns`, session cwd | `code/ch50/output/on_connect.txt` |
| 7 requests for the first count, 6 for the second, 0 after cache | `code/ch51/output/api_source.txt` |
| `local[4,3]` retries a task | `code/ch51/output/api_task_retries.txt`; `MAX_LOCAL_TASK_FAILURES = 1` in `SparkContext.scala` |
| Conversion table, `1.5` -> `1`, bare `AssertionError` in `conversion.py` | `code/ch51/output/arrow_batches.txt` |
| Tuples 1.29 s, Row 3.32 s, batches 0.22 s (medians of 5 after warm-up, 2,000,000 rows, 4 partitions, local[*] 16 cores, under the bench lock) | `code/ch51/output/arrow_timing.txt` |
| Naive datetimes take the worker time zone | `arrow_batches.txt` |
| SQLite affinity results, JDBC comparison | `code/ch51/output/database_source.txt`, `jdbc_sqlite.txt` |
| JDBC dialect list | `JdbcDialects.scala` at v4.3.0 |
| JDBC ranges [1001, 0, 0, 2], quantile [301, 302, 302, 302] | `database_source.txt` |
| 1209 / 1208 rows with and without the snapshot mark | `database_source.txt` |
| Flag internal, default false | `SQLConf.scala` at v4.3.0 line 6876 |
| Offered filters table, `explain()` calls `pushFilters()` | `code/ch52/output/what_arrives.txt` |
| Filters matched by identity, `DATA_SOURCE_EXTRANEOUS_FILTERS` | `what_arrives.txt`; `FilterRef` in `data_source_pushdown_filters.py` |
| Rows, opened files, Spark Filter | `code/ch52/output/files_pushdown.txt` |
| Injection 1207 vs 1, mismatch table, guarded SQL | `code/ch52/output/sqlite_pushdown.txt` |
| Shared planned read: 5 instead of 26, union 10 instead of 31, view 329 instead of 1207 | `files_pushdown.txt`, `sqlite_pushdown.txt`; `PythonScanBuilder.pushFilters` calls `ds.setReadInfo`, `PythonBatch` calls `getOrCreateReadInfo` |
| SPARK-58551 (limit pushdown) not in v4.3.0; SPARK-59909 in v4.3.0 | `git log v4.3.0 --grep`, `git log --all --grep` |
| Writer call trace, manifest, abort `[None, None, None]`, save modes | `code/ch53/output/writers.txt` |
| Retry with `local[4,2]`: 6 staged, 3 committed | `code/ch53/output/writers_retry.txt` |
| Arrow writer batches [67] / [50, 17] | `writers.txt` |
| Stream calls, checkpoint, restart, replay, AvailableNow with/without, RTM refusal, simple reader | `code/ch53/output/streaming.txt`; `RealTimeModeAllowlist.scala` at v4.3.0 |
| Lookup code, `PYSPARK_PYTHON` default `python3` | `lookup_data_sources.py`, `pyspark/core/context.py` line 341 at v4.3.0 |
| Package found / not found, broken package hides all, Connect artifacts order, conditions, worker logs | `code/ch54/output/packaging.txt` |
| `register()` captures python includes at registration | `UserDefinedPythonDataSource.scala` (`pythonIncludes = dataSourceCls.pythonIncludes`), `PythonPlannerRunner.scala` at v4.3.0 |

## Environment

- `pyspark[connect]==4.3.0`, `pyarrow` 26.0.1, Python 3.12, uv project in `code/`.
- JDBC comparison: `org.xerial:sqlite-jdbc:3.50.3.0` from Maven Central (`spark.jars.packages`). Derby: the JAR that ships in PySpark.
- SQLite 3.45.1 (the `sqlite3` module of the Python build).
- Chapter 54: two venvs made by `ch54/packaging.sh` (`with`: `pyspark[connect]==4.3.0`, `pytest`, `pyspark-mailbox` editable; `bare`: `pyspark[connect]==4.3.0`, `pytest`).
- Ports: `SPARK_UI_PORT=4029`. Connect server (ch50 `server.sh`, ch54 `packaging.sh`): gRPC 10290, UI 4129, `SPARK_IDENT_STRING` `part09a` / `part09a-ch54`. The earlier draft used 15059 / 4059; changed to the assigned ports.
- Local mode, 16 cores. The one quoted timing (`arrow_batches.py --timing`) ran under `flock ~/spark-masterclass/data/.bench.lock`. Films and other runs ran without the lock, per the updated brief.

## Open findings

1. **Shared planned read gives wrong results with filter pushdown (4.3.0).** `PythonDataSourceV2` keeps one `readInfo` per relation. `PythonScanBuilder.pushFilters()` replaces it with `ds.setReadInfo(...)`, and a scan without pushed filters uses whatever is stored. Minimal repro (classic, `spark.sql.python.filterPushdown.enabled=true`, any reader that accepts `EqualTo`): `df = spark.read.format("files").load(); df.filter("kind = 'image'").count()  # 5; df.count()  # 5, expected 26`. Self-union `d.union(d.filter(...)).count()` gives 10, expected 31. A temp view gives 329 for `SELECT count(*)`, expected 1207. Scripts: `code/ch52/files_pushdown.py`, `code/ch52/sqlite_pushdown.py`. Candidate JIRA; not filed. Check against master before filing (the SparkTutor lesson claimed a fix on master; not verified here).
2. **Bare `assert` in PySpark type converters.** A wrong Python type for `decimal`, `timestamp`, `timestamp_ntz` or `binary` fails with `AssertionError` and no message, from `pyspark/sql/conversion.py` (`convert_decimal` and others). No column or value in the error. Also, a `float` for an `int` column truncates silently (`1.5` -> `1`). Repro: `code/ch51/arrow_batches.py`. Error-message quality issue; not filed.
3. **`addArtifacts()` after `register()` is not seen by a Python data source on Connect.** The include list is fixed at registration. The read works after a second `register()`. Repro: `code/ch54/packaging.sh` section 5. Doc gap (the order is not documented); not filed.
4. **`getCondition()` gaps (4.3.0).** Classic: `DATA_SOURCE_NOT_FOUND` arrives as `Py4JJavaError` with no condition. Connect: `PYTHON_EXCEPTION` from `read()` has `getCondition() == None`. Repro: `code/ch54/registration.py` (classic and connect modes).
5. **`DATA_SOURCE_PUSHDOWN_DISABLED` fails every query** of a reader that defines `pushFilters()` while the flag is off (SPARK-55087 per the lesson; status not re-checked).
6. **Not verified:** SPARK-59135 (Python DS inside Declarative Pipelines makes `spark-pipelines` exit 1). The commit named SPARK-59135 in the local clone is "[CONNECT][PYTHON] Handle metadata in command response streams" and is not in v4.3.0. Not reproduced; left out of the book. SPARK-59547 (asynchronous worker logs): in the 4.3.0 run, the first read had all rows; the book says the run neither confirms nor excludes it.

## Not verified

- Behavior on a real cluster (task retries, executor time zones, `spark.archives`) is described from the source and docs; all runs are local mode.
- PostgreSQL snapshot export is described from the PostgreSQL docs, not run.
- Speculative execution is described, not run.

## Clips

| Clip | Shows |
|---|---|
| `ch50-where`, `ch50-balance`, `ch50-errors` | from the earlier render of `ch50.tape` |
| `ch51-requests`, `ch51-ranges`, `ch51-timezone` | `ch51.tape` |
| `ch52-offered`, `ch52-opened`, `ch52-shared` | `ch52.tape` |
| `ch53-commit`, `ch53-stream` | `ch53.tape` |
| `ch54-workers`, `ch54-logs` | `ch54.tape` |

## Nav entries

```yaml
      - "47. Python data sources I: readers and partition planning": chapters/ch50-python-data-sources-readers.md
      - "48. Python data sources II: APIs, databases and Arrow batches": chapters/ch51-python-data-sources-apis-databases.md
      - "49. Python data sources III: filter pushdown": chapters/ch52-python-data-sources-pushdown.md
      - "50. Python data sources IV: writers and streaming": chapters/ch53-python-data-sources-writers-streaming.md
      - "51. Shipping a source: packaging, testing and debugging": chapters/ch54-shipping-a-python-data-source.md
```

Chapter 54 links to Chapter 55 through `../roadmap.md`, because the Chapter 55 file is on the part-09b branch. The integrator can change that link to the real file.
