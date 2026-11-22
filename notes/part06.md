# Part VI notes: Pipelines and tables

Chapters (old numbers in the file names; see `notes/renumber.md`: 29 becomes 30, 30-34 become 32-36):

| File | Title | Final number |
|---|---|---|
| `ch29-tables-delta-iceberg.md` | Tables: Delta and Iceberg, MERGE and time travel | 30 |
| `ch30-declarative-pipelines.md` | Declarative Pipelines: the model and the CLI | 32 |
| `ch31-medallion.md` | Medallion in practice: bronze, silver, gold and quality | 33 |
| `ch32-change-data-capture.md` | Change data capture: reading changes and Auto CDC | 34 |
| `ch33-metric-views.md` | Metric views: a semantic layer in Spark SQL | 35 |
| `ch34-pipeline-end-to-end.md` | A pipeline end to end, and how SDP runs it | 36 |

**Cross-reference warning for the integrator.** In `ch29`, "Chapter 31" means the new Iceberg chapter (final 31, another writer); do not remap it. In `ch30` to `ch34`, plain "Chapter 29" to "Chapter 34" are old numbers and follow the map. Links between my chapters use file names (`ch31-medallion.md` and so on); rename the files and the links together.

## Technical names and technical verbs (STE)

Technical names: Spark Declarative Pipelines (SDP), pipeline, dataset, flow, append flow, materialized view, streaming table, temporary view, sink, spec, `spark-pipelines`, CLI, Spark Connect, server, client, catalog, Hive metastore, Iceberg, Delta Lake, table format, checkpoint, micro-batch, event log, event, bronze, silver, gold, medallion, quarantine, gate, check, rule, profile, Auto CDC, SCD Type 1, SCD Type 2, change feed, changelog, tombstone, auxiliary table, run (of SCD2 events), metric view, dimension, measure, grain, semantic layer, running total, window, frame, carry-over, sketch.

Technical verbs: run, dry-run (as the CLI command), register, resolve, materialize, truncate, commit, append, merge, refresh, full-refresh, checkpoint (noun only), cast, aggregate, render (films), import (a Python file), parse.

STE results (final, `scripts/ste-check`):

| Chapter | Findings | Kept on purpose |
|---|---|---|
| ch29 | 0 | |
| ch30 | 0 | |
| ch31 | 2 | `running` in "running total" (a technical name of a window result, two places) |
| ch32 | 1 | `is between` flagged as passive: a false positive (the verb "is" with a preposition) |
| ch33 | 1 | `were created` inside the quoted title of SPARK-60206 (literal text) |
| ch34 | 0 | |

## Coverage

| Lesson | Topic | Where |
|---|---|---|
| LS09 data_lakes | databases, warehouses, lakes, lakehouses (history) | left out: generic history; ch29 opens with the folder-versus-table problem instead |
| LS09 | Delta, Iceberg, Hudi; connector release lag | ch29 note on versions, side-by-side table; Hudi/Paimon named as not tested |
| LS09 | adding Delta (`--packages`, extension, catalog, `configure_spark_with_delta_pip`) | ch29 Step 2 |
| LS09 | ACID, optimistic concurrency | ch29 Steps 3, 9, 11 |
| LS09 | schema enforcement/evolution, `mergeSchema`, `INSERT WITH SCHEMA EVOLUTION`, `MERGE WITH SCHEMA EVOLUTION` | ch29 Steps 7, 10 |
| LS09 | time travel (`VERSION AS OF`, `TIMESTAMP AS OF`, options, Iceberg `snapshot-id`) | ch29 Steps 5, 9 |
| LS09 | MERGE incl. `WHEN NOT MATCHED BY SOURCE`, `mergeInto`, `REPLACE WHERE`, `REPLACE USING` | ch29 Step 10 |
| LS09 | Delta change data feed | ch29 Step 6, ch32 Step 3 |
| LS09 | Spark 4.2 CDC API, `loadChangelog`, `UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE` | ch32 Step 2 |
| LS09 | SDP Auto CDC | ch32 Steps 4 to 9 |
| LS09 | maintenance (`OPTIMIZE`, `VACUUM`, `expire_snapshots`, history) | ch29 Steps 5, 8; Iceberg maintenance in the Iceberg chapter (31) |
| MD18 landing_in_delta | Parquet folder trap | ch29 Step 1 |
| MD18 | `_delta_log` actions, commit protocol, read only as `delta` | ch29 Steps 3, 4 |
| MD18 | idempotent bronze with insert-only MERGE, MERGE source duplicates, `INSERT *` drops columns | ch29 Steps 7, 8 |
| MD18 | enforcement table, constraints, `CHECK` and NULL | ch29 Step 7 |
| MD18 | time travel time zone, history, RESTORE, VACUUM, CDF | ch29 Steps 5, 6 |
| MD18 | streaming into Delta | left out: Delta does not run on 4.3; streaming sinks are Part (streaming) material |
| MD18 | `mergeInto` with path tables `INTERNAL_ERROR` | left out: Delta-on-4.2 specific, Delta not runnable on 4.3 |
| MD34 production_ingestion | read as strings under ANSI | ch31 Steps 2, 8 |
| MD34 | profiling: `summary()` trap, one-pass profile, `approx_count_distinct` rsd, `mode`, `percentile_approx`, HLL sketches | ch31 Step 8 (`approx_top_k`, theta, KLL named only in passing: not covered) |
| MD34 | rules as data, `failed_rules`/`warnings` | ch31 Step 9 |
| MD34 | three-valued logic trap | ch31 Step 9 |
| MD34 | dataset checks (uniqueness, referential integrity, freshness, volume) | ch31 Steps 3, 5, 9 (freshness and volume: left out) |
| MD34 | `observe`, `Observation`, `NO_OBSERVE_BEFORE_GET`, streaming variant | ch31 Step 9 |
| MD34 | quality gate, `raise_error`/`assert_true`, gate that never ran | ch31 Step 10 |
| MD34 | Delta idempotency toolkit (replaceWhere, MERGE after dedupe, txnVersion, userMetadata, CHECK) | ch29 Steps 7, 8 |
| MD34 | SDP mapping without expectations, failed flow and SKIPPED downstream | ch31 Steps 5, 10 |
| MD34 | `withColumn` dependency loss, empty silver after failed refresh | ch31 Steps 3, 10 (both re-tested on 4.3.0, both still present) |
| SDP01 why_declarative | imperative vs declarative | ch30 Steps 1, 2 |
| SDP01 | datasets, flows, pipelines | ch30 Step 2 |
| SDP01 | `spark` injection, session mutation, builder trap, calls at registration, `ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION` | ch30 Steps 4, 5 |
| SDP01 | always on Connect | ch30 Step 4 |
| SDP02 datasets_and_flows | decision table, decorator options | ch30 Step 6 |
| SDP02 | `create_streaming_table` + `append_flow`, SQL `CREATE FLOW ... BY NAME` | ch30 Step 7 |
| SDP02 | checkpoint path, available-now, in-memory catalog `LOCATION_ALREADY_EXISTS`, Hive fix, rename abandons checkpoint, full refresh, MV no checkpoint | ch30 Steps 9, 10 |
| SDP02 | multi-flow race into Parquet | ch30 Step 7 (re-tested: still loses rows on 4.3.0, also with one flow at a time; Iceberg is correct) |
| SDP03 pipeline_projects | `init` layout, spec fields, errors, path relativity | ch30 Step 3 |
| SDP03 | CLI commands, refresh selection, spec discovery | ch30 Steps 3, 10 |
| SDP03 | wrapper over spark-submit, argument sorting | ch30 Step 4 |
| SDP03 | `--packages` jars not visible | not re-tested on 4.3 (the book uses `--jars`); stated as advice only |
| SDP03 | SQL files, no PIVOT, sinks Python-only, no `read_files` | ch30 Step 6 (SQL files); `read_files` and PIVOT: not re-tested, not claimed |
| SDP03 | dry-run exercise (cycle, name, column) | ch30 Step 8 with real error text |
| SDP04 bronze_ingestion | bronze contract, schema, strings, `_metadata` | ch31 Step 2 |
| SDP04 | file options, available-now, identified by path | ch31 Step 2 |
| SDP04 | Kafka bronze | ch31 Step 2 note (not run) |
| SDP05 silver_quality | `try_cast`, `try_to_timestamp`, window dedupe, quarantine, conservation | ch31 Step 3 |
| SDP05 | `dropDuplicates`/`drop` registration failure, SPARK-59663 | ch31 Step 3 (both re-tested, both still present) |
| SDP05 | no expectations, public API list, aggregate-then-assert | ch31 Step 5 |
| SDP05 | streaming silver | ch31 Step 6 (design text) |
| SDP06 gold_aggregates | shared business rule view, windows, rank functions, single-partition window | ch31 Step 4 |
| SDP06 | loop datasets, default-arg binding, list only grows, no `collect()` at module scope | ch31 Step 4 |
| SDP06 | MV full recompute, metric views not in pipeline SQL | ch31 Step 6, ch33 Step 10 |
| SDP07 auto_cdc | apply vs append, out-of-order example | ch32 Step 1 |
| SDP07 | `create_auto_cdc_flow` parameters, keys in state | ch32 Step 5 |
| SDP07 | target must support row-level ops, `AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE` | ch32 Step 4 |
| SDP07 | test catalog workaround | replaced: the book uses the Iceberg build for 4.3 as a real target |
| SDP07 | Delta CDF and Kafka sources | ch32 Step 10 (text; Delta not runnable on 4.3) |
| SDP07 | reserved metadata column, tombstones, aux table | ch32 Steps 6, 9 |
| SDP07 | 4.3 SCD Type 2, track history | ch32 Steps 5 to 9 |
| SDP08 capstone | full pipeline with sink, reconciliation checks | ch34 |
| SDP08 | running for real, Hive, full refresh, production server, scheduling, CI | ch30 Step 9, ch34 |
| SDP09 under_the_hood | toy regex SDP | replaced, as the inventory asks, by the real source path in ch34 |
| WN08 cdc_nearest_metrics | CDC read surface and options, why it fails, pushdown, streaming lag, netChanges caveat | ch32 Step 2 |
| WN08 | `changes_between()` snapshot diff | ch32 Step 3 |
| WN08 | Auto CDC | ch32 |
| WN08 | NEAREST BY, `time_bucket` | left out: not in my scope (joins and date-time chapters, per inventory placement) |
| WN08 | metric views: YAML, `measure()`, grain-correct ratios, missing `measure()` error, no TEMP, `DESCRIBE` shows `METRIC_VIEW` | ch33 Steps 1 to 8 |

## Claim-to-source table

| Claim | Source |
|---|---|
| Delta 4.4.1 fails on Spark 4.3.0 with `NoSuchMethodError` (`LogicalRDD.<init>`) | `code/ch29/output/delta_on_43.txt` |
| Delta and Iceberg log contents, MERGE metrics, time travel, VACUUM, checkpoints, CDF, enforcement, idempotency, OPTIMIZE, concurrency | `code/ch29/output/delta_log.txt`, `iceberg_log.txt`, `parquet_folder.txt` |
| Spark 4.3 `@v<id>` time travel syntax (SPARK-57421, SPARK-58012) | ch29 author's check; `iceberg_log.txt` shows it working |
| Spec keys and required keys | `python/pyspark/pipelines/cli.py` `unpack_pipeline_spec` (v4.3.0) |
| Spec errors | `code/ch30/output/spec_errors.txt` |
| `init` layout | `code/ch30/output/init.txt` |
| Wrapper argument rules, `--remote local`, `spark.api.mode=connect` | `core/src/main/scala/org/apache/spark/deploy/SparkPipelines.scala` |
| Query function called once at registration; `spark` injected | `code/ch30/output/run_default.txt`, `session_rules.txt`; `cli.py` `register_definitions` |
| Session mutation errors, builder trap, action error | `code/ch30/output/session_rules.txt` |
| Decorator options | `python/pyspark/pipelines/api.py` |
| Multi-flow loses rows on Parquet (11 of 13), also serial (11 and 2 of 12), Iceberg correct (13, 12) | `code/ch30/output/flows.txt`; `FileStreamSink.addBatch` |
| Dry-run error order, cycle detection among failed flows | `code/ch30/output/dry_run_errors.txt`; `GraphValidations.validateSuccessfulFlowAnalysis` |
| `LOCATION_ALREADY_EXISTS` on second run, in-memory catalog | `code/ch30/output/run_default.txt` |
| Checkpoint path and numbered version folder, new version on full refresh | `code/ch30/output/storage.txt`, `refresh.txt`; `SystemMetadata.scala` |
| `EXCLUDED` flows, `CONFLICTING_PIPELINE_REFRESH_OPTIONS` | `code/ch30/output/refresh.txt` |
| Bronze reads only the new file in run 2 (11 then 4 rows) | `code/ch31/output/two_runs.txt` (event log section) |
| MVs recomputed, gold numbers | `code/ch31/output/two_runs.txt` |
| `PipelineEvent` has only timestamp and message | `sql/connect/common/src/main/protobuf/spark/connect/pipelines.proto` |
| Profile, approx distinct 37 vs 40, NULL trap 2 vs 4, observe counts, gate pruned | `code/ch31/output/quality_rules.txt` |
| Gate failure, 3 attempts, SKIPPED, silver 103 -> 0 in Parquet and Iceberg, last good snapshot 103 | `code/ch31/output/gate.txt`; `DatasetManager.materializeTable` (`TRUNCATE TABLE`), `maxFlowRetryAttempts` default 2 in `SQLConf.scala` |
| `dropDuplicates`/`drop` and `withColumn` still fail on 4.3.0 | `code/ch31/output/variants.txt` |
| Spark 4.3 archive reads (SPARK-58110, SPARK-58246, related) | git log v4.2.0..v4.3.0; not tested |
| CDC read API fails on session catalog and Iceberg build | `code/ch32/output/read_changes.txt`; `TableCatalog.loadChangelog` default |
| Changelog contract (pushdown, streaming lag, netChanges) | `Changelog.java` (v4.3.0) |
| Auto CDC SCD2 and SQL AUTO CDC are new in 4.3 | v4.2.0 `api.py` accepts only SCD 1, v4.2.0 grammar has no `autoCdc`; commits SPARK-56249, SPARK-58261, SPARK-58247 |
| Auto CDC target needs `SupportsRowLevelOperations` | `code/ch32/output/parquet_target.txt` |
| SCD1/SCD2 results for three files | `code/ch32/output/drops.txt` |
| Runs and hidden rows of SCD2 | `Scd2BatchProcessor.scala` comments |
| Aux tables, tombstones, snapshots | `code/ch32/output/internals.txt`; `AutoCdcAuxiliaryTable.scala` |
| Wrong `sequence_by` error | `code/ch32/output/dry_seq.txt` |
| Metric view numbers (6.3158, 10.9201, 7083, 258, 6.3138, 256, 11.5498, 7023) | `code/ch33/output/two_errors.txt`, `grains.txt` |
| YAML keys, version 0.1, source parse order | `MetricViewCanonical.scala`, `MetricViewSerDeBase.scala` |
| What Spark stores | `code/ch33/output/define.txt` |
| `MEASURE()` rewrite | `ResolveMetricView.scala`; plans in `grains.txt` |
| Definition and query rules | `code/ch33/output/rules.txt` |
| Lifecycle results | `code/ch33/output/lifecycle.txt` |
| Unqualified source resolves in current database (SPARK-60206 not in 4.3.0) | `lifecycle.txt`; `git merge-base --is-ancestor decdde7153b v4.3.0` is false |
| Metric views new in 4.2, V2 catalogs in 4.3 (SPARK-54119) | git log v4.2.0..v4.3.0 |
| Metric view in pipeline SQL | `code/ch33/output/in_pipeline.txt` |

## Environment

- `pyspark[connect]==4.3.0` (the book environment, `code/pyproject.toml`), Java 17.0.21, Python 3.12.
- Delta: `io.delta:delta-spark_4.2_2.13:4.4.1` with `pyspark==4.2.0` (`uv run --no-project --with pyspark==4.2.0 --with delta-spark==4.4.1`). No Delta release for Spark 4.3 on the date of writing.
- Iceberg: unreleased runtime `iceberg-spark-runtime-4.3_2.13` built from apache/iceberg PR #18749 at commit `b1eab64` ("Spark: Initial support for 4.3.0"), at `$BOOK_DATA/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar`. Iceberg 1.12.0 supports Spark up to 4.1. sha256 `f915c7b075e58012f6a9ee2e34dffcccac9a5715816e01738ff5560efe6dc4d5`. The earlier Part VI writer built it; the build steps were not recorded in this worktree.
- Ports: Connect 10260, Spark UI 4026 (Classic) and 4126 (Connect server), through `CONNECT_PORT` and `SPARK_UI_PORT` for `code/common/pipeline.sh`.
- Pipelines run with `spark.sql.catalogImplementation=hive` (Derby `metastore_db/` in each work folder) unless the text says the in-memory catalog.
- Data: `code/data/retail` (with new `backfill/` and `later/` folders), `code/data/quality`, `code/data/crm` (with new `drop3/`), NYC taxi June 2026 from `$BOOK_DATA/nyc-taxi`.
- Films: VHS through the film-demo kit, `--format chapter`, rendered without the bench lock (no timings are quoted in Part VI).

## Open findings (possible Spark bugs or doc gaps, unfiled)

1. **SDP: two flows into one streaming table of the session catalog lose rows** (4.3.0). The file sink skips a batch ID that another flow already committed to the shared `_spark_metadata` log. Concurrent: one flow fails, the retry "completes", 2 of 13 rows missing. Serial (`maxConcurrentFlows=1`): no error at all, rows missing in both tables. Repro: `code/ch30/flows.sh`. SDP should reject a second flow into a file-sink table, or give each flow its own sink log.
2. **SDP: a failed or skipped materialized view is left empty** (4.3.0). `DatasetManager.materializeTable` runs `TRUNCATE TABLE` on each MV before the flows start; a failed flow (gate, data error) or a SKIPPED downstream flow leaves 0 rows, in Parquet and in Iceberg. Repro: `code/ch31/gate.sh`. At least a doc gap; arguably the truncate should be part of the flow's write.
3. **SPARK-59663 still present in 4.3.0**: `withColumn` over a pipeline dataset gives stale data in the second run against a persistent catalog. Repro: `code/ch31/variants.sh` (`variants/with_column.py`), silver stays at 7 rows when bronze has 15.
4. **`dropDuplicates` / `drop` on a pipeline dataset fail at registration** with `TABLE_OR_VIEW_NOT_FOUND` (4.3.0). Repro: `code/ch31/variants.sh` (`variants/dedupe.py`).
5. **Metric views, internal errors** (4.3.0 Classic): `SELECT *` -> `INTERNAL_ERROR` (planning); `MEASURE(<dimension>)` -> `NoSuchElementException: key not found: ExprId`; a join of a metric view to a table -> `INTERNAL_ERROR UnevaluableAggregateFunc.aggBufferAttributes`. Repro: `code/ch33/rules.py`.
6. **Metric views: `MEASURE()` in `ORDER BY` / `HAVING`** fails with a misleading `UNRESOLVED_COLUMN` (4.3.0). Repro: `code/ch33/rules.py`.
7. **Metric views: `CREATE OR REPLACE VIEW ... WITH METRICS` fails with `TABLE_OR_VIEW_ALREADY_EXISTS`** on 4.2.0, 4.2.1 and 4.3.0 (Classic, in-memory and Hive catalogs). Repro: `code/ch33/lifecycle.py`; minimal script used: create a table, `CREATE VIEW mv WITH METRICS ...`, then `CREATE OR REPLACE VIEW mv WITH METRICS ...`. Not checked over Connect.
8. **Metric views: `DROP TABLE` drops a metric view** in the Hive catalog (4.3.0) without `WRONG_COMMAND_FOR_OBJECT_TYPE`. Repro: `code/ch33/lifecycle.py`.
9. **Metric views: a definition with no dimensions and no measures is accepted**, and a query of it reads the source. Repro: `code/ch33/rules.py`.
10. **SPARK-60206** (fix on Lisa's branch, not in 4.3.0): unqualified metric view source resolves in the querying session's current database. Repro: `code/ch33/lifecycle.py` (5 rows instead of 3,694,387).
11. **Doc gap**: the `PipelineEvent` proto carries only `timestamp` and `message`; OSS has no persisted pipeline event log. Users who need run history must save CLI output or the Spark event log.

## Not verified

- `--packages` invisibility for pipeline code (SDP03): not re-tested on 4.3.
- `read_files` absence and no PIVOT in SQL pipelines: not re-tested; the chapters do not claim them.
- Kafka bronze and Delta CDF as Auto CDC sources: text only.
- Archive reads in 4.3: named from the commit log, not run.

## Film

| Tape | Beats | Clips |
|---|---|---|
| `code/film/ch29.tape` | delta (Delta log after the MERGE), iceberg (snapshot tree) | `ch29-delta`, `ch29-iceberg` |
| `code/film/ch30.tape` | dryrun (cycle, then the wrong name), run (event stream of a run) | `ch30-dryrun`, `ch30-run` |
| `code/film/ch31.tape` | incremental (event-log progress of two runs), gate (report and empty silver) | `ch31-incremental`, `ch31-gate` |
| `code/film/ch32.tape` | history (SCD Type 2 after three files) | `ch32-history` |
| `code/film/ch33.tape` | measure (average of ratios, then `MEASURE()`) | `ch33-measure` |
| `code/film/ch34.tape` | trace (pipeline commands of a dry run), server (server log of run 2) | `ch34-trace`, `ch34-server` |

Storyboard (VO drafts, checked against the outputs):

| Beat | On screen | VO (draft) |
|---|---|---|
| ch29 delta | `_delta_log` listing, actions of version 1, add and remove | Version one was the MERGE. It added one file with five rows and removed the old file with three. Nothing changed in place. |
| ch29 iceberg | version hint, snapshot list with parents | Iceberg keeps a tree. Each snapshot points to its parent: one append, then two overwrites. |
| ch30 dryrun | two datasets that read each other, dry run, fix, dry run | A dry run reads no data. It finds the cycle first, then the wrong table name, before anything is written. |
| ch30 run | flows RUNNING and COMPLETED | Each flow starts only when its inputs are complete. Products and orders run at the same time; the summaries wait. |
| ch31 incremental | progress.py over two event logs | The second run read only the new file: four rows in batch one, from the checkpoint of the first run. |
| ch31 gate | quality report, empty silver | Day three broke the gate. The report shows why, and silver is empty, because the run truncated it before the gate failed. |
| ch32 history | customers_history | Ada moved from Toronto to Ottawa to Hanoi. The Ottawa event came last, and Auto CDC still put it in the middle. |
| ch33 measure | film.py | Averaging the borough ratios gives eleven and a half dollars a mile. The metric view gives six thirty-one, the real ratio, at every grain. |
| ch34 trace | trace.txt | The CLI only sends declarations: a graph, an output and a flow per dataset, then one start command. |
| ch34 server | server_log_short.txt | The server prepares every table first, then starts flows as their inputs finish. |

## Nav entries

```yaml
      - "Tables: Delta and Iceberg": chapters/ch29-tables-delta-iceberg.md
      - "Declarative Pipelines: the model and the CLI": chapters/ch30-declarative-pipelines.md
      - "Medallion in practice": chapters/ch31-medallion.md
      - "Change data capture and Auto CDC": chapters/ch32-change-data-capture.md
      - "Metric views": chapters/ch33-metric-views.md
      - "A pipeline end to end": chapters/ch34-pipeline-end-to-end.md
```

## Shared files changed

- `code/common/pipeline.sh` (Part VI runner; the previous Part VI writer added it) and `code/common/lake.py` (Iceberg session helper). Only Part VI uses them.
- `code/data/retail/backfill/`, `code/data/retail/later/`, `code/data/crm/drop3/`: new small data files.

Clip cutting: `ffmpeg -ss <start> -to <end> -i code/film/renders/<film>.mp4 -an -c:v libx264 -crf 28 -preset slow -pix_fmt yuv420p -movflags +faststart`, poster `ffmpeg -ss <poster_at> -i <clip>.mp4 -frames:v 1 -q:v 80 <clip>.webp` (poster time relative to the clip). Cut list: `book/media/clips/clips-part06.tsv`.
