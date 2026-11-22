# v2 notes: the Spark Declarative Pipelines part

Branch `v2-sdp`. Writer scope: make SDP its own part, with a tutorial, a CLI and project chapter, and a configuration chapter; make ch32 "the model".

## Scope and what changed

New chapters (provisional numbers):

| File | Title | Code | Media |
|---|---|---|---|
| `book/chapters/ch77-first-etl-pipeline.md` | Your first ETL pipeline | `code/ch77/` (`tutorial.sh`, `make_days.py`, `look.py`, `project/`, `figures.py`, `anim/`) | `ch77-project-layout.svg`, `ch77-two-runs.svg`, `ch77-anim-run`, `ch77-day2` |
| `book/chapters/ch78-pipelines-cli-project.md` | The spark-pipelines CLI and the project | `code/ch78/` (`cli.sh`, `spec.sh`, `extra.sh`, `local_cwd.sh`, `anim/`) | `ch78-argument-split.svg`, `ch78-client-server.svg`, `ch78-anim-refresh`, `ch78-cli` |
| `book/chapters/ch79-configuring-pipelines.md` | Configuring a pipeline | `code/ch79/` (`config.sh`, `iceberg.sh`, `flowconf.sh`, `probe/`, `anim/`) | `ch79-two-environments.svg`, `ch79-config-layers.svg`, `ch79-anim-config`, `ch79-envs` |

Shared files added: `code/common/sdp.sh` (venv, project copy, Connect server start and stop by PID, `x` and `clean` helpers; used by ch77 to ch79 and the films), `code/film/sdp-film.sh` (hidden film setup), `code/film/ch77.tape`, `ch78.tape`, `ch79.tape`, `book/media/clips/clips-sdp.tsv`. All six figures come from `code/ch77/figures.py` (one script for the part).

Existing chapters changed:

- **ch32** is now "Declarative Pipelines: the model". Removed: old Step 3 (project and spec, now ch78 Steps 3 to 6), the wrapper half of old Step 4 (now ch78 Step 1), old Step 9 (runs, Hive, storage: now ch77 Steps 6 to 9, ch78 Step 9, ch79) and old Step 10 (refresh selection: now ch78 Step 8). Kept and renumbered: imperative vs declarative (1), declared tables (2), how SDP builds and runs the graph (3, was 4), what pipeline code must not do (4, was 5), dataset types (5, was 6), flows and two flows into one table (6, was 7), graph checks with a dry run (7, was 8). Added: links to ch77 to ch79 in the opening, a short "What a run leaves behind" section with links, a note on `spark_conf` (ch79 finding). Fixed: the time-zone sentence (the CLI prints its own lines in local time and server events in UTC, `handle_pipeline_events`; the old text said the server used UTC). The removed Step 3 also held a wrong claim ("a relative path starts from the project folder"): ch78 Step 10 shows that the server resolves relative paths in its own current folder.
- **ch33**: links only. "Step 9 of Chapter 32" (Hive) now points to ch77 Step 6; "Chapter 32, Step 4" is now Step 3; "Chapter 32 showed all states" is now "Chapters 32 and 78"; `sources/0/0` now points to ch77 Step 8.
- **ch36**: links only. "Chapter 32, Step 7" is now Step 6; "Chapter 32, Step 8" (cycle order) is now Step 7; the CI dry run now points to ch78 Step 7; "Chapter 32, Step 10" (old checkpoint folder kept) now points to ch78 Step 8.
- ch34 and ch35: no change (no links into the moved steps).

Not changed, needs the integrator: `ch49-pipelines-with-agents.md` line 79 says "[Chapter 32] teaches the model and the CLI". After the move, the CLI is ch78. Chapter 74 (configurations, another writer) is named as plain text "Chapter 74" in ch79, with no link, because the file does not exist yet on this branch. Add the link when ch74 lands (ch79 intro and Step 10).

## New SDP part: order and titles

Part title suggestion: **Part: Spark Declarative Pipelines** (subtitle "*Data: NYC taxi trips and zones, SparkTutor's dirty retail set*").

| Order | File | Title suggestion |
|---|---|---|
| 1 | ch77-first-etl-pipeline.md | Your first ETL pipeline |
| 2 | ch78-pipelines-cli-project.md | The spark-pipelines CLI and the project |
| 3 | ch79-configuring-pipelines.md | Configuring a pipeline: catalogs, environments and secrets |
| 4 | ch32-declarative-pipelines.md | The pipeline model: datasets, flows and the graph |
| 5 | ch33-medallion.md | Medallion in practice: bronze, silver, gold and quality |
| 6 | ch34-change-data-capture.md | Change data capture: reading changes and Auto CDC |
| 7 | ch36-pipeline-end-to-end.md | A pipeline end to end, and how SDP runs it |

ch30 (tables), ch31 (Iceberg) and ch35 (metric views) stay in a tables or SQL part. The ch32 file title is "Declarative Pipelines: the model"; rename it to the suggestion above if the integrator agrees (the H1 is the only place).

### Nav entries

```yaml
  - "Spark Declarative Pipelines":
      - "Your first ETL pipeline": chapters/ch77-first-etl-pipeline.md
      - "The spark-pipelines CLI and the project": chapters/ch78-pipelines-cli-project.md
      - "Configuring a pipeline": chapters/ch79-configuring-pipelines.md
      - "The pipeline model": chapters/ch32-declarative-pipelines.md
      - "Medallion in practice": chapters/ch33-medallion.md
      - "Change data capture and Auto CDC": chapters/ch34-change-data-capture.md
      - "A pipeline end to end": chapters/ch36-pipeline-end-to-end.md
```

### Roadmap rows

```markdown
## Part VII: Spark Declarative Pipelines
*Data: NYC taxi trips and zones, SparkTutor's dirty retail set*

| # | Chapter | Covers |
|---|---|---|
| 77 | [Your first ETL pipeline](chapters/ch77-first-etl-pipeline.md) | From an empty folder: `pip install pyspark[pipelines]`, `spark-pipelines init`, raw taxi trips and zones, a clean streaming table and a daily summary in SQL, dry run, run, the tables and files on disk, a second day and what is incremental, a broken column and its error |
| 78 | [The spark-pipelines CLI and the project](chapters/ch78-pipelines-cli-project.md) | The wrapper over `spark-submit` and how it splits arguments, every subcommand and flag, exit codes, `init`, every spec key and its error, spec discovery, `libraries` globs, Python and SQL together, helper modules, `dry-run`, `--refresh`, `--full-refresh`, `--full-refresh-all`, where tables, checkpoints and events go, a remote Connect server |
| 79 | [Configuring a pipeline](chapters/ch79-configuring-pipelines.md) | The spec `configuration` and how it reaches the session and each flow, static configurations, CLI `--conf`, parameters in Python and SQL (`${...}`), catalog and database, dev and prod specs, paths and secrets without hard-coding, an Iceberg catalog |
| 32 | [The pipeline model](chapters/ch32-declarative-pipelines.md) | Datasets and flows, how SDP builds and runs the graph, what pipeline code must not do, dataset types, two flows into one table, graph checks |
| 33 | ... (unchanged rows for 33, 34, 36) |
```

## Technical names and technical verbs (STE)

Technical names: Spark Declarative Pipelines (SDP), pipeline, project, spec, spec file, key, glob, root directory, dataset, flow, streaming table, materialized view, temporary view, checkpoint, checkpoint folder, version folder, storage root, catalog, database, namespace, Hive metastore, Derby, Iceberg, Delta Lake, warehouse, Spark Connect, Connect server, session, client, CLI, wrapper, subcommand, flag, exit code, event, event log, dry run, full refresh, selection, `spark-submit`, `PythonRunner`, `argparse`, PyYAML, YAML, virtual environment, template, environment variable, parameter, configuration (noun; replaces "setting" everywhere), static configuration, runtime configuration, decorator, module code, query function, helper module, UserWarning, HyperFrames, VHS.

Technical verbs: run, dry-run (the command), register, resolve, import, load, commit, append, refresh, full-refresh, analyze, parse, start (a server), stop.

## STE results (`scripts/ste-check`)

| Chapter | Findings | `--allow` |
|---|---|---|
| ch77 | 0 | `programming` (the title "programming guide") |
| ch78 | 0 | `programming`, `Submitting` (the doc title "Submitting applications") |
| ch79 | 0 | `programming` |
| ch32 | 0 | none |
| ch33 | 2 (unchanged, `running` in "running total", as before) | none |
| ch36 | 0 | none |

Deliberate choices: "Thus" starts sentences, as in Part I. Code identifiers and error classes stay in code format and can make a sentence look long; each stays within 25 words. "setting" (noun) is replaced by "configuration" in ch79, and "open finding" by "open problem", to avoid `-ing` nouns.

## Coverage (SparkTutor SDP lessons, now split across ch77 to ch79 and ch32)

| Lesson | Topic | Where now |
|---|---|---|
| SDP01 | imperative vs declarative, datasets, flows, pipelines, session injection, session mutation, builder trap, calls at registration | ch32 Steps 1 to 4 |
| SDP01 | always on Connect | ch32 Step 3, ch78 Step 1 |
| SDP02 | decision table, decorator options, append flows, SQL `CREATE FLOW` | ch32 Steps 5, 6 |
| SDP02 | checkpoint path, available-now, Hive fix, full refresh, MV no checkpoint | ch77 Steps 6, 8, 9; ch78 Steps 8, 9 |
| SDP02 | in-memory catalog `LOCATION_ALREADY_EXISTS` | ch77 Step 6 (one sentence, output in `code/ch32/output/run_default.txt`) |
| SDP03 | `init` layout, spec fields, errors, path relativity | ch78 Steps 3 to 6, 10 (path relativity corrected: server folder, not spec folder) |
| SDP03 | CLI commands, refresh selection, spec discovery | ch78 Steps 2, 5, 8 |
| SDP03 | wrapper over spark-submit, argument sorting | ch78 Step 1 |
| SDP03 | `--packages` jars not visible | ch78 Step 2 table and Step 10: `--jars`/`--packages` reach only a local server (from `SparkPipelines` routing; not tested with `--packages`) |
| SDP03 | dry-run exercise | ch78 Step 7, ch32 Step 7 |
| SDP08 | running for real, production server, scheduling, CI | ch77, ch78 Steps 2, 7, 10, ch36 |
| (new) | configuration, catalogs, environments, secrets | ch79 |

## Claim-to-source table

| Claim | Source |
|---|---|
| `pyspark[pipelines]` adds PyYAML; versions installed | `python/packaging/classic/setup.py` (v4.3.0, extra `pipelines`); `code/ch77/output/01-install.txt` |
| `spark-pipelines --help`, subcommand help texts | `code/ch77/output/01-install.txt`, `code/ch78/output/help.txt`; `cli.py` `main` |
| `init` output, files, absolute `file://` storage, refuses an existing folder | `code/ch77/output/02-init.txt`, `code/ch78/output/init.txt`; `init_cli.py` |
| Trip counts per day (111,546; 126,260; 140,184) | `code/ch77/output/03-data.txt`, `10-day2.txt`, `code/ch79/output/iceberg-server.txt` |
| Run events, order of flows, exit codes 0 and 1 | `code/ch77/output/06-dry-run.txt`, `07-run1.txt`, `11-break.txt` |
| Table counts after run 1 and 2 (111,546 / 104,952 / 265 / 7; 237,806 / 223,444 / 265 / 14), borough rows | `code/ch77/output/08-look1.txt`, `10-day2.txt` |
| 6,594 rows filtered, 118,492 new clean rows, Queens fare about three times Manhattan | arithmetic on the outputs above (52.88 / 17.25 = 3.07) |
| Checkpoint layout, `sources/0/0` and `sources/0/1`, commits 0 and 1 | `code/ch77/output/09-disk1.txt`, `10-day2.txt` |
| Client lines in local time, events in UTC | `logging_utils.py` (`datetime.now()`), `spark_connect_pipeline.py` (`ToDatetime().replace(tzinfo=utc)`); outputs show the 8-hour jump |
| Analysis error leaves tables unchanged | `code/ch77/output/11-break.txt` |
| Wrapper routing rules (`--name`, `-h` to CLI; `--class` and non-connect API mode refused; `--remote local` default) | `core/src/main/scala/org/apache/spark/deploy/SparkPipelines.scala` (v4.3.0); `code/ch78/output/wrapper.txt`, `verbose.txt` |
| `--name` with `run`: argparse exit code 2 | `code/ch78/output/wrapper.txt` |
| CLI waits with no server | `code/ch78/output/exit-codes.txt` |
| Spec keys, required keys, `schema` alias, `database` wins | `cli.py` `unpack_pipeline_spec`; `code/ch78/output/spec.txt` |
| Spec errors (missing, unexpected, not dict, key not string, value not string, TypeError, RUN_EMPTY_PIPELINE, PIPELINE_STORAGE_ROOT_INVALID) | `code/ch78/output/spec.txt`, `code/ch79/output/not-string.txt`, `env-vars.txt`; error text in `error-conditions.json` (v4.3.0) |
| Guide lists `libraries` as required | `docs/declarative-pipelines-programming-guide.md` (v4.3.0) vs `REQUIRED_FIELDS` in `cli.py` |
| Spec discovery results | `code/ch78/output/discovery.txt`; `find_pipeline_spec` |
| Spec folder is the root; `../` globs | `code/ch78/output/spec-elsewhere.txt`; `register_definitions` (`change_dir`) |
| Glob forms; invalid glob; file list; README refused; `__pycache__` ignored | `validate_patch_glob_pattern`, `register_definitions`; `code/ch78/output/globs.txt` |
| Helper module import depends on the empty `PYTHONPATH` entry | `bin/spark-pipelines` (`export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"`); `code/ch78/output/helpers.txt` |
| Dry run calls `dryRunPipeline`, writes nothing, exit code 1 on error | `PipelinesHandler.scala` (v4.3.0); `code/ch78/output/dry-run.txt` |
| Refresh selections, EXCLUDED flows, checkpoint folders 0 1 2, overlap and conflict errors, unknown name accepted | `code/ch78/output/refresh.txt`; `cli.py` `run`; `PipelinesHandler.createTableFilters`; `State.reset` |
| Remote server resolves relative paths in its folder; tables land in the server folder | `code/ch78/output/remote.txt` |
| Local server resolves relative paths in the shell's folder | `code/ch78/output/local-cwd.txt` |
| `PipelineEvent` has only time and message | `pipelines.proto` (from part06 notes) |
| Spec `configuration` goes to the builder and to `CreateDataflowGraph`; flow conf = graph ++ flow | `cli.py` `run`; `spark_connect_pipeline.py`; `GraphRegistrationContext.scala` line 62 |
| Probe values with and without configuration; 1 vs 3 files | `code/ch79/output/defaults.txt`, `configuration.txt` |
| Decorator `spark_conf` did not change the write (140/140, 3/3 files) | `code/ch79/output/flowconf.txt`; `BatchTableWrite` in `FlowExecution.scala` (write in a `Future`) |
| Static conf in spec: UserWarning on the session path, CANNOT_MODIFY_STATIC_CONFIG on each flow, MVs empty | `code/ch79/output/static.txt`; `pyspark/sql/connect/session.py` `_apply_options` (`silent=True`); `StaticSQLConf.scala` |
| CLI `--conf` on a remote server: only `spark.sql.*`, spec wins | `code/ch79/output/cli-conf.txt`; `_apply_options` (filters init options to `spark.sql.`) |
| `${taxi.min_fare}` substitution in SQL files; 223,408 rows, min 2.98; 43,707 rows, min 30.01; ParseException without a value | `code/ch79/output/sql-param.txt` |
| Database must exist (`SCHEMA_NOT_FOUND`) | `code/ch79/output/database.txt`, `iceberg-server.txt`; `PipelinesHandler.createDataflowGraph` |
| dev/prod specs, 223,444 vs 223,408 rows, separate checkpoints | `code/ch79/output/envs.txt` |
| No env var expansion, envsubst, TAXI_ENV visible to module code | `code/ch79/output/env-vars.txt`; `load_pipeline_spec` (`yaml.safe_load`) |
| Iceberg catalog run, snapshots (append; delete + append), 140,184 rows | `code/ch79/output/iceberg-server.txt` |
| Catalog conf in spec works if the jar is on the server | `code/ch79/output/iceberg-spec-conf.txt` |
| `maxFlowRetryAttempts` default 2 | `SQLConf.scala` (v4.3.0) |
| CLI code largely unchanged 4.2 to 4.3 (no "New in 4.3" claimed) | `git log v4.2.0..v4.3.0 -- python/pyspark/pipelines/cli.py init_cli.py SparkPipelines.scala` (encoding change, session stop on failure, session injection, all also in 4.2.x) |

## Environment

- `pyspark[pipelines]==4.3.0` installed with pip into a fresh venv (ch77: `$BOOK_DATA/work/sdp/ch77/.venv`; ch78, ch79, films: `$BOOK_DATA/work/sdp/venv`), with `pandas<3` (2.3.3), pyarrow 26.0.1, grpcio 1.85.3, PyYAML 6.0.4. Python 3.12, OpenJDK 17.0.21.
- Connect server port 15104, Spark UI 4144 (`CONNECT_PORT`, `SPARK_UI_PORT`). Local-mode runs (ch78 `local_cwd.sh`) use the fixed port 15002, checked free before the run.
- Hive metastore (`spark.sql.catalogImplementation=hive`) on the server unless the text says otherwise.
- Iceberg: unreleased `iceberg-spark-runtime-4.3_2.13` build `b1eab64` at `$BOOK_DATA/jars/` (see `notes/ch31.md`).
- Data: NYC taxi June 2026 (`$BOOK_DATA/nyc-taxi/yellow/yellow_tripdata_2026-06.parquet`), split into days by `code/ch77/make_days.py`; zones CSV.
- Animations: HyperFrames 0.9.19 through the create-animation kit, theme `ide`, landscape, scaled to 1280x720 with ffmpeg (crf 28). Sources in `code/ch7N/anim/`.
- Films: VHS through the film-demo kit, `--format chapter`, no bench lock (no timings quoted).
- No timings are quoted in these chapters, so no run used the bench lock.
- All servers were stopped by PID. No containers or clusters.

## Open findings (not filed; not called bugs in the book)

1. **Decorator `spark_conf` does not reach the write of a materialized view** (4.3.0). Two MVs with the same aggregation, one with `spark_conf={"spark.sql.shuffle.partitions": "2"}`, AQE off: both write 140 files (default 200 partitions), and with spec `shuffle.partitions: "3"` both write 3. Likely cause: `BatchTableWrite.executeInternal` wraps only the analysis in `withSqlConf`; `saveAsTable` runs in a `Future` that can plan after the block restores the confs. Repro: `code/ch79/flowconf.sh`. Not tested on streaming tables. Book text: ch79 Step 2, ch32 Step 5 note.
2. **`--refresh` with a name that is not in the graph is accepted**: every flow is EXCLUDED, the run is COMPLETED, exit code 0. Repro: `code/ch78/cli.sh` (section refresh, `--refresh no_such_table`).
3. **Local mode with a custom Connect port waits forever**: `spark-pipelines --conf spark.connect.grpc.binding.port=15104 --conf spark.ui.port=4144 run` (no `--remote`) starts a server on 15104 and then hangs after `Creating Spark session...` until killed. The same command without the port conf completes. Repro: `spark-pipelines init --name t1 && cd t1 && timeout 200 spark-pipelines run --conf spark.connect.grpc.binding.port=15104`. Root cause not examined (the client may connect to the default port). Book text: ch78 Step 10, as an observation.
4. **A `libraries` entry without `glob: include:`** gives a raw `TypeError: string indices must be integers, not 'str'`, no error class. Repro: `code/ch78/spec.sh`.
5. **Doc gap: the programming guide lists `libraries` as required**; `cli.py` requires only `name` and `storage`, and the server then refuses the empty graph with `RUN_EMPTY_PIPELINE`.
6. **Doc gap: the guide says `SparkSession.builder.config(...).getOrCreate()` "is blocked in SDP"**; ch32 Step 4 (`code/ch32/output/session_rules.txt`) shows it runs and changes the pipeline session.
7. **Message text**: `CONFLICTING_PIPELINE_REFRESH_OPTIONS` names `--full_refresh` (underscore) for the flag `--full-refresh` (`cli.py` `messageParameters`).
8. **Mixed time zones in one log**: `log_with_curr_timestamp` uses `datetime.now()` (local) while its docstring says only UTC is supported; events are printed in UTC. One CLI log jumps by the UTC offset.
9. **Helper module imports depend on an unset `PYTHONPATH`**: `bin/spark-pipelines` builds `${SPARK_HOME}/python/:$PYTHONPATH`; when `PYTHONPATH` is empty the trailing empty entry makes the spec folder importable, otherwise `ModuleNotFoundError`. Doc gap at least. Repro: `code/ch78/extra.sh` (section helpers).
10. **Static configuration in the spec** is ignored with a UserWarning on the session, then fails each flow (`CANNOT_MODIFY_STATIC_CONFIG`) and leaves MVs empty. The CLI could refuse static keys up front. Repro: `code/ch79/config.sh` (section static).
11. **Relative `storage`** passes the CLI checks and fails only on the server at `StartRun`, after registration ran module code. Could be a client-side check. Repro: `code/ch78/spec.sh`.
12. **The CLI has no connect timeout**: with no server it keeps trying (20 s and more in `exit-codes.txt`).
13. **Existing chapter defect, not mine to fix (ch33 Step 7)**: the output block pastes a traceback from `code/ch33/progress.py` (`IsADirectoryError` on the Spark 4.3 `eventlog_v2_*` folder) instead of the progress lines. Needs the Part VI code fixed and the output regenerated.

## Not verified

- `spark_conf` on streaming tables (finding 1 covers only materialized views).
- `--packages` with a local server (text relies on `SparkPipelines` routing, not a run).
- Secrets redaction in the Spark UI: the text says only that session configurations are readable with `spark.conf.get`; the UI was not examined.
- Delta Lake with SDP on 4.3: no Delta release runs on 4.3.0 (from part06 notes, ch30); text only.

## Films and animations

| Tape / scene | Beat | Clip | VO draft (checked against outputs) |
|---|---|---|---|
| `code/film/ch77.tape` | day2 | `ch77-day2` | After the first run the pipeline has one day. A second day arrives. The run reads only the new file: raw trips is the first day plus the second. |
| `code/film/ch78.tape` | cli | `ch78-cli` | The CLI finds the spec from a subfolder. It refuses a glob that is not a folder with two stars, and it refuses a README in the transformations folder. |
| `code/film/ch79.tape` | envs | `ch79-envs` | A static key in the spec fails every flow. Then one project runs twice, with a dev spec and a prod spec: different database, different partitions. |
| `code/ch77/anim` | run | `ch77-anim-run` | The server resolves the whole graph, then starts each flow when its inputs are complete. |
| `code/ch78/anim` | refresh | `ch78-anim-refresh` | Only the selected flow runs. A full refresh starts a new checkpoint folder. |
| `code/ch79/anim` | config | `ch79-anim-config` | The value goes to the session and to the graph; in the test, the write used three partitions, not two. |

Cut list: `book/media/clips/clips-sdp.tsv`. Clip cutting as in part06 notes (`ffmpeg ... -crf 28 -preset slow`). Each mp4 is under 400 KB.
