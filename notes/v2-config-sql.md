# v2-config-sql notes: chapters 74 and 75 (provisional numbers)

Branch `v2-config-sql`, worktree `~/spark-masterclass-parts/config-sql`. Ports: Spark UI 4142, Connect 15102 (the Thrift server in ch12 also uses 15102, never at the same time as Connect).

## Scope

Two new chapters. No existing chapter was edited.

- `book/chapters/ch08-setting-configurations.md`: Setting Spark configurations, a practical guide. Goes in Part I (Getting started) after the quick start (ch07).
- `book/chapters/ch12-spark-sql.md`: Spark SQL: the language, the catalog and the tools. Opens the new Spark SQL part, before ch13 and ch14.

Cross-references use current numbers. Chapter 12 links to Chapter 8 as `ch08-setting-configurations.md`, and Chapter 8 links to Chapter 12 as `ch12-spark-sql.md`. The last paragraph of ch12 says "The next chapter, Chapter 13": correct after renumbering only if ch13 directly follows ch12 in the new Spark SQL part.

Shared files added: `code/common/svgfig.py` and `code/common/build_anim.sh`, byte-identical copies of the files on branch `v2-vis-a` (so a merge has no conflict).

## Files

- `code/ch08/`: `layers.py`, `env_vars.py`, `static_runtime.py`, `find_configs.py`, `source_counts.sh`, `defaults.py`, `connect_server.sh`, `connect_conf.py`, `sdp_conf.sh` + `sdp_conf/` project, `figures/make_figures.py`, `anim/layers/`, `output/*.txt`.
- `code/ch12/`: `sql_cli.sh`, `thrift.sh`, `thrift_demo.sh`, `thrift_client.py`, `ConnectJdbc.java`, `connect_jdbc.sh`, `catalogs.py`, `tables_views.py`, `type_system.py`, `functions.py`, `function_groups.sh`, `ansi_changes.py`, `same_plan.py`, `figures/make_figures.py`, `anim/names/`, `output/*.txt`.
- Figures: `book/media/figures/ch08-layers-{light,dark}.svg`, `ch08-who-may-change-*`, `ch12-catalogs-*`, `ch12-drop-table-*`, `ch12-function-groups-*`. Each was rendered to PNG with headless Chrome and checked.
- Animations (HyperFrames, paper theme, 1280x720): `book/media/clips/ch08-anim-layers.{mp4,webp}`, `ch12-anim-names.{mp4,webp}`. Storyboards next to the sources.
- Film clips (VHS, `film render code/film/chNN.tape --format chapter --demo code`): `ch08-layers`, `ch08-static`, `ch08-connect`, `ch12-cli`, `ch12-thrift`, `ch12-catalogs`. Cut list: `book/media/clips/clips-v2-config-sql.tsv`. `ch08-layers` keeps 0-6 s, plays 40-92 s at 4x, then 92-105.7 s (the caption says so). `ch12-cli` and `ch12-thrift` keep the first 5 s and the last 14 s (the caption says the wait is not shown). All mp4 files are under 230 kB.

### VO lines (drafts, checked against the output)

| clip | VO |
|---|---|
| ch08-layers | Each row starts a new session with one more source. The value climbs from the default, to the file, to the flag, to the builder. In the last row the SparkConf says four gigabytes, but the heap is fifteen hundred megabytes. |
| ch08-static | A runtime SQL config changes. A static one raises CANNOT_MODIFY_STATIC_CONFIG, and a core one raises CANNOT_MODIFY_CONFIG. |
| ch08-connect | Two Connect sessions on one server. The builder set twenty-two, the other session keeps the server's twenty-one, and the static value never changed. |
| ch12-cli | spark-sql starts a driver with a Hive metastore, makes a database and a table, and prints rows as tab-separated text. |
| ch12-thrift | Two beeline connections get two sessions, so the temporary view is in only one. PyHive queries the same server with no JVM. |
| ch12-catalogs | One join across two catalogs: a Parquet table in the session catalog and a table in an H2 database. |

## Technical names and technical verbs

TN: configuration, configuration key, source, launcher, driver, JVM, heap, SparkConf, SQLConf, session, session catalog, catalog, namespace, database, schema, table, managed table, external table, data source table, view, temporary view, global temporary view, warehouse, metastore, Derby, Hive, Thrift server, JDBC, ODBC, driver (JDBC), beeline, PyHive, H2, CTAS, registry, group, annotation, flag, preview, collation, ANSI mode, store assignment, keyword, plan, parsed plan, analyzed plan, optimized plan, command, event log, REST API, Environment tab, spec, flow, materialized view, Connect server, Config RPC.

TV: set, run, read, parse, analyze, resolve, register, redact, launch, drop, insert, cast, query, connect.

STE allowlist: none needed beyond `scripts/ste-allow.txt`.

## STE checker results

| Chapter | Findings |
|---|---|
| ch08 | 0 |
| ch12 | 0 |

Rules broken on purpose: none flagged. Output blocks, error messages, config keys and SQL keywords stay as code. Figure alt texts were shortened to two sentences so the checker does not join them into long paragraphs.

## Environment

- pyspark 4.3.0 (pip, `code/.venv`), OpenJDK 17.0.21.1, Python 3.12.
- Tarball `spark-4.3.0-bin-hadoop3` in `$BOOK_DATA/tarball/` (for `jars/connect-repl/spark-connect-client-jdbc_2.13-4.3.0.jar` and `spark-connect-client-jvm_2.13-4.3.0.jar`).
- `com.h2database:h2:2.3.232` through `spark.jars.packages` (ch12 catalogs).
- `pyhive[hive_pure_sasl]` through `uv run --with` (ch12 Thrift client).
- Spark source: `~/repos/spark` at tag `v4.3.0` (also v3.5.0, v4.0.0, v4.1.0, v4.2.0 for the "new in" checks).
- No timings are quoted in the text, so no run took the bench lock. The `Time taken` lines in `sql_cli.txt` are shown as output only.

## Claim-to-source table

| Claim | Source |
|---|---|
| Precedence: default < spark-defaults.conf < --conf < builder | `code/ch08/output/layers.txt` §1; docs `configuration.md` v4.3.0 |
| SPARK_DRIVER_MEMORY is a fallback, conf file wins | `layers.txt` rows 2-3 |
| `--driver-memory` wins over `--conf spark.driver.memory` | `layers.txt` row 5; `SparkSubmit.scala` OptionAssigner |
| builder conf goes to spark-submit as --conf before PYSPARK_SUBMIT_ARGS | `python/pyspark/java_gateway.py` v4.3.0 lines 71-75 |
| SparkConf says 4g, heap 1500 MiB | `layers.txt` row 6 and the two extra rows |
| `--load-spark-defaults` new in 4.0 | `SparkSubmitArguments.scala`: absent at v3.5.0, present at v4.0.0 |
| SPARK_ANSI_SQL_MODE sets the ANSI default | `SQLConf.scala` v4.3.0 line 5846; `env_vars.txt` |
| RESET -> session start value, unset -> default, newSession keeps 13 | `layers.txt` §2, §3 |
| CANNOT_MODIFY_STATIC_CONFIG / CANNOT_MODIFY_CONFIG, default.parallelism exception | `static_runtime.txt`; `classic/RuntimeConfig.scala` `requireNonStaticConf` |
| setCommandRejectsSparkCoreConfs default true | `SQLConf.scala` v4.3.0 |
| "Using an existing Spark session; only runtime SQL configurations will take effect." | `classic/SparkSession.scala` v4.3.0 line 1122 |
| SET -v 324 rows (Classic), 345 (Connect server) | `find_configs.txt`, `connect_conf.txt` |
| SET -v rows since 4.0/4.1/4.2/4.3: 43/25/24/25 | `find_configs.txt` §1b |
| Environment tab: runtime SQL changes absent, redaction | `find_configs.txt` §3, §6 |
| SQLConf 789 declarations (478 internal), StaticSQLConf 36 (21), core 559 (62) | `source_counts.txt` (text search, approximate, stated in the chapter) |
| Connect builder drops static/core keys silently | `connect_conf.txt` §1; `pyspark/sql/connect/session.py` `_apply_options` |
| Connect Config RPC hides redacted keys, new in 4.3 | `connect_conf.txt` §4; SPARK-58658 (`git log v4.2.0..v4.3.0` on `SparkConnectConfigHandler.scala`) |
| SDP spec configuration goes to builder and to CreateDataflowGraph | `python/pyspark/pipelines/cli.py` v4.3.0 lines 321-337 |
| Spec value 7 in both flows, per-flow spark_conf 5 not in the plan | `sdp_conf.txt` (event log, zstd) |
| Defaults in the reference table | `defaults.txt` (ConfigEntry registry of the 4.3.0 jars); session.timeZone default is a function (JVM time zone); warehouse default `Utils.resolveURI("spark-warehouse")` in `StaticSQLConf.scala` |
| Arrow defaults true in 4.2 and 4.3, false in 4.1 | `SQLConf.scala` at v4.1.0/v4.2.0/v4.3.0; PySpark migration guide v4.3.0 lines 31-32 |
| ANSI default since 4.0 (SPARK-44444) | ch10 notes, `git log` grep SPARK-44444 |
| spark-sql and Thrift server use the Hive catalog unless in-memory is set | `SparkSQLEnv.scala` v4.3.0 lines 56-65 |
| Thrift server rejects `set:system:` since 4.3 | `docs/sql-migration-guide.md` v4.3.0 |
| Connect JDBC driver new in 4.1 | `sql/connect/client/jdbc`: 0 files at v4.0.0, 18 at v4.1.0; driver class in META-INF/services |
| JDBC driver: complex types, no prepareStatement | `connect_jdbc.txt` |
| SHOW CATALOGS lists a catalog only after first use | `catalogs.txt` §1, §4 |
| current_database = current_schema | `catalogs.txt` column names |
| DROP managed removes files, external keeps them | `tables_views.txt` §6 |
| DESCRIBE AS JSON new in 4.0, SHOW ... AS JSON new in 4.3 | `SqlBaseParser.g4`: "AS JSON" 0 at v3.5.0, 1 at v4.0.0 to v4.2.0, 4 at v4.3.0 (lines 371, 373, 380, 393) |
| CREATE VIEW WITH SCHEMA new in 4.0, default COMPENSATION | grammar at v4.0.0; `tables_views.txt` §9, §10 |
| global temp view from SQL over a temp view fails in another session | `tables_views.txt` §8 |
| Type additions by release | type class files in `sql/api/.../types/` at each tag (VariantType 4.0; Geography/Geometry/Time 4.1; nanos 4.3); migration guide (CHAR 3.1, ANSI intervals 3.2); `spark.sql.timestampType` version 3.4.0 |
| TIME and nanos behind internal flags; timeType default Utils.isTesting | `type_system.txt` §1, §3; `SQLConf.scala` v4.3.0 line 8316-8321 |
| 591 SHOW FUNCTIONS, 581 registry, 27 groups, since counts (150 in 4.x) | `functions.txt` |
| 536 annotations in source | `function_groups.txt` |
| ANSI expression changes, storeAssignmentPolicy, reserved keywords | `ansi_changes.txt` |
| Parsed plans differ at the leaf, analyzed equal | `same_plan.txt` §1 |
| CTAS/saveAsTable/INSERT/insertInto command nodes | `same_plan.txt` §2 (REST API planDescription) |

## Open findings

1. **SDP per-flow `spark_conf` does not reach the plan of a batch materialized view (4.3.0).** Repro: `code/ch08/sdp_conf/` without the `configuration` key (two MVs over `spark.range(1000).groupBy(id % 10).count()`, the second with `@dp.materialized_view(spark_conf={"spark.sql.shuffle.partitions": "5"})`). Run with `EVENTLOG=1 common/pipeline.sh`. The event log shows `hashpartitioning(..., 200)` for both flows (with the spec value 7: 7 for both). Possible cause, not verified: `BatchTableWrite.executeInternal` (`sql/pipelines/.../FlowExecution.scala`) wraps only the creation of a `Future` in `SparkSessionUtils.withSqlConf`. The write runs inside the Future, possibly after the conf is restored. Check with `spark-bug` before filing. Chapter 49 says that `spark_conf` "sets configurations for the flow of that dataset only": the integrator may want a caution there.
2. **`TIME` type name needs an internal flag in 4.3.0.** `CAST(x AS TIME)` and DDL with `TIME` raise `UNSUPPORTED_TIME_TYPE` unless `spark.sql.timeType.enabled=true` (default `Utils.isTesting`). `TIME'10:07:00'` literals work. Chapter 9 presents `TIME(p)` in its type table without the flag. Not a bug claim: possibly intended (the type is still in development); a doc gap at least.
3. **The Spark Connect JDBC driver has no page in the 4.3.0 docs.** To run it outside a build tool you need the JDBC jar, the JVM client jar, Spark's common jars (`spark-common-utils`, `network-common`, Scala library) and `--add-opens=java.base/java.nio=ALL-UNNAMED`. `prepareStatement` raises `SQLFeatureNotSupportedException`.
4. Not a finding, a documented behavior shown in ch08: in client mode a builder value for `spark.driver.memory` can show in the SparkConf without effect on the heap (the docs warn about it).

## Not verified

- The version that added `SaveAsV1TableCommand` (the chapter does not claim one).
- The source counts of configuration declarations are a text search (the chapter says approximate).
- Behavior of the Connect JDBC driver with BI tools (only a Java program was run).

## Nav entries and roadmap rows for the integrator

Nav (Part I after ch07, and the first entry of the Spark SQL part):

```yaml
  - Getting started:
      - chapters/ch07-quick-start.md
      - chapters/ch08-setting-configurations.md
      - chapters/ch18-spark-connect.md
  - Spark SQL:
      - chapters/ch12-spark-sql.md
      - chapters/ch13-spark-sql-in-depth.md
      - chapters/ch14-procedural-sql.md
```

Roadmap rows:

```markdown
| 74 | [Setting Spark configurations](chapters/ch08-setting-configurations.md) | Every source of a config and which one wins, static vs runtime, SQL vs core, finding configs (`SET -v`, Environment tab, REST, `buildConf`), Classic vs Connect, SDP `configuration`, a reference table of defaults. **4.3:** Connect hides redacted configs |
| 75 | [Spark SQL: the language, the catalog and the tools](chapters/ch12-spark-sql.md) | `spark-sql`, the Thrift server, the Connect JDBC driver, catalogs and three-part names, managed and external tables, views, the 4.x type system, the function library by group, ANSI mode, one plan for SQL and DataFrames. **4.3:** `SHOW ... AS JSON`, nanosecond types (preview) |
```
