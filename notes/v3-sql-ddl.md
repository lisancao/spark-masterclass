# v3-sql-ddl notes: chapters 84 and 85 (provisional numbers)

Branch `v3-sql-ddl`, worktree `~/spark-masterclass-parts/sql-ddl`. Ports: Spark UI 4153, Connect 15113 (not used), Hive Metastore container 19113.

## Scope

Two new chapters in Part IV (Spark SQL), from the gap audit rows "DDL" (Hive-format tables, CREATE TABLE LIKE, REPAIR TABLE/MSCK, TRUNCATE, DEFAULT columns, constraints, ALTER TABLE, view schema binding, COMMENT ON), "DML" (INSERT INTO/OVERWRITE, INSERT OVERWRITE DIRECTORY, LOAD DATA), "Hive tables", "Hive UDF" and items 10 and 18 of the recommendations.

- `book/chapters/ch16-tables-ddl-writes.md`: Tables, DDL and writes.
- `book/chapters/ch17-hive-tables-metastore.md`: Hive tables and the metastore.

Changes to existing chapters: `ch15-spark-sql.md` only, links to the new chapters (two bullets in the opening list, one sentence after Step 1's paragraph on the Hive catalog, one sentence at the end of Step 5). No other existing chapter was edited.

Cross-references use current numbers: Chapter 8, 15, 20, 43, 45, 54, 55 and the two new chapters as 84 and 85. Chapter 16 ends with "The next chapter, Chapter 17": correct after renumbering only if ch17 directly follows ch16 in Part IV.

Not repeated from ch15: managed against external tables, the three kinds of view, CHAR/VARCHAR, `storeAssignmentPolicy`, the Thrift server and `spark-sql`. Ch84 Step 7 goes deeper into `WITH SCHEMA` than ch15 Step 6 (all four modes against five table changes).

## Files

- `code/ch16/`: `ddl.py` (helpers), `create_variants.py`, `defaults_constraints.py`, `alter_comment.py`, `view_schema.py`, `writers.py`, `partition_overwrite.py`, `insert_forms.py`, `figures/make_figures.py`, `anim/overwrite/`, `output/*.txt`.
- `code/ch17/`: `hive.py` (helpers), `hive_session.py`, `metastore_versions.py`, `serde_tables.py`, `partitions.py`, `derby_rows.sh`, `load_and_dirs.py`, `hive_udf/` (`BoroughCode.java`, `Initials.java`, `build.sh`; the jar is gitignored and built by `build.sh`), `hive_udf.py`, `transform.py`, `zone_short.py`, `standalone_metastore.sh`, `remote_metastore.py`, `figures/make_figures.py`, `anim/msck/`, `output/*.txt`.
- Figures (rendered to PNG with headless Chrome and checked): `book/media/figures/ch16-partition-overwrite-{light,dark}.svg`, `ch16-view-modes-*`, `ch17-serde-paths-*`, `ch17-metastore-rows-*`.
- Animations (HyperFrames, paper theme, 1280x720, `code/common/build_anim.sh`): `book/media/clips/ch16-anim-overwrite.{mp4,webp}`, `ch17-anim-msck.{mp4,webp}`. Storyboards next to the sources.
- Film clips (VHS, `film render code/film/chNN.tape --format chapter --demo code`): see "Clips" below. Cut list `book/media/clips/clips-v3-sql-ddl.tsv`.
- No shared helper was added to `code/common/`.

## Technical names and technical verbs

TN: catalog, session catalog, V1, V2, V2 catalog, in-memory catalog, Hive catalog, metastore, Hive Metastore, metastore service, Derby, table, managed table, external table, data source table, Hive SerDe table, SerDe, input format, output format, provider, partition, partition folder, bucket, storage property, table property, column, default, existence default, constraint, generated column, identity column, view, view mode, schema, save mode, writer, writer option, staging folder, commit, snapshot, plan, command, scan node, client, jar, class, UDF, UDAF, UDTF, script, field separator, warehouse, working directory, container, Ivy cache, docstring.

TV: create, drop, insert, overwrite, append, replace, truncate, rename, alter, load, repair, refresh, register, cast, commit, mount, download, parse, resolve, write, read.

STE allowlist for these chapters (passed with `--allow`): `staging docstring working`. Shared list unchanged.

## STE checker results

| Chapter | Command | Findings |
|---|---|---|
| ch16 | `scripts/ste-check book/chapters/ch16-tables-ddl-writes.md --allow staging docstring` | 0 |
| ch17 | `scripts/ste-check book/chapters/ch17-hive-tables-metastore.md --allow staging docstring working` | 0 |
| ch15 (after the link edits) | `scripts/ste-check book/chapters/ch15-spark-sql.md` | 0 |

Rules bent on purpose: none flagged. Output blocks, error messages, config keys and SQL stay as code. Figure alt texts were shortened so the checker does not join them with the caption into one long sentence. Sentences that start with a link were reworded ("For ..., read [Chapter N]"), because the checker joins a sentence that starts with `[` to the previous one.

## Environment

- pyspark 4.3.0 (pip, `code/.venv` from `uv sync`), OpenJDK 17.0.21.1, Python 3.12.
- Spark source: `~/repos/spark` at `v4.3.0` (also `v3.3.0`, `v3.4.0`, `v3.5.0`, `v4.0.0`, `v4.1.0`, `v4.2.0` for the "since" checks), read with `git show` and `git grep` only.
- Iceberg: unreleased runtime `$BOOK_DATA/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar`, built from apache/iceberg PR #18749 at commit `b1eab64` (the same jar as ch55 and ch37; sha256 `f915c7b075e58012f6a9ee2e34dffcccac9a5715816e01738ff5560efe6dc4d5` per ch55 notes). Hadoop catalog `lake`. Labelled in ch16 as not a release.
- Hive client: built-in Hive 2.3.10 jars of the pyspark wheel. For ch17 Step 12, Hive 4.0.1 client jars from Maven (`spark.sql.hive.metastore.jars=maven`), cached in `~/.ivy2.5.2`.
- Hive Metastore: Docker image `apache/hive:4.0.1`, `SERVICE_NAME=metastore`, embedded Derby inside the container, container `sm-ch17-hms` on port 19113. Removed at the end (`ch17/standalone_metastore.sh down`).
- Derby `ij`: the `derby-10.16.1.1`, `derbyshared` and `derbytools` jars of the pyspark wheel.
- Hive UDF jar: `javac --release 17` against the pyspark jars.
- No timings are quoted in the text, so no run took the bench lock.

## Claim-to-source table

| Claim | Source |
|---|---|
| `OPTIONS` become storage properties, `TBLPROPERTIES` table properties; bucket files `_00000`..`_00003`, zstd names | `code/ch16/output/create_variants.txt` section 1 |
| No `USING` gives `parquet` (`spark.sql.sources.default`); Hive before 4.0, `spark.sql.legacy.createHiveTableByDefault` | `create_variants.txt` section 2; SQL migration guide v4.3.0 "Upgrading from Spark SQL 3.5 to 4.0" |
| CTAS with a column list is a ParseException; `LIKE ... USING json`; `IF NOT EXISTS` no compare; REPLACE refused; EXTERNAL needs LOCATION; MSCK on external table 0 -> 3 rows | `create_variants.txt` sections 3 to 7 |
| `INSERT` with fewer values fails; defaults filled with a column list or `DEFAULT` | `defaults_constraints.txt` section 1 |
| `ADD COLUMNS ... DEFAULT` leaves file sizes 1645 B and 1630 B unchanged; old rows read 0.00 | `defaults_constraints.txt` section 2 |
| `SET DEFAULT`/`DROP DEFAULT` affect new rows only | `defaults_constraints.txt` section 3 |
| Non-deterministic default and subquery refused; `allowedProviders=csv,json,orc,parquet`; text refused | `defaults_constraints.txt` section 4 |
| NOT NULL not checked by default; `spark.sql.fileSource.insert.enforceNotNull` (version 4.2.0, default false) | `defaults_constraints.txt` section 5; `SQLConf.scala` v4.3.0 `FILE_SOURCE_INSERT_ENFORCE_NOT_NULL`; `createDataSourceTables.scala` line 85 |
| Generated/identity/constraints refused on session catalog and Iceberg build | `defaults_constraints.txt` section 6 |
| Session catalog capabilities = `SUPPORT_COLUMN_DEFAULT_VALUE` only | `V2SessionCatalog.scala` v4.3.0 `capabilities()`; `TableCatalogCapability.java` |
| Generated columns in grammar since 3.4, identity 4.0, constraints 4.1; DEFAULT 3.4 | `SqlBaseParser.g4` at v3.3.0 (no GENERATED), v3.4.0, v4.0.0 (IDENTITY), v4.1.0 (`tableConstraintDefinition`); `spark.sql.defaultColumn.enabled` `.version("3.4.0")` |
| Iceberg build checks NOT NULL; refuses non-null default in format v2 | `defaults_constraints.txt` section 7 |
| RENAME TO moves the folder; partitioned table in in-memory catalog keeps old partition location and reads 0 rows | `alter_comment.txt` section 1; repro below (open finding 1) |
| Column rename/drop/type/replace refused on session catalog; accepted on Iceberg | `alter_comment.txt` sections 2 and 7 |
| Partition DDL; renamed partition shows 140184 trips under 2026-06-30 | `alter_comment.txt` section 4 |
| TRUNCATE keeps partition list, refuses external table; Iceberg TRUNCATE works, ADD PARTITION refused | `alter_comment.txt` sections 5 and 7 |
| COMMENT ON COLUMN and list form new in 4.3 | `SqlBaseParser.g4` v4.2.0 (`#commentNamespace`, `#commentTable` only) against v4.3.0 (`#commentColumn`); new page `docs/sql-ref-syntax-ddl-comment.md` at v4.3.0 (not at v4.2.0); `alter_comment.txt` section 6 |
| `IS NULL` sends null to `TableChange.updateColumnComment` | SQL migration guide v4.3.0, 4.2 to 4.3 section |
| View mode results grid | `view_schema.txt` section 2 |
| Default mode COMPENSATION from `spark.sql.legacy.viewSchemaCompensation=true` (4.0.0); `viewSchemaBindingMode` (4.0.0) | `view_schema.txt` section 5; `SQLConf.scala` v4.3.0 |
| EVOLUTION writes the new columns back; column list makes TYPE EVOLUTION; temp views refuse | `view_schema.txt` sections 3 to 5; `docs/sql-ref-syntax-ddl-create-view.md` v4.3.0 |
| Save mode results on a path and on a table | `writers.txt` sections 1 and 2 |
| insertInto by position wrote vendor 126260, trips 1 | `writers.txt` section 3 |
| insertInto treats errorifexists/ignore as append | `DataFrameWriter.scala` v4.3.0 docstring of `insertInto` (lines 300 to 304) |
| writeTo on session catalog: only create works, "Cannot write into v1 table" | `writers.txt` section 4; `DataFrameWriterV2Suite.scala` v4.3.0 |
| writeTo methods to plans | `DataFrameWriterV2.scala` (classic) v4.3.0: `CreateTableAsSelect`, `AppendData.byName`, `OverwriteByExpression.byName`, `OverwritePartitionsDynamic.byName`, `ReplaceTableAsSelect` |
| DataFrameWriterV2 added in 3.0 | file exists at v3.0.0, not at v2.4.0 |
| Iceberg writeTo results and five metadata files | `writers.txt` section 5 |
| partitionOverwriteMode default STATIC, version 2.3.0 | `SQLConf.scala` v4.3.0 `PARTITION_OVERWRITE_MODE` |
| Static/dynamic file results | `partition_overwrite.txt` sections 1 to 5 |
| Static deletes first, dynamic writes to staging | `InsertIntoHadoopFsRelationCommand.scala` v4.3.0 (`deleteMatchingPartitions` before the write, `dynamicPartitionOverwrite`); `HadoopMapReduceCommitProtocol.scala` (`.spark-staging-{jobId}`); `partition_overwrite.txt` section 8 |
| Writer option ignored by insertInto; WITH options and session conf work; path option works | `partition_overwrite.txt` sections 6 and 7; `DataFrameWriter.scala` v4.3.0 lines 360 to 400 (open finding 2) |
| Hive SerDe tables not affected by partitionOverwriteMode | config doc of `spark.sql.sources.partitionOverwriteMode`; `ch17/output/partitions.txt` section 6 |
| BY NAME since 3.5 | `SqlBaseParser.g4` v3.4.0 (no `BY NAME`) and v3.5.0 |
| REPLACE WHERE before 4.0 | `REPLACE whereClause` in grammar at v3.4.0, v3.5.0, v4.0.0 |
| WITH SCHEMA EVOLUTION, REPLACE ON/USING added in 4.2 | grammar v4.1.0 (only REPLACE whereClause) against v4.2.0 |
| REPLACE ON/USING refused for each table in 4.3.0 | `Analyzer.scala` v4.3.0 `ResolveInsertInto` (`unsupportedInsertReplaceOnOrUsing`); `insert_forms.txt` sections 2 and 4 |
| INSERT ... WITH (options) since 4.0 | `optionsClause` in `insertInto` rule at v4.0.0, absent at v3.5.0 |
| Command per INSERT form | `insert_forms.txt` section 2 |
| REPLACE WHERE accepted rows outside the condition | `insert_forms.txt` section 3 (open finding 5) |
| `enableHiveSupport` folder contents, 174 Derby files, Hive configs, `HiveExternalCatalog`, XSDB6 | `ch17/output/hive_session.txt` |
| Metastore versions per release | `metastore_versions.txt` section 1 (from `sql/hive/src/main/scala/org/apache/spark/sql/hive/client/package.scala` at each tag); docs `sql-data-sources-hive-tables.md` v4.3.0 |
| Below 2.0 dropped in 4.0 because they need JDK 8 | SQL migration guide v4.3.0, "Upgrading from Spark SQL 3.5 to 4.0" |
| Hive jars in the wheel; errors for 1.2.1 and 3.1.3 builtin | `metastore_versions.txt` sections 2 and 3 |
| SerDes and UDFs always run with built-in Hive classes | `docs/sql-data-sources-hive-tables.md` v4.3.0 |
| Provider/SerDe per table, file contents, scan and write nodes | `serde_tables.txt` sections 2 to 4 |
| SHOW CREATE TABLE Spark DDL fails when run | `serde_tables.txt` section 5 (open finding 3) |
| USING hive; legacy default | `serde_tables.txt` section 6 |
| Partition counts 43, 1, 69; 113 -> 159; REFRESH 116; MSCK ADD/SYNC; RECOVER | `partitions.txt` sections 1 to 4 |
| strict dynamic partition mode error | `partitions.txt` section 6 |
| DROP PARTITION deletes folder outside warehouse | `partitions.txt` section 5 |
| Hive tables keep other partitions on INSERT OVERWRITE; data source table loses Bronx | `partitions.txt` sections 6 and 7 |
| Derby rows, Spark table parameters | `derby_rows.txt` |
| LOAD DATA copy/move/overwrite/partition; refused for data source table; CSV in Parquet table unreadable | `load_and_dirs.txt` sections 1 to 4 |
| INSERT OVERWRITE DIRECTORY commands; convertMetastoreInsertDir since 3.3 | `load_and_dirs.txt` section 5; SQL migration guide (3.2 to 3.3 section) |
| Hive UDF wrappers, persistent function in new process, no Hive support error | `hive_udf.txt` |
| Hive UDAF in ObjectHashAggregate | `hive_udf.txt` section 3; physical plan |
| TRANSFORM: Hive default LazySimpleSerDe with tab; non-Hive default `\u0001`; SERDE needs Hive | `transform.txt`; `BaseScriptTransformationExec.scala` line 380 (`TOK_TABLEROWFORMATFIELD -> \u0001`); `SparkSqlParser.scala` line 1311 (`field.delim -> \t`) |
| Builtin client against HMS 4.0.1: `Invalid method name: 'get_table'`; 4.0.1 maven client works; TRANSLATED_TO_EXTERNAL and purge; two processes | `remote_metastore.txt` |
| Thrift server ORDINAL_POSITION (4.1), hive-llap-common removed (4.0) | SQL migration guide v4.3.0 |

## Open findings

1. **In-memory catalog: `ALTER TABLE ... RENAME TO` on a partitioned managed table leaves the partition locations at the old folder (4.3.0).** The folder moves, the table location changes, each partition keeps `.../<old>/p=1`, and the table reads 0 rows. The Hive catalog moves the partition locations and the row stays. `InMemoryCatalog.renameTable` (lines 286 to 320 at v4.3.0) rewrites partition URIs with `uri.toString.replace(oldDir.toString, newDir.toString)`; a mismatch in the URI string forms (for example `file:/` against `file:///`) is a possible cause, not verified. Not checked on other releases. Check with `spark-bug` before filing. Repro:

   ```python
   import sys, tempfile
   from pyspark.sql import SparkSession
   wh = tempfile.mkdtemp()
   spark = SparkSession.builder.master("local[1]").config("spark.sql.warehouse.dir", wh).getOrCreate()
   spark.sql("CREATE TABLE t USING parquet PARTITIONED BY (p) AS SELECT 1 AS v, 1 AS p")
   spark.sql("ALTER TABLE t RENAME TO t2")
   print(spark.sql("DESCRIBE TABLE EXTENDED t2 PARTITION (p=1)").where("col_name='Location'").first()[1])  # .../t/p=1
   print(spark.table("t2").count())  # 0 (1 with enableHiveSupport())
   ```

2. **`DataFrameWriter.insertInto` ignores the writer option `partitionOverwriteMode` (4.3.0).** Session `STATIC` plus `.option("partitionOverwriteMode", "dynamic").mode("overwrite").insertInto(t)` deleted the other partitions. The V1 `insertIntoCommand(tableIdent)` builds `UnresolvedRelation(tableIdent)` with no options; the V2 branch reads only `conf.partitionOverwriteMode`. The SQL `INSERT OVERWRITE ... WITH ('partitionOverwriteMode' = 'dynamic')` and a path write with the option do work. The config doc says the option "takes precedence over this setting" with a `save(path)` example. At least a doc gap; possibly a bug. Repro: `code/ch16/partition_overwrite.py` section 6. Chapter 16 states the behavior and the source lines, not "bug".

3. **`SHOW CREATE TABLE` on a delimited Hive text table gives Spark DDL that cannot run.** For `ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE` with an `INT` column, the output is `CREATE TABLE ... USING text` with the typed columns. Running it fails with `UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE`, and the field separator is lost. `AS SERDE` gives correct Hive DDL. Possibly intended (the 3.0 migration note says Spark DDL is always returned), but the result is not usable. Repro: `code/ch17/serde_tables.py` section 5.

4. **Misleading error for a Hive UDF class in a session without Hive support.** `CREATE TEMPORARY FUNCTION f AS '<GenericUDF class>' USING JAR ...` then `SELECT f(...)` raises `[NO_HANDLER_FOR_UDAF] No handler for UDAF '<class>'`, although the class is a UDF. Repro: `code/ch17/hive_udf.py` step 5. Error message gap only.

5. **`INSERT INTO ... REPLACE WHERE` does not check that the new rows match the condition (Iceberg build b1eab64 on 4.3.0).** `REPLACE WHERE borough = 'Bronx' VALUES (9, 'Astoria', 'Queens')` deleted the Bronx rows and inserted a Queens row. Not known whether Spark or the connector is expected to check; Delta has such a check for `replaceWhere`. Observation for the integrator, not a bug claim. Repro: `code/ch16/insert_forms.py` section 3.

6. **Doc gap: `REPLACE ON` / `REPLACE USING` are in the 4.3.0 grammar but always refused.** `docs/sql-ref-syntax-dml-insert-table.md` lists only `REPLACE WHERE`, which matches the behavior. No action needed unless the grammar is meant to be documented.

## Not verified

- The release that added `REPLACE WHERE` (grammar has it at v3.4.0; earlier tags not checked). The chapter says "before 4.0".
- The reason why Hive 4.0.1 translates managed tables to external (the chapter only reports the table properties that the service set).
- Iceberg behavior with format version 3 defaults (only format version 2 was run).
- The download time of the Hive 4.0.1 client jars (no timing is quoted).
- Behavior of the Hive features on Spark Connect (all scripts use Classic sessions).

## Clips

| Clip | Beat | Caption claim |
|---|---|---|
| `ch16-overwrite` | `partition_overwrite.py`, sections 1 and 2 | static deletes June 1 and 3, dynamic keeps them |
| `ch17-msck` | `partitions.py`, section 2 | two folders invisible until MSCK REPAIR TABLE |
| `ch17-hms` | `remote_metastore.py` | builtin client fails, 4.0.1 client works, second process adds a partition |
| `ch16-anim-overwrite` | animation | static deletes first, dynamic stages then replaces one partition |
| `ch17-anim-msck` | animation | ADD then SYNC brings folders and partitions together |

The `writers` beat of `ch16.tape` was rendered but is not embedded (Step 9 already shows the output as text). It can become a clip later.

VO lines (drafts, checked against the output):

- overwrite: "The job computes June 2 again. In static mode, INSERT OVERWRITE with no PARTITION clause deletes June 1 and June 3. In dynamic mode, only June 2 changes."
- msck: "Another job writes two partition folders. SHOW PARTITIONS does not list them, and the count stays 113. After MSCK REPAIR TABLE, the metastore lists five partitions, and the count is 159."
- hms: "The built-in Hive 2.3 client cannot fetch a table from a Hive 4 metastore. With the 4.0.1 client, Spark creates a partitioned table, and a second Spark process adds a partition at the same time."

## Nav entries and roadmap rows for the integrator

Nav (Part IV, Spark SQL, after ch15 or after ch23; ch16 before ch17):

```yaml
  - Spark SQL:
      - chapters/ch15-spark-sql.md
      - chapters/ch16-tables-ddl-writes.md
      - chapters/ch17-hive-tables-metastore.md
      - chapters/ch18-spark-sql-in-depth.md
```

(Or at the end of Part IV, after `ch23-metric-views.md`. The text of both chapters works in either position.)

Roadmap rows:

```markdown
| 84 | [Tables, DDL and writes](chapters/ch16-tables-ddl-writes.md) | CREATE TABLE variants, CTAS, LIKE, DEFAULT values, NOT NULL and constraints, ALTER TABLE on the session catalog, TRUNCATE, view schema modes, DataFrameWriter against DataFrameWriterV2, static and dynamic partition overwrite, the forms of INSERT. **4.3:** COMMENT ON COLUMN |
| 85 | [Hive tables and the metastore](chapters/ch17-hive-tables-metastore.md) | enableHiveSupport and the Derby metastore, metastore versions and client jars, SerDe against data source tables, partitions, MSCK REPAIR TABLE, LOAD DATA, INSERT OVERWRITE DIRECTORY, Hive UDFs, script TRANSFORM, a Hive Metastore 4.0.1 in Docker |
```
