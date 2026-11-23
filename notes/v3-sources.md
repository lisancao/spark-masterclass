# v3 notes: external storage and databases (object stores, JDBC)

Branch `v3-sources`. Writer scope: a new chapter on object storage and the output committers, a new chapter on the JDBC source, and a Derby note in ch66.

## Scope and what changed

New chapters (provisional numbers):

| File | Title | Code | Media |
|---|---|---|---|
| `book/chapters/ch87-object-storage.md` | Spark on object storage: S3A, ABFS, GCS and the output committers | `code/ch87/` (`minio.sh`, `s3.py`, `connect.py`, `committers.py`, `failure.py`, `dpo.py`, `magic_log.py`, `success.py`, `manifest.py`, `listing.py`, `ranges.py`, `anim/commit/`) | figures `ch87-commit-paths.svg`, `ch87-requests.svg`, `ch87-failure.svg`; clips `ch87-failure`, `ch87-success`, `ch87-dpo`; animation `ch87-anim-commit` |
| `book/chapters/ch88-jdbc.md` | Databases over JDBC: partitioned reads, pushdown and writes | `code/ch88/` (`pg.sh`, `jdbc.py`, `load.py`, `partitions.py`, `fetch.py`, `pushdown.py`, `show_sql.py`, `options.py`, `writes.py`, `batch.py`, `catalog.py`, `anim/ranges/`) | figures `ch88-ranges.svg`, `ch88-pushdown.svg`, `ch88-batch.svg`; clips `ch88-pushdown`, `ch88-append`, `ch88-catalog`; animation `ch88-anim-ranges` |

Placement: both chapters go in Part VI (Files, formats and messy data), after ch31 (Documents, images, audio and tables for blobs). ch87 first, then ch88.

Existing chapter changed:

- **ch66** (Python data sources II): a warning admonition at the start of the Derby run in Step 11 ("Spark 4.2 deprecated the Derby JDBC data source", from `docs/sql-migration-guide.md` line 63 at `v4.3.0`, and the dialect list "Derby (deprecated)" in `docs/sql-data-sources-jdbc.md`). The "New in 4.3" note now ends with "Spark 4.2 deprecated the Derby dialect (Step 11)." Further reading links Chapter 88. Nothing else changed. ch66 still uses Derby for its range demo, because its JAR ships with PySpark; the warning says so.

Shared files: none changed. `code/figures/ch87.py` and `ch88.py` use `svgkit.py` but set their own `FIG` path (the kit's `FIG` points at another worktree, `/home/lnc/spark-masterclass-parts/vis-d/...`; the integrator may want to fix that constant in `svgkit.py`).

## Technical names and technical verbs (STE)

Technical names: object store, bucket, key, object, prefix, directory marker, multipart upload, part, connector, committer, commit protocol, task attempt, job attempt, task commit, job commit, job abort, algorithm 1, algorithm 2, magic committer, staging committer, manifest committer, manifest, `_SUCCESS`, `.pending`, `.pendingset`, credential provider, provider chain, instance role, request, `HEAD`, `GET`, `PUT`, `LIST`, `COPY`, `DELETE`, MinIO, S3A, ABFS, GCS, Ivy, table format, transaction log, dynamic partition overwrite, listing (the noun), timing (the noun), staging (modifier), pending (modifier). JDBC: JDBC driver, dialect, data source (DSv1), DSv2, JDBC catalog, namespace, cursor, fetch, batch, save mode, transaction, isolation level, staging table, upsert, pushdown, predicate, top-N, partition column, bound, stride.

Technical verbs: commit, abort, upload, complete (an upload), copy, rename, list, push (down), fetch, truncate, deprecate.

## STE results (`scripts/ste-check`)

| Chapter | Findings | `--allow` |
|---|---|---|
| ch87 | 0 | `listing staging pending timing Committing attempt` |
| ch88 | 0 | `staging timing` |
| ch66 | 0 (9 before my edit, all pre-existing: `planning`, `attempt`) | `planning attempt` |

Rules broken on purpose: quotes from the Spark documentation keep their words ("for performance", "not critical for job correctness", "uses Apache Hadoop Magic Committer for all S3 buckets by default"). `attempt` is the Spark and Hadoop term "task attempt".

## Environment and versions

- `pyspark==4.3.0` (uv project in `code/`), OpenJDK 17.0.21.1, local mode, 16-core laptop.
- Hadoop client in Spark 4.3.0: 3.5.0 (`hadoop.version` in `pom.xml` at `v4.3.0`).
- `org.apache.spark:spark-hadoop-cloud_2.13:4.3.0` from Maven Central, with `hadoop-aws-3.5.0`, `software.amazon.awssdk:bundle:2.35.4` (686 MB), `hadoop-azure-3.5.0`, `hadoop-gcp-3.5.0`, `analyticsaccelerator-s3-1.3.1` and others (Ivy resolution in `~/.ivy2.5.2/jars`).
- MinIO `minio/minio:RELEASE.2025-04-08T15-41-24Z`, client `minio/mc:RELEASE.2025-04-08T15-39-49Z`, container `book-ch87-minio`, ports 19055 (S3) and 19056 (console). Test keys `bookadmin` / `bookadmin-secret`, local only.
- PostgreSQL `postgres:16` (16.15), container `book-ch88-pg`, port 15515, user `book`, password `book-secret` (local only). `log_statement=all` is set with `ALTER SYSTEM` (not `-c`), so that scripts can lower it during bulk loads.
- PostgreSQL JDBC driver `org.postgresql:postgresql:42.7.11` from Maven Central.
- Spark UI port 4155 (4156 for one side run).
- No cloud account, no cloud credentials, no spend. ABFS and GCS are from docs and jar contents only; ch87 says which parts.

## Claim-to-source table (ch87)

| Claim | Source |
|---|---|
| Magic committer is the default with hadoop-cloud since 4.1, and the five configurations | `core/src/main/scala/org/apache/spark/SparkContext.scala` `enableMagicCommitterIfNeeded` at `v4.3.0`; `docs/core-migration-guide.md` line 61; `hadoop-cloud/src/test/scala/org/apache/spark/SparkContextSuite.scala` (SPARK-47618) |
| `fs.s3a.committer.name` default in Hadoop is `file` | `core-default.xml` in `hadoop-client-api-3.5.0.jar` |
| Spark sets `mapreduce.fileoutputcommitter.algorithm.version=1` if unset | `SparkHadoopUtil.appendSparkHadoopConfigs` at `v4.3.0` |
| Spark copies `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` into `fs.s3a.*` | `SparkHadoopUtil.scala` lines 395-495 at `v4.3.0`; `output/connect.txt` |
| Default credential provider chain | `core-default.xml` (Hadoop 3.5.0); `output/connect.txt` |
| Rename safety and cost table, consistency "as of 2021", committer recommendations, manifest committer quotes | `docs/cloud-integration.md` at `v4.3.0` |
| hadoop-cloud module has three Spark classes | `git ls-tree v4.3.0 hadoop-cloud` |
| `fs.gs.impl` = `org.apache.hadoop.fs.gs.GoogleHadoopFileSystem` in Hadoop 3.5.0; scheme-to-committer bindings for s3a, abfs, gs | `core-default.xml` and `mapred-default.xml` in `hadoop-client-api-3.5.0.jar` |
| GCS keys `fs.gs.auth.type`, `fs.gs.auth.service.account.json.keyfile`, `fs.gs.project.id`, `AuthenticationType` values | classes in `hadoop-gcp-3.5.0.jar` (`javap`, `strings`) |
| ABFS keys `fs.azure.account.auth.type`, `oauth.provider.type`, `oauth2.client.id/secret/endpoint` | `ConfigurationKeys` in `hadoop-azure-3.5.0.jar` |
| Staging conflict mode default `append`; magic enabled by default in Hadoop | `CommitConstants` in `hadoop-aws-3.5.0.jar` |
| AWS SDK bundle is 686 MB | `ls -la ~/.m2/repository/software/amazon/awssdk/bundle/2.35.4` |
| All committer numbers (wall, task commit, job commit, request counts, `_SUCCESS` sizes) | `output/committers.txt`, `committers.json` (bench lock, median of 3 after warm-up) |
| 12.8 s write in Step 3 | `output/connect.txt` (labelled "not a benchmark") |
| Failed-job results (7 part files, 70,000 rows) | `output/failure.txt` |
| Magic refuses dynamic partition overwrite | `output/dpo.txt`; `PathOutputCommitProtocol.scala` at `v4.3.0` |
| Magic committer log lines | `output/magic_log.txt` |
| `_SUCCESS` JSON content, 211 counters, 128 magic files | `output/success.txt`, `success-magic.json` |
| Manifest committer refuses s3a; its `_SUCCESS` and task manifest | `output/manifest.txt` |
| Read-side request counts and bytes | `output/listing.txt`; whole-file `Range` in `output/ranges.txt` |

## Claim-to-source table (ch88)

| Claim | Source |
|---|---|
| The option list, defaults and scopes | `JDBCOptions.scala` at `v4.3.0`; `docs/sql-data-sources-jdbc.md` at `v4.3.0` |
| `preferTimestampNanos` new in 4.3.0, needs `spark.sql.timestampNanosTypes.enabled` (internal, preview) | SPARK-57460 commit `f223b52bf70`; `JdbcUtils.getCatalystType`; `SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED`; option absent from the 4.2.0 docs |
| `JdbcDialects.getBuiltInDialect` is `@Since("4.3.0")` | `JdbcDialects.scala` at `v4.3.0`; `output/options.txt` |
| Derby deprecated since 4.2 | `docs/sql-migration-guide.md` line 63 at `v4.3.0` |
| PostgreSQL dialect fetch size 1000 when unset, auto-commit off | `PostgresDialect.getFetchSize` and `beforeFetch` at `v4.3.0` (also in `v4.2.0`, not in `v4.1.0`); `output/partitions.txt` (about 3,837 fetches per full read) |
| `pushDownJoin` option since 4.1.0; `spark.sql.optimizer.datasourceV2JoinPushdown` internal, default false, version 4.1.0; dialects with `supportsJoin` | `JDBCOptions.scala`, `SQLConf.scala`, `*Dialect.scala` at `v4.1.0` and `v4.3.0` |
| The jdbc format is DSv1 (`JDBCRelation extends BaseRelation with PrunedFilteredScan`) | `JDBCRelation.scala` at `v4.3.0` |
| PostgreSQL `isSupportedFunction` list | `PostgresDialect.scala` lines 46-54 at `v4.3.0` |
| `hint` only for MySQL, Oracle, Databricks | `supportsHint` overrides at `v4.3.0`; `output/options.txt` |
| `sessionInitStatement` is read only | `JDBCRDD.scala` (only use) at `v4.3.0`; docs scope "read"; `output/writes.txt` |
| Ten dialects through `ServiceLoader` | `META-INF/services/org.apache.spark.sql.jdbc.JdbcDialect` at `v4.3.0`; `output/options.txt` |
| TRUNCATE TABLE ONLY on PostgreSQL | `PostgresDialect.getTruncateQuery`, `isCascadingTruncateTable` at `v4.3.0`; `output/writes.txt` |
| Partition rows, SQL per task, skew | `output/partitions.txt` |
| fetchsize memory (2,592 MiB against 73 MiB) and times | `output/fetch.txt` (bench lock, second read in a fresh JVM, G1 old-gen pool peak after `resetPeakUsage`) |
| Pushdown SQL and fetch counts | `output/pushdown.txt`, `output/show_sql.txt` |
| Write statements, failed append (700 rows stay), upsert, MERGE error | `output/writes.txt` |
| batchsize numbers | `output/batch.txt` (bench lock, 1,000,000 rows, median of 3 after a warm-up) |
| Catalog statements | `output/catalog.txt` |
| Load DDL and 17.0 s load (not a benchmark) | `output/load.txt` |
| PostgreSQL runs READ UNCOMMITTED as READ COMMITTED | PostgreSQL documentation (transaction isolation); not measured |

## Open findings

1. **Docs gap: the magic committer default is not in `cloud-integration.md`.** The page (at `v4.3.0`) still tells users to "switch to the S3A committers" with three configurations, although Spark 4.1+ sets them itself when `hadoop-cloud` is present (`SparkContext.enableMagicCommitterIfNeeded`). It also still says the v2 algorithm is recommended "for performance". Only the core migration guide mentions the change. Candidate doc fix.
2. **Docs gap: the page does not mention `hadoop-gcp`.** Hadoop 3.5.0 ships its own `gs://` connector in `hadoop-gcp`, and `spark-hadoop-cloud_2.13:4.3.0` pulls it in. The page still links only Google's connector. Not tested against GCS.
3. **Docs gap (JDBC page): `truncate` says that `PostgresDialect` does not support it.** Spark 4.3.0 sent `TRUNCATE TABLE ONLY zone_copy` (and `... CASCADE` with `cascadeTruncate=true`) to PostgreSQL 16 (`code/ch88/output/writes.txt`). Source: `PostgresDialect.isCascadingTruncateTable` returns `Some(false)`, so `JdbcRelationProvider` truncates. Repro: write any DataFrame with `mode("overwrite").option("truncate", "true")` to a PostgreSQL table that exists, with `log_statement=all`.
4. **Docs gap (JDBC page): `pushDownJoin` and `tableComment` are options in `JDBCOptions.scala` at `v4.3.0`, but the page does not list them.** Join pushdown also needs the internal `spark.sql.optimizer.datasourceV2JoinPushdown=true`; it worked against PostgreSQL in `pushdown.txt`.
5. **Docs gap (JDBC page): `fetchsize` default is shown as 0.** The PostgreSQL dialect uses 1000 when the option is unset (since 4.2.0). Users who read the page would think PostgreSQL reads load all rows.
6. **Observation, not a bug: with algorithm 2, a failed job leaves committed task output and no `_SUCCESS`** (`output/failure.txt`). This is documented Hadoop behaviour; the chapter puts it in a caution.

## Nav entries (Part VI, after ch31)

```yaml
      - "Spark on object storage": chapters/ch87-object-storage.md
      - "Databases over JDBC": chapters/ch88-jdbc.md
```

## Roadmap rows (Part VI)

```markdown
| 87 | [Spark on object storage: S3A, ABFS, GCS and the output committers](chapters/ch87-object-storage.md) | Object stores against file systems, the `hadoop-cloud` module, S3A against MinIO, credential providers, FileOutputCommitter v1 and v2, the magic and staging committers (default since 4.1), five committers measured, a failed job, `_SUCCESS` files, the manifest committer for ABFS and GCS, read-side requests |
| 88 | [Databases over JDBC: partitioned reads, pushdown and writes](chapters/ch88-jdbc.md) | The options of 4.3.0 from source, partitioned reads and skew, `predicates`, `fetchsize`, pushdown in DSv1 and DSv2 (filters, aggregates, limit, offset, top-N, sample, joins), `sessionInitStatement` and `query`, the write path, upserts, dialects, `JDBCTableCatalog`, against PostgreSQL in Docker |
```

## Clips

Cut list `book/media/clips/clips-v3-sources.tsv`. Rendered with `film render code/film/chNN.tape --format chapter` (no bench lock). Each published clip is the beat clip with repeated still frames removed (`ffmpeg -vf mpdecimate,setpts=N/30/TB`), an 8 s hold on the last frame (`tpad`), CRF 28; the poster is the last frame. The films show their own runs; captions quote no numbers.

Animations: HyperFrames through `code/common/build_anim.sh chNN <name> <poster_s>` (paper theme, landscape, 1280x720). Sources in `code/chNN/anim/<name>/` with `storyboard.md` and `data.json`.
