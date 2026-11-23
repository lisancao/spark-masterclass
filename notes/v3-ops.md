# v3 notes: operating Spark (branch `v3-ops`)

Writer scope (gap audit rows "Deploying, operating and securing", items 1, 2, 21, 28, 30, 31 and the Mesos and minimums rows of section 3): submitting applications, security, building from source, and extensions to ch02, ch06, ch73, ch74, ch77.

## Scope and what changed

New chapters (provisional numbers; the integrator renumbers):

| File | Title | Code | Media |
|---|---|---|---|
| `book/chapters/ch70-submitting-applications.md` | Submitting applications: spark-submit, standalone, YARN and Kubernetes | `code/ch70/` (`env.sh`, `standalone.sh`, `status.py`, `app.py`, `submit.sh`, `ha.sh`, `k8s/`, `figures.py`, `anim/failover`) | figures `ch70-deploy-modes`, `ch70-failover`, `ch70-pod-template`; animation `ch70-anim-failover`; clips `ch70-submit`, `ch70-ha` |
| `book/chapters/ch71-securing-spark.md` | Securing Spark | `code/ch71/` (`auth.sh`, `proxy.py`, `probe.py`, `k8s_auth.sh`, `podenv.py`, `ssl_acl.sh`, `filter/book/HeaderUserFilter.java`, `io_encryption.py`, `redaction.py`, `kerberos.sh`, `figures.py`, `anim/wire`) | figures `ch71-surfaces`, `ch71-eavesdrop`; animation `ch71-anim-wire`; clips `ch71-redaction`, `ch71-disk` |
| `book/chapters/ch92-building-spark.md` | Building Spark from source | `code/ch92/` (`build.sh`, `Dockerfile`, `inspect.sh`, `figures.py`, `anim/build`) | figures `ch92-module-times`, `ch92-jars`; animation `ch92-anim-build`; clip `ch92-inspect` |

Placement: ch70 and ch71 go in Part XII, ch70 before ch76 (operator). ch92 is appendix-style; put it at the end of Part XII or after ch91 in Part XIV.

Existing chapters changed (numbers kept):

- **ch02**: one row `mesos://...` in the cluster manager table, a short paragraph (deprecated in 3.2, removed in 4.0, link to the core migration guide), and the real `spark-submit --master mesos://` error (`code/ch02/mesos.sh`, `code/ch02/output/mesos.txt`). Link to ch70.
- **ch06**: new section "The version minimums of Spark 4.3" before "Get the data" (`code/ch06/minimums.py`, `output/minimums.txt`), a table of floors with sources, the pandas `<3.0.0` doc range against the metadata without an upper limit; one bullet in the summary.
- **ch73**: new Step 11 "ADD FILE, ADD ARCHIVE, ADD JAR and LIST in SQL" before "Choose a method" (`code/ch73/sql_resources.py`, `code/ch73/sqlres/book/Shout.java`, `output/sql_resources.txt`); one row in the "Choose a method" table; one summary bullet. No renumbering (Steps 1 to 10 unchanged).
- **ch74**: Step 11 extended with a subsection "Read a JSON log as a DataFrame" (`code/ch74/logs_df.sh`, `logs_df_app.py`, `logs_df.py`, `output/logs_df.txt`); one sentence added to a summary bullet.
- **ch77**: new Steps 12 to 15 after Step 11, before "When to use which": external shuffle service, push-based shuffle (YARN only), graceful decommission, stage-level scheduling with a GPU discovery script (`code/ch77/ops/`: `ess.sh`, `ess_app.py`, `decom.sh`, `decom_app.py`, `stage_level.sh`, `stage_level.py`, `getGpus.sh`; outputs `ess.txt`, `decom.txt`, `stage_level.txt`). Three summary bullets, three lines in the command list, one sentence in "When to use which".

Shared files: none in `code/common/` changed. `code/film/ch70.tape`, `ch71.tape`, `ch92.tape`; clip list `book/media/clips/clips-ops.tsv` (ch70 rows added after the last film render).

Technical names and verbs used (STE): spark-submit, master URL, deploy mode, driver, executor, worker, master, cluster manager, standalone, YARN, Kubernetes, kind, pod, pod template, service account, Role, RoleBinding, ConfigMap, headless service, volume, RBAC, recovery mode, REST submission server, shuffle, shuffle service, decommission (verb and noun), migrate, ResourceProfile, discovery script, secret, keytab, principal, delegation token, KDC, TLS, AES, GCM, key store, trust store, servlet filter, ACL, redaction (noun), redact, MDC, profile (Maven), distribution, jar, tgz, compile, build, submit, deploy.

## Claim-to-source table

| Claim | Source |
|---|---|
| Cluster mode refused for Python/R on standalone, for local, shells, spark-sql, Thrift server | `SparkSubmit.scala:299-312` at v4.3.0; `ch70/output/submit-cluster-python.txt` |
| `spark.master.rest.enabled` default `true` on the master, `false` on the submit side | `package.scala:2186`, `SparkSubmitArguments.scala:103` |
| REST port default 6066 | `package.scala:2197` |
| 4.3 standalone RPC gateway filters the environment, `spark.standalone.submit.filterEnvironment` | `package.scala:2738-2752`, core migration guide "Upgrading from Core 4.2 to 4.3" |
| Recovery modes ZOOKEEPER, FILESYSTEM, ROCKSDB, CUSTOM, default NONE | `Master.scala:178-191`, `Deploy.scala:25-28` |
| `spark.deploy.recoveryCompressionCodec`, `recoveryTimeout` since 4.0; `recoverySerializationFilter` 4.3.0 | `Deploy.scala:30-74` |
| Master states STANDBY, RECOVERING, COMPLETING_RECOVERY, ALIVE; FILESYSTEM uses MonarchyLeaderAgent (immediate leader) | `RecoveryState.scala`, `RecoveryModeFactory.scala:68` |
| Failover times 17.8 s, 28.6 s, 31.2 s, 54.1 s, recovery 392 ms | `ch70/output/ha.txt` |
| YARN needs HADOOP_CONF_DIR or YARN_CONF_DIR | `SparkSubmitArguments.scala:310-314` |
| YARN uploads `$SPARK_HOME/jars` without `spark.yarn.jars`/`archive` | `Client.scala:706-712` |
| ApplicationMaster vs ExecutorLauncher | `Client.scala:1142-1147` |
| Volume types hostPath, emptyDir, nfs, persistentVolumeClaim; keys mount.path, mount.readOnly, mount.subPath | `Config.scala:948-957` (k8s) |
| Executor service account fallback to the driver's | `Config.scala:390-395` (k8s) |
| `spark.kubernetes.executor.useDriverPodIP` default true since 4.1; SparkContext uses bindAddress | `Config.scala:167-172` (k8s), `SparkContext.scala:453-460` |
| Non-JVM memory overhead factor 0.4 (1433Mi for 1g) | `Constants.scala:110` (k8s); `ch70/output/k8s-cluster.txt` |
| Driver pod runs `spark-submit --deploy-mode client` | `resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh:86` |
| No-RBAC error, cluster mode pods, client-mode results | `ch70/output/k8s-no-rbac.txt`, `k8s-cluster.txt`, `k8s-client-default.txt`, `k8s-client-host.txt`, `k8s-client-in-cluster.txt` |
| Standalone secret required; file secrets only on k8s | `SecurityManager.scala:346-400` |
| AES default `AES/CTR/NoPadding`, authEngineVersion default 1, saslFallback default true, crypto default false | `TransportConf.java:213-238` |
| TLS preferred, AES legacy, RPC SSL not inherited, both on: TLS wins | `docs/security.md` lines 154-196 at v4.3.0 |
| Proxy byte counts and strings (7707 / 21 / visible; 13713 / 0) | `ch71/output/auth.txt` |
| `_SPARK_AUTH_SECRET` in the executor pod spec | `BasicExecutorFeatureStep.scala:138-142` (k8s); `ch71/output/k8s_auth.txt` |
| Executor startup confs exclude spark.master | `SparkConf.scala:825-833` |
| UI HTTPS port = port + 400 | `JettyUtils.scala:435` |
| ACL passes when user is null | `SecurityManager.scala:402-411` |
| JWSFilter checks a token, sets no user; `requireExpiration` param | `JWSFilter.scala` at v4.3.0 |
| Redaction default regex `(?i)secret|password|token|access[.]?key|credential`; SQL options regex `(?i)url`; SQL string regex falls back to core | `package.scala:1326-1340`, `SQLConf.scala:5566-5582` |
| LZ4Block bytes and +64/+16 bytes with encryption | `ch71/output/io_encryption.txt` |
| `spark.io.encryption.keySizeBits` 128/192/256, default 128 | `package.scala:1204-1208` |
| Kerberos providers hadoopfs, hive, hbase, kafka; `spark.kerberos.renewal.credentials` keytab/ccache; standalone not supported | `docs/security.md` 938-1000, `package.scala:913-919` |
| `spark.security.directCredentialProviders.enabled` new in 4.3.0 | `package.scala:1695`, `docs/security.md` table (version 4.3.0) |
| Connect token config and interceptor | `Connect.scala:388-401`, `SparkConnectService.scala:445` |
| Build 26:43 min, 1659 s, 41 modules, 16 cores | `ch92/output/reactor.txt`, build log (scratch), `nproc` |
| zip needed (SPARK-53327) | `dev/make-distribution.sh:221-224`; `ch92/output/first-build-zip.txt` |
| hadoop-provided sets hadoop.deps.scope=provided | `assembly/pom.xml:329-332` |
| Official build flags | `RELEASE` of the official tarball; `ch92/output/inspect.txt` |
| Maven 3.9.15, Java 17/21/25 for the build | `pom.xml:124-127`, `docs/building-spark.md:30` |
| One ESS per host | worker log, `ch77/output/ess.txt` |
| Push-based shuffle conditions | `Utils.scala:2581-2612` |
| Decommission removes outputs of the host | `DAGScheduler.scala:4188-4197`; `ch77/output/decom.txt` |
| ResourceProfile support matrix | `ResourceProfileManager.scala:61-100` |
| fractional `spark.task.cpus` in 4.3 | core migration guide 4.2 to 4.3 |
| Java 17/21/25, Python 3.11+, Java 25 < 25.0.3 deprecated | `docs/index.md:37-38` |
| PyArrow 18.0.0, pandas 2.2.0, NumPy 1.23.2, gRPC 1.76.0 | `python/packaging/classic/setup.py:161-167`, `pyspark/sql/pandas/utils.py:25,74,111`; `ch06/output/minimums.txt` |
| pandas `<3.0.0` in the docs table | `python/docs/source/getting_started/install.rst:238` |
| Mesos removed in 4.0 | `docs/core-migration-guide.md:79` |
| ADD/LIST commands call SparkContext methods | `sql/core/.../command/resources.scala:30-140`, `SessionState.scala:171-196` |
| SPARK_LOG_SCHEMA | `python/pyspark/logger/logger.py:27`, `LogUtils.scala:34`; `ch74/output/logs_df.txt` |

## Environment

- Spark 4.3.0: tarball `spark-4.3.0-bin-hadoop3` from `$BOOK_DATA/tarball` (Chapter 6) for `spark-submit`, `sbin/` and examples; pyspark 4.3.0 in `code/.venv`, Python 3.12.3.
- OpenJDK 17.0.21.1 on the host. Build container: `maven:3.9-eclipse-temurin-17` plus `zip` (`ops80/spark-build:jdk17`, local image built from `code/ch92/Dockerfile`).
- kind cluster `ops80` with `kindest/node:v1.37.0` (1 control-plane, 2 workers), image `spark-masterclass/spark:4.3.0-book` (Chapter 76's image: Python 3.10.12, PyArrow 18.1.0).
- Ports: SPARK_UI_PORT 4151 (HTTPS 4551 in Step 3 of ch71); standalone master 7151, web 8151, workers 8152+, REST 6151; shuffle service 7351; proxy 7161; client-mode driver ports 7178/7179. Connect port 15111 not used (no Connect server in this slice).
- Outputs replace `$HOME` with `~` and the machine's host name with `laptop` (`sed s#systemsnull#laptop#g` in ch70 outputs; the ch70 film has a hidden `scrub` function that does the same).

## Open findings

1. **Kubernetes file-based auth secret does not start executors** (4.3.0). `spark.authenticate=true` with `spark.authenticate.secret.driver.file` and `spark.authenticate.secret.executor.file` (both pointing at a mounted Kubernetes Secret): the driver starts, every executor fails with `IllegalArgumentException: A secret key must be specified via the spark.authenticate.secret config`. Cause from source: the executor bootstrap conf only has `isExecutorStartupConf` keys (`SparkConf.scala:825`), which exclude `spark.master`; `SecurityManager.secretKeyFromFile` (`SecurityManager.scala:386`) returns `None` when `spark.master` is unset, so `getSecretKey` throws. Tried workaround `spark.executor.extraJavaOptions=-Dspark.master=k8s://...`: refused with `INVALID_SPARK_CONFIG.INVALID_EXECUTOR_SPARK_OPTIONS`. Repro: `code/ch71/k8s_auth.sh file` on the kind cluster of `code/ch70/k8s/cluster.sh up`. Not checked against other versions or JIRA. Do not call it a bug in the book; run spark-bug skill before filing.
2. **Doc gap:** the doc string of `spark.standalone.submit.filterEnvironment` says spark-submit uses the RPC gateway "unless spark.master.rest.enabled is set to true", while the master-side default of that key is `true` (`package.scala:2189`). Both are right (the submit side reads it with default `false`, `SparkSubmitArguments.scala:103`), but the two defaults of one key confuse readers. Possible docs clarification.
3. **Doc gap:** PySpark install page lists pandas `>=2.2.0,<3.0.0`, but the package metadata has `pandas>=2.2.0` without an upper bound.
4. **Observation (not a bug):** a worker decommission on a single host removes the map outputs of every executor of that host (`DAGScheduler.scala:4188-4197`), so the migrated shuffle blocks did not save the rerun in the one-host lab (`ch77/output/decom.txt`). Expected by design; the chapter explains it.
5. **Make-distribution needs `zip`**, which the `maven:3.9-eclipse-temurin-17` image lacks; the script fails after the full Maven build (`ch92/output/first-build-zip.txt`). Possible improvement: check for `zip` at the start of `make-distribution.sh`.
6. **Observation:** with `spark.authenticate` on and no secret, the driver retries for minutes instead of failing fast (`ch71/output/auth.txt`, killed by `timeout 60`).

## Not verified / not run

- YARN (no Hadoop cluster): ch70 Step 5 and push-based shuffle on YARN are from source only, and say so.
- ZooKeeper recovery mode with standby masters: explained, not run (FILESYSTEM ran).
- Kerberos login and delegation tokens: no KDC; only the pre-login checks of `kerberos.sh` ran.
- Connect authentication and TLS: not re-run; ch71 Step 8 summarizes and links ch30.
- Client mode from the laptop against kind: blocked by the host firewall (INPUT policy DROP); the chapter shows the timeout and runs client mode inside the cluster instead.
- Stage-level scheduling used pretend GPU addresses from a discovery script, not real GPUs.
- SBT build, `docker-image-tool.sh`, `--pip`: described from docs, not run.
- No timings are quoted as benchmarks; the build time is one run with downloads and is labelled so. No bench lock taken.

## STE counts (scripts/ste-check)

| Chapter | Findings | Allow words |
|---|---|---|
| ch70 | 0 | `--allow Running Submitting` (doc titles in Further reading) |
| ch71 | 1 (paragraph count inside an output block that ste_prep does not strip; false positive) | none |
| ch92 | 0 | `--allow inspect Building` (`inspect.sh` script name; doc title) |
| ch02 | 10, same as before the change except none added | none |
| ch06 | 0 | none |
| ch73 | 5, same as before (pre-existing) | none |
| ch74 | 1, same as before (pre-existing "Debugging" title) | none |
| ch77 | 0 with `--allow tracking scheduling docstring Running` (the pre-existing text needs the same list: shuffle tracking, gang scheduling are technical names) | as listed |

Rules broken on purpose: quoted log lines and config names inside sentences count as single words; no other deliberate breaks.

## Clips and media

- Clip list `book/media/clips/clips-ops.tsv` (clip, source under `code/film/renders/`, start, end, poster): `ch70-submit`, `ch70-ha`, `ch71-redaction`, `ch71-disk`, `ch92-inspect`. Cut with `code/common/cut_clips.sh book/media/clips/clips-ops.tsv`.
- Animations: `code/common/build_anim.sh ch70 failover 11.5`, `ch71 wire 11`, `ch92 build 11.8`.
- Figures: `uv run python ch70/figures.py`, `ch71/figures.py`, `ch92/figures.py` (all read `output/`).

## Clusters and containers

All removed at the end: kind cluster `ops80` deleted, standalone master and workers stopped, build container exited (`--rm`). The image `ops80/spark-build:jdk17` remains as a local image (about 0.5 GB); remove with `docker rmi ops80/spark-build:jdk17` if not wanted.

## Nav entries (mkdocs.yml)

```yaml
  - "Running Spark in production":
      ...
      - "Observability": chapters/ch74-observability.md
      - "Submitting applications": chapters/ch70-submitting-applications.md
      - "Spark on Kubernetes with the operator": chapters/ch76-kubernetes-operator.md
      - "Orchestration": chapters/ch77-orchestration.md
      - "Securing Spark": chapters/ch71-securing-spark.md
      - "Building Spark from source": chapters/ch92-building-spark.md
```

## Roadmap rows (Part XII)

```markdown
| 80 | [Submitting applications: spark-submit, standalone, YARN and Kubernetes](chapters/ch70-submitting-applications.md) | Master URLs and deploy modes, spark-submit flags, a standalone cluster with FILESYSTEM recovery killed during a job, the REST submission server, YARN from the source, native Kubernetes with RBAC, volumes, pod templates and client mode, against the operator |
| 81 | [Securing Spark](chapters/ch71-securing-spark.md) | spark.authenticate, AES and TLS for RPC with a proxy that reads the wire, UI TLS, UI ACLs behind a servlet filter, local disk encryption, redaction, Kerberos and delegation tokens, Connect. **4.3:** token providers without Kerberos |
| 82 | [Building Spark from source](chapters/ch92-building-spark.md) | Maven profiles, make-distribution.sh, a hadoop-provided build of v4.3.0 in Docker, SPARK_DIST_CLASSPATH |
```

Roadmap edits for existing rows: ch77 "Covers" add "the external shuffle service, push-based shuffle, graceful decommission, stage-level scheduling"; ch74 add "JSON logs as a DataFrame"; ch73 add "ADD FILE/ARCHIVE/JAR in SQL"; ch06 add "the version minimums of 4.3".

## Cross-reference notes for the integrator

- ch70 links Chapter 2, 6, 8, 24, 73, 76, 77, 71, 92 by current numbers; ch71 links 18, 24, 63, 80; ch92 links 6, 77. When renumbering, ch70/81/82 become their final numbers through `scripts/renumber.py`.
- ch02 now links ch70; ch77 links ch70.
