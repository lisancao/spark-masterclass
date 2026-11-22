# Part VIII notes: Running Spark in production

Chapters (old numbers in the file names; final numbers in brackets, from `notes/renumber.md`):

| File | Final | Title |
|---|---|---|
| `book/chapters/ch43-determinism-retries.md` | 43 | Determinism, retries and fault injection |
| `book/chapters/ch44-shipping-dependencies.md` | 44 | Shipping Python dependencies |
| `book/chapters/ch45-observability.md` | 45 | Observability: listeners, event logs, metrics and profilers |
| `book/chapters/ch46-connect-in-production.md` | 46 | Spark Connect in production: deployment patterns |
| `book/chapters/ch47-kubernetes-operator.md` | 47 | Spark on Kubernetes with the Spark Kubernetes Operator |
| `book/chapters/ch49-pipelines-with-agents.md` | 49 | Writing Spark pipelines with agents |

## Cross-references (for the integrator)

- References inside Part VIII use the old numbers and file names (`ch43-...` to `ch49-...`, "Chapter 43" to "Chapter 49"). Rename them with the map.
- References to other parts use the **final** numbers, as `assignments.md` says, and link to `../roadmap.md` (the files do not exist in this worktree): Chapter 20 (Python UDFs, transpiler), Chapter 23 (Project Feather), Chapter 30 (tables, in ch43), Chapter 32 (SDP model and CLI), Chapter 35 (metric views), Chapter 48 (orchestration), Chapter 49 (agents, from ch46). **Do not apply the old-to-new map to these.** Exception: "Chapter 49" in ch46 Step 9 means old ch49 (final 49) and is already final; and "Chapter 1" and "Chapter 4" are Part I (unchanged).

## Technical names and verbs

- Names: Spark Connect, Connect server, session, `SessionHolder`, operation ID, job tag, reattachable execution, `ReattachExecute`, `ReleaseExecute`, task attempt (`attempt`), stage attempt, fetch failure, shuffle checksum, determinism level, accumulator, `Observation`, commit protocol, py-file, archive, artifact, files root, Python worker, daemon, planner worker, `PYSPARK_PYTHON`, listener bus, event log, History Server, REST API, metrics system, Prometheus servlet, UDF profiler, data source profiler, py-spy, structured logging, `PySparkLogger`, FIFO, FAIR, pool, pre-shared token, Envoy, TLS, persistent local server, discovery file, single-task execution, Project Feather, sandbox, kind, Helm, chart, CRD, `SparkApplication`, `SparkCluster`, operator, driver pod, executor pod, state machine (`Submitted`, `DriverRequested`, `RunningHealthy`, `Succeeded`, `Failed`, `ScheduledToRestart`, `ResourceReleased`, `TerminatedWithoutReleaseResources`), restart policy, retain policy, Spark Declarative Pipelines (SDP), dry-run, flow, materialized view, skill, metric view, `MEASURE()`, namespace, ANSI mode.
- Verbs: retry, reattach, release, shuffle, spill, cache, commit, checkpoint, transpile, pickle, unpack, ship, profile, sample, interrupt, authenticate, forward (a port), deploy, restart, submit, register, resolve, validate, dry-run.

## STE check results

`scripts/ste-check <file> description --allow <words>` (the updated wrapper); the counts below are the findings left after the rewrite. Each is a literal title or a quotation, which STE does not change.

| Chapter | Findings | What is left, and why |
|---|---|---|
| ch43 | 0 (allow: `attempt`, `transpiled`) | `attempt` is the Spark technical name (task attempt, `attemptNumber`) |
| ch44 | 2 (allow: `packaging`, `planner`) | a 7-sentence paragraph the lint counts from version dots (`pandas>=2.2.0` list); the doc title "Running Spark on Kubernetes" |
| ch45 | 1 (allow: `planner`) | doc title "Debugging PySpark" |
| ch46 | 2 | `running` in pasted script output that the prep script does not strip; doc title "Job scheduling" |
| ch47 | 1 (allow: `attempt`, `attempts`) | doc title "Running Spark on Kubernetes" |
| ch49 | 4 | talk title "Getting your agents ..." (two times), a quoted slide sentence with "was removed", doc title "programming guide" |

Rules broken on purpose: quotations of the talk's slides keep their wording (ch49), and the tables of study numbers keep the slides' text in quotes.

## Environment

- pyspark 4.3.0 (`pyspark[connect]==4.3.0` from `code/pyproject.toml`), OpenJDK 17.0.21.1, Python 3.12.3, uv 0.11.10, a 16-core Linux machine.
- Ports: `SPARK_UI_PORT=4028`; Connect server 10280 (UI 4128) via `code/ch43/server.sh`; flaky proxy 10281; Envoy TLS 10282; sandbox servers 10284/10285 (UI 4129/4130); ch49 scope server 10286 (UI 4131); kind port-forward 10283 (UI 4129); History Server 18028. The 4.3 persistent local server (ch46 Step 7) and `spark-pipelines` (ch49) use their default port 15002.
- `server.sh` moved from 15058/4158 to the assigned 10280/4128.
- ch44: `apache/spark:4.3.0-python3` image (Ubuntu 22.04.5, Python 3.10.12, OpenJDK 21.0.13, no PyArrow/pandas); `venv-pack` from PyPI; `ujson` 6.1.0 and `tabulate` 0.10.0 wheels (manylinux2014, cp312).
- ch46: Envoy `envoyproxy/envoy:v1.31.5` in Docker; self-signed certificate made by `openssl` in each run.
- ch47: kind with `kindest/node:v1.37.0` (cluster name `part08`, 1 control plane + 2 workers); Helm chart `spark-kubernetes-operator/spark-kubernetes-operator` **1.9.0** = operator image `apache/spark-kubernetes-operator:1.1.0` (its `gradle/libs.versions.toml` pins `spark = "4.3.0"`); book image `spark-masterclass/spark:4.3.0-book` = `apache/spark:4.3.0-python3` + `pyarrow==18.1.0` + `pandas==2.2.3` (`code/ch47/Dockerfile`). The cluster was deleted at the end (`ch47/cluster.sh down`).
- ch49: `spark-pipelines` needs PyYAML, which `pyproject.toml` does not list; `dryrun.sh` runs it with `uv run --with pyyaml` instead of editing the shared `pyproject.toml`. Metric view data and YAML copied from `~/spark-demos/demos/01_metrics_views/` (1 MB).
- Timings quoted (all under `flock .../data/.bench.lock`, new DataFrame for each timed run): ch45 `profilers.py` (median of 5 after a warmup), ch46 `feather_server.py` (median of 20 after a warmup), ch46 `local_reuse.py` (single process start times). ch46 `contention.py` and `sandbox.py` ran without the lock (their numbers are sleep-dominated or start-up times; the text gives them as approximate, with all runs shown). The ch46 Feather clip ran while other renders used the machine, and its caption says so.

## Coverage

### MD52 determinism_retries (ch43)

| Topic | Section |
|---|---|
| Table of re-execution causes with scheduler code locations; AQE not a cause | "When Spark runs your code again" |
| Local mode hides retries (`MAX_LOCAL_TASK_FAILURES`, `local[N,F]`), no speculation locally | Step 1 |
| `rand(seed)` vs Python `random` across attempts; `Nondeterministic.initialize`; `ResolveRandomSeed`; `PYTHONHASHSEED=0`; `monotonically_increasing_id` | Steps 1, 2, 7 |
| Catalyst and nondeterminism (10 vs 1000 calls, plans, `INVALID_NON_DETERMINISTIC_EXPRESSIONS`) | Step 3 |
| 4.3 UDF transpilation and nondeterminism | Step 3 |
| RDD `outputDeterministicLevel`, `sortBeforeRepartition` | Step 4 |
| Lost shuffle three ways; order-independent checksums (SPARK-51756), rollback/abort | Step 5 |
| Accumulators vs re-execution, `observe()`, cache | Step 6 |
| Connect re-analyzes each action; client-side seeds | Step 9 |
| Commit protocol vs reruns; `partitionOverwriteMode`; keyed side effects | Step 8 |
| Connect operation IDs, reattach (4.3) | Step 10 |
| Fault injection toolkit; checkout exercise (fragile vs correct job) | Step 11, toolkit table |
| Gotchas: DDL return type at import, UDF pickles globals once | Step 3 |

### MD53 shipping_dependencies (ch44)

| Topic | Section |
|---|---|
| Three places Python runs | "Three places where Python runs" |
| Worker `sys.path` layers; local-mode project-folder trap; `SparkFiles.getRootDirectory` | Step 1 |
| Interpreter selection; `spark.pyspark.python` ignored in the builder; `PYTHON_VERSION_MISMATCH` | Step 2 |
| Thin env meets Arrow UDFs; minimum versions; 4.3 transpilation caveat | Step 3 |
| py-files, zip layout, `.whl` not on the path | Step 4 |
| Include list captured once (`pythonIncludes`), reused-worker residue | Step 5 |
| No reload in reused workers | Step 6 |
| Native code in a zip vs an archive; manylinux tags | Step 7 |
| Archives, `venv-pack` recipe failing in local mode, portable site folder; `uv run` recipe | Step 8 |
| Connect artifacts: per-session folders and workers, immutability, refusals; Classic `addArtifacts` | Step 9 |
| Kubernetes image facts (4.3.0 image re-read) | Step 10 |
| Decision table and checklist | "Choose a method" |
| Left out: notebook-scoped libraries (Databricks feature, not OSS Spark); Connect lifecycle gotchas `CANNOT_CONFIGURE_SPARK_CONNECT_MASTER` and in-process `stop()` (covered in Part I ch04 and ch46 Step 6 for the server side) | |

### MD54 observability (ch45)

| Topic | Section |
|---|---|
| Bus/queues/metrics architecture; bounded queues drop events | "Where the numbers come from" |
| Task metrics vs SQL metrics (`SQLMetric`, Python metric names) | Step 2 |
| REST API, `taskSummary` as a skew detector, `/sql` | Step 2 |
| Metrics system, Prometheus servlet, namespace, executor endpoint, `/metrics/json` version | Step 3 |
| Event log settings (rolling, zstd), folder layout, appstatus marker; Spark reads `.zstd` | Step 4 |
| Event census; messy TaskEnd JSON; Python time from accumulables | Steps 4, 5 |
| Diagnosis exercise (skew/spill/python), equal to REST | Step 6 |
| 4.3 Connect REST endpoints; job tags | Step 7 |
| History Server from a pip install | Step 8 |
| Python `StreamingQueryListener` with `observe()` drift; `lastProgress` UUID trap; Connect listeners run in the client | Step 9 |
| Batch listeners (Java, `spark.extraListeners`) | Step 9, last paragraph (described, not run; see open findings) |
| UDF profiler overhead; data source profiler covers planning | Step 10 |
| py-spy with `--subprocesses`, ptrace_scope | Step 10 |
| Structured logging JVM vs Python | Step 11 |
| Left out: accumulators as custom counters (ch43 Step 6 covers them); custom metrics API for Python data sources (Part IX) | |

### LS11 spark_connect, server side (ch46, with ch04 for the client)

| Topic | Section |
|---|---|
| Server starts, `remote()` vs `spark.api.mode`, `local*` embedded server | ch04; ch46 Step 7 (persistent local server, 4.3) |
| Connection string params (`token`, `use_ssl`, `user_id`) | ch46 Steps 4, 5 |
| Sessions, per-session config, isolation | ch46 Steps 1, 2 |
| `addArtifacts` | ch44 Step 9 |
| `addTag`/`interruptTag`/`interruptAll` | ch46 Step 6 |
| Remote UDFs need the server's libraries and Python | ch44 Steps 3, 9; ch47 Step 5 |
| Listeners run client-side | ch45 Step 9 |
| 4.3 operation IDs and REST endpoints | ch43 Step 10, ch45 Step 7 |
| Left out (client-side, covered in Part I ch04 or Part II): Classic-only API table, `zipWithIndex`, `toJSON`, local relation limits | |

### New topics

- Connect deployment patterns: ch46 (patterns, isolation, scheduler contention, token, TLS proxy, lifecycle, persistent local server, Feather on the server, sandbox per agent, two client traps).
- Spark Kubernetes Operator: ch47 (install, `SparkApplication`, restart policy, `SparkCluster`, Connect server on Kubernetes), run on kind.
- Writing pipelines with agents (OSA Con 2026): ch49 (study, four checks, ANSI, dry-run, what dry-run misses, scope, version-pinned skill, metric views, costs).

## Claim-to-source table (selected; every number in the text is in the named output file)

| Claim | Source |
|---|---|
| ch43 all numbers | `code/ch43/output/*.txt`; scheduler source paths in the text |
| ch44 worker paths, versions, errors | `code/ch44/output/workers.txt`, `pyfiles.txt`, `archives.txt`, `artifacts.txt`, `transpile.txt`, `image.txt` |
| pyspark 4.3.0 `python_requires=">=3.11"`; minimums pandas 2.2.0, pyarrow 18.0.0, numpy 1.23.2 | `python/packaging/classic/setup.py` at v4.3.0 |
| `PACKAGE_EXTENSIONS`, `pythonExec` line 341 | `python/pyspark/core/context.py` at v4.3.0 |
| Transpiler runs at UDF creation; conditions | `python/pyspark/sql/udf.py` lines 209-297 at v4.3.0; `transpile.txt` |
| `SPARK_JOB_ARTIFACT_UUID`, per-session worker cwd | `PythonRunner.scala` 627, `PythonWorkerFactory.scala` 272 at v4.3.0 |
| ch45 stage/REST/event log numbers | `code/ch45/output/observed_app.txt`, `eventlog.txt` (same application run) |
| Event log defaults (rolling 128m, compress, zstd); queue capacity 10000 | `core/.../internal/config/package.scala` at v4.3.0; `docs/core-migration-guide.md` |
| `spark.ui.prometheus.enabled` internal, default true | `core/.../internal/config/UI.scala` line 127 at v4.3.0 |
| Connect REST endpoints | `ConnectResource.scala` at v4.3.0; `connect_obs.txt` |
| Profiler ratios 21.4/4.2/0.8 | `profilers.txt` (lock held, median of 5) |
| `wrap_perf_profiler` per-call wrapping | `python/pyspark/worker.py` line 519 at v4.3.0 |
| Connect listener bus in the client | `python/pyspark/sql/connect/streaming/query.py` line 267; `stream_listener_connect.txt` |
| ch46 isolation/contention/auth/lifecycle/Feather/reuse/sandbox numbers | `code/ch46/output/*.txt` |
| Token interceptor, `defaultSessionTimeout` 60m, tombstones 1000 | `SparkConnectService.scala` 444, `Connect.scala` 134-170, 389 at v4.3.0 |
| Token forces TLS for non-localhost hosts | `python/pyspark/sql/connect/client/core.py` 596-643 at v4.3.0 |
| SESSION_CHANGED then NO_ACTIVE_SESSION | `core.py` 2429 and 964 at v4.3.0 |
| Tags are per thread | `python/pyspark/sql/session.py` 2492 docstring at v4.3.0 |
| `F.udf` reads the active session's Arrow config | `python/pyspark/sql/connect/udf.py` 50-73 at v4.3.0 |
| ch47 states, pods, logs | `code/ch47/output/*.txt` |
| Operator adds `spark.authenticate`, `spark.io.encryption.enabled`, master | operator 1.1.0 `SparkAppSubmissionWorker.java` 166-183; `inspect.txt` |
| Restart config semantics | operator 1.1.0 `docs/spark_custom_resources.md` |
| ch49 study numbers | OSA Con 2026 deck `~/Documents/spark_content/decks/osa-con-2026/index.html` (slides 3, 4, 6, 8, 16, 17, 27, 28, 29, 30, 38), quoted as stated |
| ch49 ANSI, dry-run, epoch, idioms, scope, metric view outputs | `code/ch49/output/*.txt` on 4.3.0 (metric view numbers equal the deck's 4.2 numbers) |

Left out of ch49 on purpose: the deck's executor-second figures ("up to about 521 executor-seconds", "about 34x", appendix "596 / 17", "~1000×"). The deck labels the appendix as "A separate 48-cell sweep on EKS", but the memory note `h3-compute-provenance` says these numbers were measured in-process, not on live EKS, and `h3-compute-waste-methodology` says the metric is under redesign. The chapter quotes no compute numbers until the study settles them.

## Open findings

1. **ch44: the official `apache/spark:4.3.0-python3` image ships Python 3.10.12, but `pyspark==4.3.0` requires Python >=3.11** (`setup.py`). PySpark runs in the image from `pyspark.zip`, so nothing checks it. Repro: `docker run --rm --entrypoint python3 apache/spark:4.3.0-python3 --version`. Possibly a doc/packaging gap in apache/spark-docker; not filed. Also: no PyArrow/pandas in the "python3" image while Arrow UDFs are the default.
2. **ch44: the 4.3 packaging guide's `uv run` recipe** works in local mode only because executors inherit the driver's `VIRTUAL_ENV`; with `spark.executorEnv.VIRTUAL_ENV=''` (as on a remote executor), the worker runs a uv-managed Python without the script's dependencies (`archives.txt`). Not verified on a real cluster.
3. **ch46: Connect `F.udf` without `useArrow` calls `SparkSession.active().conf.get(...)`** (`connect/udf.py`): with sessions from `.create()` (no active session) the UDF silently becomes non-Arrow, and when the active session is dead or unauthenticated, UDF creation blocks while the client retries. Repro: create session A with no token against a token server, session B with the token, then `F.udf(f, StringType())` hangs. Possible bug or doc gap.
4. **ch46/ch47: a client whose server went away keeps a non-daemon retry thread** that keeps the Python process alive at exit (scripts use `os._exit`). Not minimized.
5. **ch45: a Python `QueryExecutionListener` through Py4J** (`ensure_callback_server_started` + `listenerManager().register`) hung on the second query in a local test (`collect()` then a `noop` write). Not investigated; the chapter does not show it. Repro script was in the scratchpad only.
6. **ch43: `DataFrame.sample(0.001, seed=7)`** ignores the keyword seed when the first positional argument is a float (`_preapare_args_for_sample`, dataframe.py 2187); recorded by the earlier writer (`writes.txt`).
7. **ch47: operator 1.1.0 sets `spark.authenticate=true` by default**, which breaks applications on a `SparkCluster` without a shared secret (`A secret key must be specified via the spark.authenticate.secret config`). Documented behavior? Not found in the operator docs; possible doc gap.
8. **ch47: `SparkCluster` workers report all host cores (16)**; `spark.worker.cores` in `sparkConf` had no effect in a test (removed from the YAML). Not investigated.

## Not verified

- Speculation behavior on a real cluster (ch43 states only the source condition).
- `venv-pack` and `uv run` recipes on YARN/Kubernetes (ch44).
- History Server for a Connect server on Kubernetes (ch47 used port-forwarded live REST only).
- A catalog with grants enforcing `sandbox_<id>` (ch49 shows that the built-in catalog does not).

## Nav entries

```yaml
  - "Part VIII: Running Spark in production":
    - chapters/ch45-determinism-retries.md
    - chapters/ch46-shipping-dependencies.md
    - chapters/ch47-observability.md
    - chapters/ch49-connect-in-production.md
    - chapters/ch47-kubernetes-operator.md
    - chapters/ch49-pipelines-with-agents.md
```

(After renaming: ch43→ch45, ch44→ch46, ch45→ch47, ch46→ch49, ch47→ch47, ch49→ch49; ch48 orchestration belongs to another writer.)

## Clips and storyboard

Cut list: `book/media/clips/clips-part08.tsv`. Tapes: `code/film/ch43.tape` to `ch49.tape`, `--format chapter`. VO lines are drafts.

| Clip | On screen | VO (draft) |
|---|---|---|
| ch43-attempts | `attempts.py` under `local[4]` and `local[4,2]` | Local mode never retries a task; `local[4,2]` gives two attempts, and `rand(7)` repeats while Python's random does not. |
| ch43-shuffle | `lost_shuffle.py` | A deleted map output: deterministic code recovers, random code changes the answer, and the 4.3 checksum stops the job. |
| ch43-reattach | `reattach.py` through the flaky proxy | The client reattaches to the same operation; without reattach, the operation ID refuses a second execution. |
| ch43-fragile | `fragile_job.py` | The fragile job duplicates under chaos; the correct job gives the same output. |
| ch44-where | `where.py` from two folders | A worker starts in the driver's folder in local mode, so a package there imports without shipping. |
| ch44-includes | `pyfiles.py` Step 5 | A UDF records its include list once; the old UDF never sees the new zip. |
| ch44-artifacts | `artifacts.py` | Two Connect sessions ship two versions of one module, each in its own folder and workers. |
| ch45-connect | `connect_obs.py` | The 4.3 REST API lists Connect sessions and operations, and the History Server keeps them after the server stops. |
| ch45-listener | `stream_listener.py` | `observe()` in a stream shows the drift in each batch. |
| ch46-isolation | `isolation.py` | What a session isolates, and what all sessions share. |
| ch46-lifecycle | `lifecycle.py` | How a session ends, and a tagged query interrupted. |
| ch46-feather | `feather_server.py` | One session turns on single-task execution on a shared server. |
| ch47-app | `run_app.py zones-app.yaml` | The operator follows a SparkApplication from request to success. |
| ch47-connect | `connect_k8s.py` | A laptop client uses a Connect server in the cluster; the operator replaces the driver, and the sessions do not survive. |
| ch49-dryrun | `dryrun.sh` | The dry-run refuses three agent errors before any data; the epoch error passes. |
| ch49-metrics | `metric_view.py` | By-hand SQL and `MEASURE()` give different answers, and both exit 0. |
