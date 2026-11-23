# v3-ml-testing: MLlib breadth, testing and debugging, TorchDistributor, JVM functions

Branch `v3-ml-testing`, worktree `~/spark-masterclass-parts/ml-testing`. Ports: `SPARK_UI_PORT=4158`, Connect gRPC 15118 (UI 4159, `code/ch64/server.sh`).

## Scope and changes

New chapters (provisional numbers, the integrator renumbers):

- `book/chapters/ch64-mllib-beyond-regression.md` (Part XI). Clustering (KMeans with silhouette over k and seeds, BisectingKMeans, GaussianMixture, LDA and PIC in the table and the Connect matrix), ALS explicit and implicit, FPGrowth (trips as baskets), PrefixSpan (days as sequences), `Correlation`, `ChiSquareTest`, `Summarizer`, feature transformers not in ch63 (Imputer, Standard/MinMax/Robust scalers, Bucketizer, QuantileDiscretizer, TargetEncoder, PCA, RFormula, SQLTransformer, UnivariateFeatureSelector), CrossValidator and TrainValidationSplit on ALS (stdMetrics, foldCol, parallelism measured), and a Classic/Connect support matrix with the server-side lists that control it. Code: `code/ch64/`.
- `book/chapters/ch75-testing-debugging-pyspark.md` (Part XII). `pyspark.testing` asserts and their parameters, a session fixture for Classic, local Connect and a Connect server, four UDF test traps, Python tracebacks from executors (simplified, full, `tracebackWithLocals` new in 4.3, `jvmStacktrace`, Connect differences), `faulthandler` for crashes, `tracebackDumpIntervalSeconds` for hangs, worker logs (stderr, `spark.sql.pyspark.worker.logging.enabled`, `spark.tvf.python_worker_logs()`, `PySparkLogger`), a remote debugger (`breakpoint()` + `remote-pdb` over TCP), profilers linked to ch74. Code: `code/ch75/`.

Extended chapters (numbers unchanged, earlier steps unchanged):

- ch63 (`ch63-mllib-mlflow.md`): new Step 12 "Barrier execution mode" and Step 13 "TorchDistributor on a CPU", a sentence in the intro, two bullets in "The whole chapter, on one page", two further-reading links. Code: `code/ch63/barrier.py`, `torch_dist.py`, `torch_rank.py`, `figures/barrier.py`, film `code/film/ch63-barrier.tape`.
- ch85 (`ch85-catalyst-extensions.md`): new Step 12 "The other JVM functions: Java UDFs, Hive UDFs and Aggregators", one bullet and run commands in "The whole chapter, on one page", one further-reading link. Code: `code/ch85/jfun/` (Java sources + `build.sh`, javac against the pyspark jars), `jvm_functions.py`, `jvm_bench.py`, `figures_jvm.py`.

What was not run: TorchDistributor with GPUs (`use_gpu=True`, `spark.task.resource.gpu.amount`), on more than one host, `train_on_dataframe`, `run("train.py")`, GPU resource scheduling, the PyCharm `spark.python.daemon.module` debugger hook (described from the PySpark debugging guide, said so in the text), LDA and PowerIterationClustering are only in the Connect matrix (no results in prose).

## Technical names and verbs (STE)

- **TN:** MLlib class and method names (`KMeans`, `BisectingKMeans`, `GaussianMixture`, `ALS`, `FPGrowth`, `PrefixSpan`, `Correlation`, `ChiSquareTest`, `Summarizer`, the transformers, `CrossValidator`, `TrainValidationSplit`, `RankingEvaluator`, `ClusteringEvaluator`), silhouette, training cost, NDCG, precision at 10, support, confidence, lift, p-value, factors, cold start, itemset, sequence, `pyspark.testing`, `assertDataFrameEqual`, `assertSchemaEqual`, `PySparkAssertionError`, `PythonException`, fixture, `faulthandler`, `remote-pdb`, `pdb`, `BarrierTaskContext`, barrier stage, `TorchDistributor`, `torchrun`, rank, `gloo`, `DistributedDataParallel`, `GenericUDF`, `GenericUDAFAverage`, `Aggregator`, `UserDefinedAggregateFunction`, `ScalaUDF`, `ScalaAggregator`, `ObjectHashAggregate`, `HashAggregate`.
- **TV:** fit, train, tune, cluster, scale, encode, impute (avoided in prose), recommend, rank, evaluate, cache, shuffle, serialize, register, compile, debug, log, profile.
- **Allowed `-ing` words used as TN or modifier** (passed with `--allow`): clustering, training, rating, smoothing, encoding, setting, centering, rounding, mining, filtering, alternating (ALS), scaling, extracting/transforming/selecting (a docs page title), testing, debugging, logging, shipping, learning (deep learning), scheduling, tracking, Scoring/scoring, Tuning, `attempt` (Spark's "stage attempt", `attemptNumber`).

## STE counts (scripts/ste-check, description mode)

| File | Findings | `--allow` words |
|---|---|---|
| `ch64-mllib-beyond-regression.md` | 0 | clustering training rating smoothing encoding setting centering rounding mining filtering alternating scaling extracting transforming selecting |
| `ch75-testing-debugging-pyspark.md` | 0 | training rating setting testing debugging logging shipping |
| `ch63-mllib-mlflow.md` (whole file) | 0 | training tracking setting Scoring scoring Tuning learning scheduling attempt |
| `ch85-catalyst-extensions.md` (whole file) | 0 | Scoring |

Rules broken on purpose: none. Output blocks quote long lines as printed.

## Claim-to-source table

| Claim | Source |
|---|---|
| 128 zones with >= 500 pickups, silhouette and cost per k, k=3 best (0.636), seed costs 387 to 431, cluster profiles, airport cluster of 4 zones | `code/ch64/output/clustering.txt` |
| An earlier unsorted run with 200 shuffle partitions gave different clusters for the same seeds | earlier runs of `clustering.py` in this session (not saved, the script now sorts) |
| BisectingKMeans sizes per partition count, norm 6.6e-16, split noise `1e-4 * norm` | `clustering.txt`, `repro_bisecting.txt`, `mllib/.../clustering/BisectingKMeans.scala` `splitCenter` at v4.3.0 |
| KMeans iteration = mapPartitions, reduceByKey, collectAsMap | `mllib/.../clustering/KMeans.scala` lines 296-336 at v4.3.0 |
| GMM: 115 of 128 with p_max >= 0.99, 5 below 0.9 | `clustering.txt` |
| 2-D KMeans history (animation numbers) | `code/ch64/output/kmeans_history.json`, printed in `clustering.txt` |
| ALS explicit: 4,824 pairs, 15 NaN rows, RMSE 2.215 vs 3.641 / 3.464 | `code/ch64/output/als.txt` |
| ALS implicit: precision and NDCG for baselines and four ALS variants, sparse zones, LaGuardia lists | `als.txt` |
| FPGrowth: 366,249 baskets, 339 itemsets, 495 rules, rules table, duplicate-item error in a task, transform plan | `code/ch64/output/patterns.txt` |
| PrefixSpan: 30 sequences of 24, 57 patterns at support 1.0, 8 with two-zone itemsets | `patterns.txt` |
| Correlation matrices, F.corr 0.773, chi-square 18115.5, dof 3, p 0.0, sample p 0.2821, Summarizer values, weighted means | `code/ch64/output/stats.txt` |
| Imputer, scalers, Bucketizer/QuantileDiscretizer, TargetEncoder, PCA, RFormula slots, selector | `code/ch64/output/features.txt` |
| TargetEncoder since 4.0.0, RobustScaler `withCentering=False` default | `python/pyspark/ml/feature.py` v4.3.0 |
| Metadata attrs grouped by kind (earlier wrong slot names) | first run of `features.py` (printed `rate_1` first), fixed by sorting by `idx` |
| CrossValidator metrics, best rank 4 / regParam 0.1, TVS same, foldCol failure, parallelism 37.5 s vs 21.8 s | `code/ch64/output/tuning.txt` (bench lock, 3 runs after a warmup, local[*] 16 cores) |
| Connect support matrix (all ok) | `code/ch64/output/connect_matrix.txt`, first run failed RFormula in both columns (column clash), fixed with `featuresCol="rf"` |
| Server lists: Estimator and Transformer service files, `ALLOWED_ATTRIBUTES` helpers | `mllib/src/main/resources/META-INF/services/org.apache.spark.ml.{Estimator,Transformer}`, `sql/connect/server/.../ml/MLUtils.scala` lines 680-694 at v4.3.0 |
| PrefixSpan over Connect calls `prefixSpanFindFrequentSequentialPatterns` | `python/pyspark/ml/fpm.py` line 519 at v4.3.0 |
| CrossValidator loop in the client | `python/pyspark/ml/tuning.py` `_fit`, `_parallelFitTasks` at v4.3.0 |
| `pyspark.testing` exports, assert parameters, versions (3.5.0, showOnlyDiff/includeDiffRows 4.0.0) | `python/pyspark/testing/__init__.py`, `utils.py` at v4.3.0 |
| Row sort by `str` when checkRowOrder=False, collect() of both sides | `utils.py` lines ~1209-1227 at v4.3.0 |
| Assert outputs (Classic and Connect equal) | `code/ch75/output/asserts.txt`, `asserts_connect.txt` |
| ignoreColumnType casts to float, fails on strings under ANSI | `utils.py` `cast_columns_to_string` at v4.3.0, `asserts.txt` |
| Three pytest runs: 12.80 s, 11.40 s, 2.16 s, setup and first-call durations | `code/ch75/output/pytest.txt` (single runs, no lock, said in text) |
| Four traps A to D and E | `code/ch75/output/traps.txt` |
| `F.udf` reads the return type at creation (JVM parse of DDL) | `python/pyspark/sql/udf.py` `_create_udf`, `returnType` (lines ~85, ~490) at v4.3.0 |
| `try_remote_functions` / `is_remote()` | `python/pyspark/sql/utils.py` lines 247-285 at v4.3.0 |
| Traceback outputs, Classic and Connect | `code/ch75/output/tracebacks.txt`, `tracebacks_connect.txt` |
| `tracebackWithLocals` version 4.3.0 and its warning, `simplifiedTraceback` default, `jvmStacktrace` doc | `SQLConf.scala` lines 5031-5378 at v4.3.0 |
| `PythonException.__str__` ignores the JVM stack | `python/pyspark/errors/exceptions/captured.py` lines 344-353 at v4.3.0 |
| Connect shows the server stack by default | `python/pyspark/sql/connect/client/core.py` `_display_server_stack_trace` (line 2372), `Connect.scala` `serverStacktrace.enabled` default true |
| faulthandler outputs, hang dump in stderr | `code/ch75/output/crash.txt`, `crash_stderr.txt` |
| faulthandler config versions (3.2.0, 4.0.0), traceback dump versions (4.1.0) | `core/.../config/Python.scala`, `SQLConf.scala` at v4.3.0 |
| Worker logs: stderr lines, the log table, same DataFrame 0 rows vs new 2 (Classic), 2/4 (Connect) | `code/ch75/output/worker_stderr.txt`, `worker_logs.txt`, `worker_logs_connect.txt` |
| `spark.sql.pyspark.worker.logging.enabled` 4.1.0, `PySparkLogger` 4.0.0 | `SQLConf.scala` line 5104, `python/pyspark/logger/logger.py` |
| Remote debugger transcript | `code/ch75/output/remote_debug.txt` |
| PyCharm daemon-module hook | `python/docs/source/development/debugging.rst` at v4.3.0 (not run) |
| Barrier outputs: timeline, mapInPandas plan, slot failure after 3.0 s, ResultStage abort, ShuffleMapStage retry | `code/ch63/output/barrier.txt` |
| Barrier poll `Thread.sleep(1000)` | `core/.../BarrierTaskContext.scala` `runBarrier` at v4.3.0 |
| Slot check defaults 15 s and 40 | `core/.../internal/config/package.scala` lines 2015-2043 at v4.3.0 |
| Barrier ResultStage abort, ShuffleMapStage resubmit | `core/.../scheduler/DAGScheduler.scala` lines 3664-3730 at v4.3.0 |
| Barrier since 2.4.0, mapInPandas barrier 3.5.0, TorchDistributor 3.4.0 | `BarrierTaskContext.scala` `@Since`, `python/pyspark/sql/dataframe.py`, `distributor.py` versionadded |
| TorchDistributor uses `mapInArrow(..., barrier=True)`, allGather of address and port | `python/pyspark/ml/torch/distributor.py` lines 650-830 at v4.3.0 |
| TorchDistributor outputs (Classic and Connect), torch versions per rank | `code/ch63/output/torch_dist.txt`, `torch_dist_connect.txt` |
| Rank race: local mode returned rank 1 in 3 of 8 runs | `code/ch63/output/torch_rank.txt`, `python/pyspark/ml/dl_util.py` `create_fn_run_script` |
| Hive UDF needs Hive support, NO_HANDLER_FOR_UDAF, registration outputs, NULL tables, plan nodes, Hive type error | `code/ch85/output/jvm_functions.txt`, `BaseSessionStateBuilder.scala` line 512, `HiveSessionStateBuilder.scala` `HiveUDFExpressionBuilder` at v4.3.0 |
| `registerJavaUDAF` accepts only UserDefinedAggregateFunction | `sql/core/.../classic/UDFRegistration.scala` line 122 at v4.3.0 |
| `functions.udaf` since 3.0.0, UDAF since 1.5.0, deprecated 3.0.0 | `sql/api/.../functions.scala`, `expressions/udaf.scala` at v4.3.0 |
| ScalaUDAF is ImperativeAggregate, ScalaAggregator and HiveUDAFFunction are TypedImperativeAggregate, HiveGenericUDF doGenCode | `sql/core/.../aggregate/udaf.scala`, `sql/hive/.../hiveUDFs.scala` at v4.3.0 |
| JVM bench numbers (and the earlier run's part B numbers) | `code/ch85/output/jvm_bench.txt` (bench lock, 5 runs after 1 warm-up, 16 cores). The earlier run (same lock, same script without min/max) printed: avg 0.121, Aggregator 0.433, UDAF 0.227, Hive 0.660, pandas 0.516, part A 0.163 / 0.179 / 0.185 / 0.154 |

## Environment

- `pyspark[connect]==4.3.0` from `code/uv.lock`, Python 3.12.3, OpenJDK 17.0.21.1, `local[*]` on 16 cores, x86_64.
- Extra packages, not in `pyproject.toml` (run with `uv run --with`): `pytest==9.2.0`, `remote-pdb` (latest from PyPI at run time), `torch==2.15.0+cpu` (`--index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match`). The Connect server's executors used `/usr/bin/python3` with a user-site `torch 2.11.0+cu130` (seen in `torch_dist_connect.txt`).
- Hive: the `hive-exec-2.3.10-core.jar` bundled with pyspark 4.3.0, Derby metastore in ``data/work/ch85-metastore` of the repo (ignored).
- `jfun.jar`: `javac --release 17` against the pyspark jars (`code/ch85/jfun/build.sh`). `jext` rebuilt with `code/ch85/build.sh` (Maven 3.8.7).
- Connect server: `code/ch64/server.sh` (pip install, gRPC 15118, UI 4159, logs in `$BOOK_DATA/logs/ml-testing`).
- Films: VHS through `film render code/film/<tape>.tape --format chapter`, no bench lock. Animations: HyperFrames (`anim render <dir> --formats landscape --themes paper`), scaled to 1280x720, CRF 30.

## Open findings (not called bugs in the book)

1. **BisectingKMeans does not split centered data, depending on the partition count.** `splitCenter` makes the two child centers at `1e-4 * norm(center)` from the parent. After `StandardScaler(withMean=True)`, the root center's norm is a rounding error, and the split can put every row in one child with no error. Repro: `uv run python ch64/repro_bisecting.py` (three separated groups of 100 synthetic points, 1, 2, 16, 32, 200 partitions give `[300, 0, 0]`, 4 and 8 give `[100, 100, 100]`). Candidate for `spark-bug` (check master and earlier releases, the noise formula is likely old).
2. **`assertDataFrameEqual(..., ignoreColumnType=True)` fails on any string column with ANSI mode on.** `cast_columns_to_string` casts every column to `float` to detect whole numbers, under ANSI the cast of a non-numeric string throws `CAST_INVALID_INPUT`. Repro: `uv run python ch75/asserts.py` (section "the same, ignoreColumnType=True"), passes with `spark.sql.ansi.enabled=false`. Same in Connect.
3. **Connect `PythonException` has `getCondition()` and `getSqlState()` = None** for a UDF error, while Classic gives `PYTHON_EXCEPTION` / `38000` and the Connect message text contains both. Repro: `SPARK_REMOTE=sc://localhost:15118 uv run python ch75/tracebacks.py`.
4. **`TorchDistributor(local_mode=True).run()` does not always return rank 0's value.** Each rank's generated script writes its return value to the same `output.pickle`, the last writer wins. Repro: `uv run --with torch ... python ch63/torch_rank.py` (rank 1 in 3 of 8 runs, `local_mode=False` returned rank 0 in 8 of 8). The docstring says "rank 0".
5. **Doc gap:** `CREATE FUNCTION` with a Hive `GenericUDF` class in a session without Hive support fails with `NO_HANDLER_FOR_UDAF ... Use sparkSession.udf.register(...)`, an aggregate-only message for a scalar function. Repro: section 1 of `ch85/jvm_functions.py`.
6. **Not a Spark issue, recorded for the integrator:** in Classic, re-running the same DataFrame keeps the runtime settings of its first planning (seen with `spark.sql.pyspark.worker.logging.enabled`), Connect re-sends the plan. Shown in ch75 Step 7.

## Media

- Figures (light and dark SVG, each rendered to PNG on white and slate and read before embedding): `ch64-kmeans-k`, `ch64-als`, `ch75-pytest`, `ch75-tools`, `ch63-barrier`, `ch85-jvm-bench`. Scripts: `code/ch64/figures.py`, `code/ch75/figures.py`, `code/ch63/figures/barrier.py`, `code/ch85/figures_jvm.py`.
- Animations: `book/media/clips/ch64-anim-kmeans.{mp4,webp}` (`code/ch64/anim/kmeans`, points and centers from `kmeans_history.json`), `ch75-anim-traceback.{mp4,webp}` (`code/ch75/anim/traceback`, counts from `tracebacks.txt`, `crash.txt`, motion schematic).
- Film clips: `ch64-rules`, `ch64-connect`, `ch75-pytest`, `ch75-debugger`, `ch63-barrier` (cut list `book/media/clips/clips-v3-ml-testing.tsv`, each clip is the beat with repeated still frames removed by `mpdecimate`, an 8 s hold, poster = last frame). The films show their own run's numbers (barrier times differ from `barrier.txt`, captions quote none).

Film storyboard (VO drafts, checked against the code):

| beat | tape | VO draft |
|---|---|---|
| `rules` | ch64 | Each trip is a basket of items. FPGrowth finds the rules, and the strongest ones describe the JFK flat fare. |
| `connect` | ch64 | Every algorithm of this chapter fits in a Classic session and over Spark Connect, without a code change. |
| `pytest` | ch75 | The tests run against a Connect server in about two seconds. Then four small changes break them. |
| `debugger` | ch75 | The worker stops at breakpoint(), and pdb commands reach it over a TCP socket. |
| `barrier` | ch63-barrier | Barrier tasks start together and wait for each other. Too few slots, and the job fails. One failed task runs the stage again. |

## Nav entries and roadmap rows for the integrator

```yaml
  - "Part XI: Machine learning and applied algorithms":
      - "MLlib beyond regression": chapters/ch64-mllib-beyond-regression.md      # after ch63
  - "Part XII: Running Spark in production":
      - "Testing and debugging PySpark": chapters/ch75-testing-debugging-pyspark.md   # near ch73/ch74
```

Roadmap rows:

| # | Chapter | Covers |
|---|---|---|
| 93 | [MLlib beyond regression](chapters/ch64-mllib-beyond-regression.md) | KMeans, BisectingKMeans, GaussianMixture on zone profiles, ALS explicit and implicit on origin-destination pairs, FPGrowth on trips as baskets, PrefixSpan on days, Correlation, ChiSquareTest, Summarizer, more feature transformers (TargetEncoder 4.0), CrossValidator and TrainValidationSplit on ALS, which parts work over Connect in 4.3 |
| 94 | [Testing and debugging PySpark](chapters/ch75-testing-debugging-pyspark.md) | `pyspark.testing` asserts, a session fixture for Classic and Connect, UDF test traps, executor tracebacks (**4.3:** `tracebackWithLocals`), faulthandler, hang dumps, the worker log table, a remote debugger |

Row changes for existing chapters: ch63 "Covers" add "barrier execution mode, TorchDistributor on CPU (local and barrier modes, over Connect)", ch85 "Covers" add "Java UDFs, Hive UDFs and typed Aggregators against the native expression, measured".

Coverage rows (gap audit): Clustering, ALS, FPM, basic statistics -> ch64, Features (scalers, Imputer, Bucketizer, PCA) -> ch64 (Word2Vec still not covered), Testing PySpark, Debugging PySpark -> ch75 (`PySparkLogger` included), Distributed deep learning -> ch63 Steps 12-13, Hive UDF/UDAF, Scala UDAF (`Aggregator`) -> ch85 Step 12 (Hive UDTF named only).

## Cleanup

Connect server stopped with `ch64/server.sh stop`. No containers or clusters started. Scratch files under `/tmp/claude-1000/-home-lnc-repos/ba9be504-7f98-4ba6-9537-bf1ce27aaf62/scratchpad/ml-testing/`.
