# Part VII notes: Machine learning and applied algorithms

Chapters (old numbers in file names, final numbers from `book/roadmap.md` in brackets):

- `ch63-mllib-mlflow.md` (37) ML pipelines with MLlib, tracked in MLflow
- `ch65-text-analytics.md` (38) Text analytics without an LLM
- `ch66-similarity-search.md` (39) Similarity search: vectors, LSH and IVF
- `ch67-entity-resolution.md` (40) Entity resolution and connected components
- `ch68-sketches.md` (41) Sketches: HLL, Theta and KLL
- `ch69-calling-models.md` (42) Calling models and services from Spark

**Cross-references.** Inside Part VII, the text says "Chapter 63" to "Chapter 69" and links to the old file names, so the integrator's renumber map (35-40 to 37-42) applies to them. References to other parts use only chapters 1 to 23 (unchanged by the map: Chapter 5, Chapter 43) or part names ("Part VI", "Part IX"), so the map cannot shift them twice.

## Technical names and verbs

- **Products and projects:** Spark, PySpark, MLlib, Spark Connect, MLflow, Apache DataSketches, GraphX, GraphFrames, NumPy, pandas, Arrow, SQLite, Parquet, JSON, VARIANT, Delta, Iceberg, SparkTutor, NYC TLC.
- **ML terms (TN):** estimator, transformer, model, pipeline, feature, feature vector, slot, label, training set, test set, training (as a modifier), tracking store, tracking, run, flavor, signature, artifact, parameter grid, setting (one point of a grid), fold, regularization, solver, iteration, line search, leaf, node, ensemble, importance, RMSE, MAE, AUC, ROC, recall, precision, threshold, baseline, segment, scoring (as a modifier).
- **Text and search (TN):** token, tokenizer, stop word, stemmer, term frequency, inverse document frequency (IDF), TF-IDF, n-gram, shingle, corpus, vocabulary, hash, hash table, bucket, collision, cosine similarity, dot product, Jaccard similarity, Jaccard distance, MinHash, signature, band, banding, locality-sensitive hashing (LSH), OR-amplification, candidate pair, ground truth, inverted index, inverted file index (IVF), centroid, list, probe, k-means, histogram, band energy, unit vector, norm.
- **Entity resolution and graphs (TN):** entity, entity resolution, blocking, block, matching key, nickname, evidence, survivorship, golden record, cluster, clustering, connected component, label propagation, large-star, small-star, star, union-find, diameter, edge, node, plan growth.
- **Sketches (TN):** sketch, HyperLogLog (HLL), HyperLogLog++, register, theta sketch, tuple sketch, KLL sketch, count-min sketch, rank, quantile, percentile, union, intersection, rollup.
- **Services (TN):** service, endpoint, request, rate limit, token bucket, backoff, jitter, retry (noun), try (noun), timeout, idempotency key, dead letter, staging (location), content key, cache, closure, worker, batch.
- **Code identifiers** are in code format: classes (`StringIndexer`, `MinHashLSH`, `BucketedRandomProjectionLSH`), functions (`vector_cosine_similarity`, `hll_union_agg`), operators (`ArrowEvalPython`, `BroadcastNestedLoopJoin`, `ObjectHashAggregate`), config keys, error classes.
- **Technical verbs (TV):** fit, train, tune, score (a pair), hash, tokenize, encode, normalize, index, cluster, merge, intersect, block (records), cache, checkpoint, broadcast, shuffle, pickle, unpickle, retry, batch, deploy, log (an MLflow run), fetch, query, parse, compute, sort, filter, sample, prune, analyze, optimize.

## stecheck results

`scripts/ste-check <file> description --allow <words>` (prepared text, shared allowlist plus the chapter words). Final counts:

| File | Findings | Chapter allowlist |
|---|---|---|
| `ch63-mllib-mlflow.md` | 0 | training tracking setting Scoring scoring Tuning |
| `ch65-text-analytics.md` | 0 | (none) |
| `ch66-similarity-search.md` | 0 | hashing banding Mining typing streaming |
| `ch67-entity-resolution.md` | 0 | Blocking blocking Clustering clustering scoring matching ordering missing |
| `ch68-sketches.md` | 0 | (none) |
| `ch69-calling-models.md` | 0 | shipping staging missing |

Note: `scripts/ste-check` passes `--allow` after the shared list, and for some files the shared words then needed to be given again (`streaming` in ch66). The runs above call `stecheck` through `scripts/ste_prep.py` with the full list.

### Rules broken on purpose

- **`-ing` words kept as technical names or modifiers:** "training set", "tracking store", "blocking", "clustering", "matching key", "scoring step", "locality-sensitive hashing", "banding", "a missing value", "staging location", the label "shipping", the book title "Mining of Massive Datasets", "typing errors".
- **Past participles as adjectives:** "a fitted model", "the cached input", "the labelled tickets", "the assembled vector", "the stored sketches".
- **Parentheses** hold config keys, JIRA numbers, short labels and examples, as in Part I. A few hold a unit or a short explanation of the preceding noun.
- **Tables and output blocks** are unchanged. Mermaid labels are short phrases.
- **"approximately"** became "about" or "almost". "verify" became "do a check of". "attempt" became "try" in prose (the output column `attempts` stays).

### Words not confirmed as approved

The official dictionary is not available here. These words were used without confirmation: baseline, segment, flat (fare), extreme, uniform, evidence, judgment, boilerplate, repost, template, marketplace, overestimate, vectorized, keep-alive, rejection, bill, budget, fixture, typo, robust (not used), arbitrary, monotonic (not used), reference (as in "a reference to a model"), fingerprint, coefficient, estimate (noun and verb), probability, theory.

## Coverage

Lesson topics from the inventories in `~/Documents/spark_content/spark-book-plan/inventory/`.

### LS10 mllib (Learning Spark 10)

| Topic | Where |
|---|---|
| Transformer, Estimator, Pipeline, PipelineModel | ch63 Step 2 |
| `pyspark.mllib` maintenance mode, absent on Connect | ch63 Step 2, Step 9 (output) |
| `StringIndexer` frequency order, `handleInvalid` error/skip/keep | ch63 Step 3 |
| `OneHotEncoder` and why linear models need it; trees do not | ch63 Step 3 |
| `VectorAssembler`, `handleInvalid` | ch63 Step 3 |
| Pipeline with a tree ensemble (RF in the lesson, GBT here) | ch63 Step 5 |
| Regression and classification families | ch63 Steps 4, 5, 8 (LinearRegression, GBT, LogisticRegression, RandomForestClassifier) |
| `featureImportances` and slot alignment with one-hot vectors | ch63 Step 6 |
| `RegressionEvaluator` rmse/r2/mae | ch63 Step 5 |
| `CrossValidator` fit arithmetic, `ParamGridBuilder`, `parallelism`, `TrainValidationSplit` | ch63 Step 8 |
| Save/load, cross-language format | ch63 Step 8 |
| MLlib on Connect (SPARK-49907): server-side fit, references, plan node, lazy attributes, server file system | ch63 Step 9 |
| ML cache controls `mlCache.memoryControl.*` | ch63 Step 9 (names checked in `Connect.scala`, defaults not quoted) |
| MLflow logging | ch63 Steps 10, 11 |
| Distributed training (`treeAggregate`), caching the training data | ch63 Step 4 (new, the inventory asked for it) |
| `StandardScaler` | ch66 Step 2 |
| ALS, clustering, recommendation | left out: the lesson only names them; KMeans appears in ch66 Step 8 |

### MD19 text_analytics

| Topic | Where |
|---|---|
| `text(..., wholetext=True)`, word counts with `split`/`explode` | ch65 Step 1 |
| `sentences()` | ch65 Step 1 |
| Tokenizer vs RegexTokenizer (`pattern`, `gaps`, `toLowercase`, `minTokenLength`) | ch65 Step 2 |
| `StopWordsRemover` (`loadDefaultStopWords`, `caseSensitive`), no stemming | ch65 Step 2 |
| TF-IDF, IDF formula `log((N+1)/(df+1))` | ch65 Step 3 |
| `HashingTF` vs `CountVectorizer`, collisions | ch65 Step 4 |
| `Normalizer(p=2)`, dot product = cosine, scoring a query | ch65 Step 5 |
| Reading scores (stemming misses, per-query comparability) | ch65 Step 5 |
| Python UDF dot product replaced by a built-in | ch65 Step 6 (`F.vector_inner_product`, new Python wrapper in 4.3) |
| Jaccard, `MinHashLSH`, `HashingTF(binary=True)`, `approxSimilarityJoin`, `approxNearestNeighbors` | ch65 Step 7 |
| Thresholds on templated e-mails, `NGram` shingles | ch65 Steps 7, 8 |
| Empty vectors fail MinHash | ch65 Step 8 |
| Connect ignores `distCol` | ch65 Step 9 (re-checked on 4.3.0: still ignored) |
| Scaling notes (broadcast queries, top-k by window, save `PipelineModel`) | ch65 Step 6, ch66 Steps 4, 5 |

### MD20 inference_with_pandas_udfs

| Topic | Where |
|---|---|
| Train on the driver, plain `@udf` with the model in the closure | ch69 Step 1 |
| Arrow-optimized UDFs by default, `ArrowEvalPython` vs `BatchEvalPython` | ch69 Step 1 |
| Scalar `pandas_udf`, batch size `maxRecordsPerBatch`, `maxBytesPerBatch` | ch69 Step 2 |
| Iterator `pandas_udf`, load once per task (marker files) | ch69 Step 3 |
| Shipping the model: closure, `UDFCompressionThreshold`, file | ch69 Steps 1, 3 |
| `__main__` globals not a cache, `predict_batch_udf` with its model cache | ch69 Step 3 |
| `mapInPandas`, columns by name (`RESULT_COLUMN_NAMES_MISMATCH`) | ch69 Step 4 |
| Tool-choice table | ch69 Step 4 |
| Why the model runs more than once (count, limit, filter duplicates UDF, `asNondeterministic`, cache) | ch69 Step 5 |
| Retries and speculation run partitions again | ch69 Step 5 |
| Accuracy against the answer key | ch69 Step 4 |
| LLM answers validated | ch69 Step 12 (with VARIANT, from MD37) |
| `SCHEMA_MISMATCH_FOR_PANDAS_UDF`, Arrow safe cast, nullable integer `Int64` trap, `preferIntExtensionDtype` | left out: these belong to the Python-on-Spark chapter (final 20), which re-measures Arrow conversion in 4.3 |
| MLlib `NaiveBayes` vs own model | left out (ch63 covers MLlib estimators) |

### MD35 similarity_search

| Topic | Where |
|---|---|
| Features, similarity, search; measure the approximation | ch66 thesis and diagram |
| `array<float>` vs `VectorUDT`, `array_to_vector`, `vector_to_array` | ch66 Step 3 |
| Color histograms from the image source (BGR, gray, alpha, fractions) | ch66 Step 1 |
| What a histogram cannot see; `sha2` for byte copies | ch66 Step 1 |
| Audio band energies, units (bin position vs Hz) | ch66 Step 2 (28 of 40 vs 40 of 40 here) |
| Cosine vs Euclidean, `d^2 = 2 - 2cos` | ch66 Step 3 |
| `StandardScaler`, `MinMaxScaler`, `Normalizer`, file size dominates | ch66 Step 2 |
| `VectorAssembler.handleInvalid`, `VectorSizeHint` | ch66 Step 2 |
| Vector SQL functions and their rules (`array<float>` only, dimension mismatch) | ch66 Step 3 |
| Float overflow (SPARK-58544) | ch66 Step 3 (fixed in 4.3.0, shown) |
| Exact kNN by cross join and window with tie-breaks; plan | ch66 Step 4 |
| `BucketedRandomProjectionLSH` mechanics, `hashes` type, recall and work sweep | ch66 Step 6 |
| `bucketLength` default and seed behavior | ch66 Step 6 (changed in 4.3.0; see open findings) |
| `approxNearestNeighbors` returns the key | ch66 Step 6 |
| MLlib on Connect differences | ch65 Step 9 |
| Real embedding models, IVF | ch66 Step 8 |
| NULL semantics of vector aggregates, `INVALID_VECTOR_NORM_DEGREE` | left out: not re-measured |

### MD36 entity_resolution

All topics are in ch67: one schema and `dropDuplicates` (Step 1); accent stripping, nickname map, Gmail rules, phone keys, `levenshtein` threshold, `soundex` with non-ASCII, Jaro-Winkler, token-set similarity (Step 2); multi-pass blocking with measured recall, block-size skew (Step 3); naive vs evidence scoring, threshold sweep, the Mei Chen false match, Fellegi-Sunter/LogisticRegression mention (Step 4); label propagation with plan growth and `localCheckpoint`, `checkpoint` vs `localCheckpoint`, `spark.checkpoint.dir` on Connect (Steps 5, 9); clustering amplifies errors (Step 6); survivorship with `max_by` NULL ordering, `mode(deterministic=True)`, sorted `collect_set`, stable entity ids (Step 7). Left out: the multi-criteria `max_by` over a struct for the name (the script uses `mode` of the name key instead).

### MD37 inference_in_production

All in ch69: the misbehaving service and its failure table (Step 6); content-hash keys and model version next to the key (Steps 6, 11); new work by left anti join (Step 11); per-row UDF vs `mapInPandas` with request counts (Steps 7, 8); keep-alive, batching, per-task rate limit, total = per-task x tasks (Steps 8, 9); retryable vs permanent, backoff with full jitter, attempt caps, attempts table (Step 8); idempotency keys (Step 8); write once then split with request counts (Step 10); never write from `mapInPandas` (Step 10); LLM answer validation with `try_parse_json` and `try_variant_get` (Step 12). Left out: `applyInPandas` with one model per group and its 4.1 iterator form, and stage-level scheduling with GPU resource profiles (not runnable on this machine). SPARK-58241 (hang) not re-checked.

### MD38 capstone_intermediate

ch65 Step 10: catalog grain of one row per document part, the wide table, per-kind parsing, PDF pages, e-mails, Latin-1 fallback, images with NULL for corrupt files, WAV durations, `_metadata.file_path` join fix, inventory without content, and the search index over the catalog. Left out: the custom Python data source with pushdown and the streaming reader (Part IX covers Python data sources) and the SDP pipeline version (Part VI). The SPARK-59135 accumulator protocol failure is mentioned as a 4.2.0 problem; it was not re-run on 4.3.0.

### MD48 lsh_and_ivf

ch66 Steps 7 and 8: shingles with HOFs, the `sequence(1, 0)` trap, exact all-pairs Jaccard and its histogram, the inverted-index blowup (1,674,500 join rows), MinHash as one Project with no shuffle, BIGINT overflow under ANSI, measured estimate error against theory, banding S-curve with predicted vs measured candidates and recall, verification, MLlib OR-only with the `LSH.scala` comment and `HASH_PRIME`, IVF with KMeans, the NumPy `createDataFrame` trap, nearest centroid with `array_position`, probes with `arrays_zip`/`array_sort`, top-k with `collect_list`, the recall curve and full-probe invariant, uneven lists. Left out: SimHash, hot buckets, the centroid-shipping benchmark, PQ and re-ranking.

### MD49 connected_components

ch67 Steps 8 and 9: edge cleanup, label propagation rounds against diameter (50 and 200 rounds on paths), large-star/small-star with a traced path, convergence test, window + explode form, rounds table, union-find check, the plan of an iterative loop four ways (nothing, cache, localCheckpoint, checkpoint) with the `InMemoryRelation` doubling, `checkpoint` vs `localCheckpoint`, GraphX/GraphFrames status. Left out: the `explainString` OOM stack trace, hub skew and `windowExec.buffer.in.memory.threshold`, the adversarial graph table, the xxhash64 id warning.

### MD50 sketches

ch68: distinct counts do not add (Step 1); hashing messy values, empty string, type (Step 2); HLL++ parameter derivation with measured RSE (Step 3); partial/final plan (Step 3); DataSketches HLL, `lgConfigK`, `HLL_8` (Step 4); table of sketches and deletion of raw data with `inputFiles()` (Step 4); union and `HLL_UNION_DIFFERENT_LG_K` (Step 5); theta intersection and its error relative to the union (Step 6); tuple sketches named (Step 6); KLL rank vs value error, randomness, `percentile_approx` deterministic (Step 7); count-min and `approx_top_k` (Step 8). Left out: HLL byte preamble, inspection through `spark._jvm`, HIP/out-of-order flag, collation-aware hashing, measured tuple sketches, cross-library compatibility (was unverified in the lesson).

## Claim-to-source

| Claim | Source |
|---|---|
| 366,249 training rows, metrics table, rate-code MAE table, importances, maxBins error, saved model layout, TrainValidationSplit | `code/ch63/output/fare_pipeline.txt` |
| StringIndexer labels and counts, handleInvalid, OneHotEncoder sizes, slots, fit jobs (2 and 11), cached vs not (1.28 s vs 1.98 s), tip classification, pruneTree nodes, CrossValidator 13 fits and grid trap | `code/ch63/output/estimators.txt` |
| Connect references, ML cache entries and sizes, plan nodes, test RMSE 8.12, `JVM_ATTRIBUTE_NOT_SUPPORTED` | `code/ch63/output/connect_ml.txt` |
| MLflow 3.18.0, runs table, artifacts, flavors, signature, pyfunc predictions | `code/ch63/output/mlflow_tracking.txt` |
| log_model fails from Connect, log_artifacts workaround | `code/ch63/output/mlflow_connect.txt` |
| 6 cache entries after a 5-tree GBT fit | `code/ch63/output/repro_trees_cache.txt` |
| `pruneTree` new in 4.3 | SPARK-34591, commit f67a8551e0b in v4.3.0 |
| MLlib on Connect since 4.0 | SPARK-49907 (inventory); `pyspark/ml/connect` in v4.3.0 |
| mlCache conf names | `sql/connect/server/.../config/Connect.scala` lines 406-445 at v4.3.0; added in 4.1 by SPARK-51974 |
| Size-estimate commits in 4.3 | `git log v4.2.0..v4.3.0 -- mllib python/pyspark/ml` (SPARK-58281, SPARK-57521) |
| 28 rows as lines, 4 with wholetext; word counts; tokenizers; 181 stop words; IDF table; HashingTF collisions; search scores; 223/240; plan operators | `code/ch65/output/text_search.txt` |
| MinHash thresholds, by-hand 7/9, empty-vector error, distCol columns, word set vs 2-grams, plan | `code/ch65/output/near_duplicates.txt` |
| Connect: distCol ignored, tie order, error wrapper, BroadcastHashJoin | `code/ch65/output/near_duplicates_connect.txt` |
| 27 parts from 26 files, kinds table, Latin-1 line, FileScan without content, catalog search | `code/ch65/output/catalog.txt` |
| Vector functions have Python wrappers in 4.3 (versionadded 4.3.0), none in 4.2.0 | `python/pyspark/sql/functions/builtin.py` at v4.3.0 and v4.2.0 |
| 43 images, 41 of 42, plant miss, audio 28/40 vs 40/40, vector rules and errors, 5e30 norm, d^2 check, scaler 32 vs 40, exact plan, LSH sweep table, hashes type, NEAREST BY result and plan, APPROX = EXACT | `code/ch66/output/vectors.txt` |
| SPARK-58544 fixed in 4.3.0 | commit da54921d8bb in v4.3.0 |
| NEAREST BY rewrite to cross join + max_by/min_by + inline | `RewriteNearestByJoin.scala` doc comment at v4.3.0 |
| NEAREST BY grammar exists in 4.2.0 too | `SqlBaseParser.g4` at v4.2.0 line 1075 |
| 424 listings, 89,676 pairs, histogram, inverted index rows, MinHash error table, banding table, MLlib MinHashLSH candidates, IVF table, NEAREST 10 recall | `code/ch66/output/indexes.txt` |
| `HASH_PRIME = 2038074743`, OR-amplification comment | `MinHashLSH.scala` line 215, `LSH.scala` line 315 at v4.3.0 |
| 139 records, 62 people, 105 pairs, keys, built-in string checks, blocking table, soundex blocks, scoring and sweep, false match, plan growth table, 61 entities, 0.921 precision, cluster of 6, `CANNOT_MODIFY_CONFIG`, golden records | `code/ch67/output/resolve.txt` |
| Star trace on 6 nodes, rounds table, union-find check, exchanges, four-ways loop table, checkpoint files | `code/ch67/output/components.txt` |
| Plan string made for each action | `SQLExecution.withNewExecutionId0` calls `queryExecution.explainString` (`SQLExecution.scala` line 214 at v4.3.0) |
| Event counts, distinct counts, HLL++ table and measured RSE, sketch table size, HLL/theta/KLL results, count-min size, top 3 zones | `code/ch68/output/sketches.txt` |
| HLL++ formulas | `HyperLogLogPlusPlusHelper.scala` lines 43, 260 at v4.3.0 |
| `HLL_8`, default lgConfigK | `datasketchesAggregates.scala` lines 85-90 at v4.3.0 |
| KLL since 4.1.0, `kll_merge_agg` since 4.1.2 | `kllAggregates.scala` `since` fields at v4.3.0; SPARK-57116 |
| datasketches-java 6.2.0 | `pom.xml` at v4.3.0 |
| Zone 237 = Upper East Side South | `taxi_zone_lookup.csv` |
| UDF calls, batch calls, loads per task, mapInPandas error, accuracy 0.925, rows into the model per action, predict_batch_udf loads | `code/ch69/output/inference.txt` |
| 232 distinct texts, per-row UDF requests, mapInPandas requests and attempts table, rate table, write-once table, second run, VARIANT validation | `code/ch69/output/calling.txt` |
| `spark.broadcast.UDFCompressionThreshold` | `core/.../internal/config/package.scala` line 2440 at v4.3.0 |

Timings: the only timing that the text quotes as a result is the cached-vs-not fit comparison in ch63 Step 4, a median of 3 after a warm-up under `$HOME/spark-masterclass/data/.bench.lock`. Other output blocks show times from single runs without the lock (fit seconds, label propagation rounds, star vs propagation, service runs, search medians in ch65), on a 16-core machine that other writers used at the same time (load average 20 to 50). The prose says that these times are not measurements and draws no speed conclusion from them; the results it uses are counts (rounds, requests, plan lines, rows). Films ran without the lock, per the updated brief.

## Environment

- `pyspark[connect]==4.3.0` from `code/uv.lock`, Python 3.12, OpenJDK 17, local mode `local[*]` on 16 cores.
- NumPy 2.5.5, pandas < 3, pyarrow (from the lock file).
- `mlflow==3.18.0`, run with `uv run --with mlflow==3.18.0` (not added to `pyproject.toml`, which belongs to the integrator). Tracking store: SQLite at `$BOOK_DATA/ch63/mlflow/mlflow.db`, no server.
- Spark Connect server from the pip install: `code/ch63/server.sh` (gRPC 10270, UI 4127).
- Apache DataSketches: `datasketches-java` 6.2.0, bundled with Spark 4.3.0. No external packages.
- Ports: `SPARK_UI_PORT=4027` (estimators), 4227 and 4237 for the other scripts and the films.

To run: `cd code && export BOOK_DATA=$HOME/spark-masterclass/data && uv run python chNN/<script>.py`. The Connect scripts need `ch63/server.sh start` first.

## Open findings

1. **`approxSimilarityJoin(..., distCol=...)` is ignored over Spark Connect in 4.3.0.** Repro: `SPARK_REMOTE=sc://localhost:10270 uv run python ch65/near_duplicates.py`. Classic gives columns `['datasetA', 'datasetB', 'jaccard']`, Connect gives `['datasetA', 'datasetB', 'distCol']`. The SparkTutor lesson saw the same on 4.2.0. Candidate for `spark-bug`.
2. **`BucketedRandomProjectionLSH` without `bucketLength` fits and transforms in 4.3.0**, with `isSet` and `hasDefault` both `False` (`code/ch66/output/vectors.txt`). The lesson recorded a failure at `transform` on 4.2.0. The value that the model uses is unknown (possibly a NaN or zero bucket width giving degenerate hashes). Needs a check of the hashes it produces and of `BucketedRandomProjectionLSH.scala` (`BLAS.gemv(1.0 / $(bucketLength), ...)`).
3. **`mlflow.spark.log_model` fails for a model fitted over Connect** (`AttributeError: 'NoneType' object has no attribute 'jvm'`) with mlflow 3.18.0 and pyspark 4.3.0. This is an MLflow limitation, not a Spark bug. Workaround in ch63 Step 11.
4. **Separate ML cache entries for each tree of a GBT** after `fit()` over Connect (6 entries for 5 trees; 30 + 1 for the fare model). Not known if this is by design. The cache sizes in `connect_ml.txt` count about 5 MB for the trees and 5 MB for the ensemble, which may count the same trees two times against `maxModelSize`/`maxInMemorySize`. The earlier writer's note said `Params.params` loads the trees; the repro on 4.3.0 shows the entries already after `fit()`, so that theory is not confirmed. Repro: `code/ch63/repro_trees_cache.py`.
5. **A Connect client script did not exit** after its last line (`ch65/near_duplicates.py` with `SPARK_REMOTE`). The server had closed the session operations. Stopped by PID. Not reproduced a second time; cause unknown.
6. **`ParamGridBuilder` with keys from another `Params` object is silently ignored** (`estimators.py`, CrossValidator section: all four metrics equal). Documented PySpark behavior (params belong to their parent), but no warning. Doc gap, not a bug.
7. **`approx_top_k` has no Python wrapper in 4.3.0** (`AttributeError` on `F.approx_top_k`), although `count_min_sketch` and the theta/KLL/tuple functions have one. Doc/API gap.

## Not verified

- MLlib on Connect `mlCache.memoryControl` defaults (the lesson's 1g/10g); only the names were checked.
- SPARK-59135 (SDP and Python data source accumulators) on 4.3.0; mentioned as a 4.2.0 problem only.
- GraphFrames compatibility with Spark 4.3.
- `predict_batch_udf` model cache size and idle timeout; only the load count was measured.
- The KLL tail estimates change between runs (randomized); the chapter quotes one run and says so.

## Films

Tapes in `code/film/ch63.tape` to `ch69.tape`, rendered with `film render <tape> --format chapter` under the bench lock. Clips in `book/media/clips/chNN-*.mp4` and `.webp`, cut list in `book/media/clips/clips-part07.tsv` (start and end are positions in the rendered `<tape>-chapter.mp4`). The scripts print their output when they end, so the raw beats have a long still period while Spark runs. Each published clip is the beat clip with repeated still frames removed (`ffmpeg -vf mpdecimate,setpts=N/30/TB`) and an 8 s hold on the last frame (`tpad`); the poster is the last frame. The films show their own run's numbers (timings and KLL estimates differ from the saved output); the captions quote no numbers. Films ran without the bench lock. Each beat's on-screen caption is a typed shell comment. VO drafts:

| Clip | On screen | VO (draft) |
|---|---|---|
| ch63-connect | `connect_ml.py`: stages, ML cache, plan nodes | Over Spark Connect, the fitted pipeline in the client is a list of references. The models stay in the server's ML cache, and transform sends a plan node that names them. |
| ch65-search | `text_search.py`: policy queries, 240 tickets two ways | TF-IDF finds the right policy for each query. Then 240 tickets are scored against 160 labelled ones, once with a Python UDF and once with a built-in vector function. |
| ch66-banding | `indexes.py`: banding table | Bands of four MinHash values find every near-duplicate pair and compare two and a half percent of the pairs. MLlib's OR-only LSH compares far more. |
| ch66-ivf | `indexes.py`: IVF recall curve | Probing more lists raises recall. At sixteen of sixty-four lists, the search finds almost every true neighbor for a quarter of the work. |
| ch67-plan-growth | `resolve.py`: label propagation two ways | Without a cut, the plan doubles every round and each count takes longer. With localCheckpoint, the plan is one line. |
| ch67-cache-plan | `components.py loop`: nothing and cache | Cache keeps the data but not the plan: the optimized plan doubles each round because each cached relation prints the plan it came from. |
| ch68-rollups | `sketches.py`: rollups after deletion | The raw events are gone. Weekly distinct users, users on two days and latency quantiles all come from thirty-five stored sketches. |
| ch69-runs | `inference.py`: rows into the model per action | Count runs the model zero times, and a filter on the label runs it twice. The optimizer decides how often your Python runs. |
| ch69-requests | `calling.py`: per-row UDF vs mapInPandas | One request per row overwhelms the service. A client per partition with batches and a rate limit needs thirty-four requests and gets no rejections. |

## Nav entries

```yaml
  - "Part VII: Machine learning and applied algorithms":
      - "ML pipelines with MLlib, tracked in MLflow": chapters/ch63-mllib-mlflow.md
      - "Text analytics without an LLM": chapters/ch65-text-analytics.md
      - "Similarity search: vectors, LSH and IVF": chapters/ch66-similarity-search.md
      - "Entity resolution and connected components": chapters/ch67-entity-resolution.md
      - "Sketches: HLL, Theta and KLL": chapters/ch68-sketches.md
      - "Calling models and services from Spark": chapters/ch69-calling-models.md
```

(Rename the files to ch66 to ch69 with `notes/renumber.md` at integration.)
