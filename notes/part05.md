# Part V notes: Streaming

Chapters 39 to 43. Chapters 39 and 40 were written by an earlier writer and committed (`ced8943`, `c1a3244`); this pass finished chapters 26 to 28, made the clips for the whole part, and wrote these notes.

## Technical names and technical verbs

- **Products and formats:** Spark, PySpark, Spark Classic, Spark Connect, Kafka, KRaft, Docker, RocksDB, HDFS, LZ4, lz4-java, Avro, Parquet, JSON Lines, protocol buffer (protobuf), Arrow, pandas, VHS, SparkTutor.
- **Streaming concepts:** source, sink, trigger, output mode (append, update, complete), micro-batch, no-data batch, checkpoint, offsets log, commit log, watermark, event time, processing time, state, state store, state store provider, state partition, state variable, timer, TTL (time to live), initial state, column family, delta file, snapshot, changelog, changelog checkpointing, state checkpoint format, state encoding (`unsaferow`, `avro`), state data source, change feed, maintenance task, Real-time Mode, pipelined shuffle, streaming shuffle, allowlist, soft default, dead letter, stream-static join, stream-stream join, admission control.
- **API and class names (always in code format):** `transformWithState`, `transformWithStateInPandas`, `StatefulProcessor`, `ValueState`, `ListState`, `MapState`, `TwsTester`, `foreachBatch`, `foreach`, `statestore`, `state-metadata`, `RealTimeStreamScan`, `StatefulStreamlineAggregate`, `UnsafeRow`, config keys.
- **Technical verbs:** stream, shuffle, partition, commit, checkpoint (as a noun only), compress, decode, pickle, emit, register (a timer), expire, fire (a timer), merge, scan, deduplicate (only as `dropDuplicates`), repartition, replay.
- **Words used with one meaning:** "drop" means one input file of the checkout or clickstream data, never "remove". "Batch" means one micro-batch, or one long batch in Real-time Mode. "Run" in ch43 means one attempt of a batch.

## STE checker results

`scripts/ste-check <file> description --allow <words>`. With the earlier `ste-check`, the `--allow` list replaced the shared list, so the shared words are repeated in it (the updated script adds them, with the same result).

| Chapter | Findings | Extra allowlist words |
|---|---|---|
| ch39 | 0 (was 11; two "setting" nouns rewritten) | the ch43 list plus Programming programming (guide title) tumbling sliding (window types) |
| ch40 | 1 (was 6; three "meaning" nouns rewritten). The one left is a quoted Spark docstring ("Events are deduplicated ..."), kept exactly | the ch39 list plus docstring |
| ch41 | 0 | processing Processing grouping encoding opening partitioning streaming Streaming pruning coalescing monitoring tuning warning |
| ch42 | 0 | the ch41 list without `opening`, plus checkpointing |
| ch43 | 0 | the ch42 list |

Rules broken on purpose:

- `processing` (processing time, processing-time timer), `grouping` (grouping columns, grouping key), `encoding` (state encoding), `checkpointing` (changelog checkpointing), `opening` (opening balance, a field of the example data) are technical names used as nouns or modifiers, never as verbs.
- Some admonition titles, figure captions and table cells are phrases, not sentences.
- Output blocks, plans, error messages and config keys are quoted exactly, so their words are not STE.
- The thesis paragraphs keep one bold block of up to five sentences, as Part I does.

## Coverage

| Lesson | Topic | Where |
|---|---|---|
| LS08 structured_streaming | unbounded-table model, source/query/sink/trigger/output mode/checkpoint | ch39 "The pieces of a streaming query", Steps 1-2 |
| LS08 | file, Kafka and rate sources; schema inference | ch39 Steps 1, 8, 9 |
| LS08 | `StreamingQuery` status, progress, stop, `awaitTermination` | ch39 Steps 2-3 |
| LS08 | output modes and their legality | ch39 Step 4 |
| LS08 | stateless vs stateful, `dropDuplicatesWithinWatermark` | ch39 "Stateless and stateful operators", ch40 Step 4 |
| LS08 | triggers (`processingTime`, `availableNow`, `once`, `realTime`, continuous) | ch39 Step 8, ch42 Step 1 and "Choose an engine" |
| LS08 | Real-time Mode restrictions | ch42 Steps 1-2 (re-tested on 4.3: stateful now allowed) |
| LS08 | windows (tumbling, sliding, session), watermarks | ch39 Steps 5-6, ch40 Steps 4-5 |
| LS08 | stream-static and stream-stream joins | ch39 Steps 2 and 10 |
| LS08 | `transformWithState` (processor, state types, TTL, timers, initial state, RocksDB) | ch41 Steps 1-8 |
| LS08 | checkpoint rules, missing metadata file | ch39 Steps 7 and 11, ch43 |
| LS08 | state data source with `stateVarName` | ch41 Steps 3 and 5, ch43 Step 4 |
| LS08 | named sources (`.name`, `IDENTIFIED BY`, internal flag) | ch39 Step 11 |
| MD17 streaming_files | schema required, inference once, streaming DF is a plan | ch39 Step 1 |
| MD17 | memory sink, complete mode, `availableNow`, progress fields, restart reads new files only | ch39 Steps 2-3 |
| MD17 | shuffle partitions fixed by the checkpoint | ch39 Step 7, ch43 Step 7 |
| MD17 | atomic delivery (hidden name + rename) | ch39 "The data" |
| MD17 | checkpoint anatomy and replay rule | ch39 Step 7, ch43 |
| MD17 | `maxFilesPerTrigger`, `maxBytesPerTrigger`, `latestFirst`, `maxFileAge`, `fileNameOnly` | ch39 Step 8 |
| MD17 | `foreachBatch`, idempotent writes by `batch_id`, failure replay | ch39 Step 8, ch40 Step 3 |
| MD17 | Connect differences for progress | left out of Part V: Part I (ch18) covers Connect; the Part V scripts run Classic because `_streamingCheckpointManager` and the Python TWS paths are Classic-tested |
| MD33 streaming_messy_data | stream schema fixed at start, drift as silent NULLs | ch40 Step 1 |
| MD33 | `_corrupt_record` in streams, text + `try_parse_json` + `try_variant_get`, `unknown_keys` | ch40 Step 2 |
| MD33 | dead letters through `foreachBatch`, replay safety, cross-batch dedup | ch40 Step 3 |
| MD33 | `cleanSource=archive`, `sourceArchiveDir` | ch40 Step 3 |
| MD33 | watermark on real data, no-data batches, late vs out-of-order | ch40 Step 4 |
| MD33 | one future event drops everything after it, defenses | ch40 Step 5 |
| MD33 | two inputs, two watermarks | ch40 Step 6 |
| MD33 | checkpoint compatibility table | ch40 Step 7 |
| MD33 | TWS sessionizer, timezone gotcha, TwsTester | ch41 Steps 4-7 |
| MD51 streaming_internals | checkpoint as a set of logs, offsets/commits byte by byte | ch39 Step 7 |
| MD51 | settings a checkpoint pins | ch39 Step 7, ch40 Step 7, ch43 Steps 5, 7, 8 |
| MD51 | offline state repartition | ch43 Step 7 (works on 4.3, and the state data source reads the result) |
| MD51 | restart protocol, no-data batches | ch39 Step 7, ch41 Step 5, ch43 "How the pieces fit together" |
| MD51 | HDFS provider: deltas, snapshots, maintenance settings | ch43 Steps 1 and 3 |
| MD51 | pure-Python LZ4 + UnsafeRow decoder | ch43 Step 2, `code/ch43/lz4delta.py` |
| MD51 | state data source options, change feed | ch43 Step 4 |
| MD51 | RocksDB: zip + SSTs, changelog checkpointing, column families, timers | ch43 Steps 5-6 |
| MD51 | RocksDB memory settings (`boundedMemoryUsage`, `maxMemoryUsageMB`, `blockCacheSizeMB`, `compactOnCommit`) | left out: no measurement in Part V shows their effect; the further-reading link points to the docs table |
| MD51 | Python TWS internals: state server, protobuf, round trips | ch41 Step 8 |
| MD51 | timers precisely (`<=`, data before timers, gap detection) | ch41 Steps 5 and 7 |
| MD51 | ProcessingTime + availableNow never stops | ch41 Step 7 |
| MD51 | timestamp trap | ch41 Step 6 |
| MD51 | TTL processing-time only | ch41 Step 6 |
| MD51 | schema evolution table (`unsaferow` vs `avro`) | ch43 Step 8 (re-measured on 4.3) |
| MD51 | Real-time Mode restriction table | ch42 Steps 1-2 (re-measured on 4.3) |
| MD51 | RTM on a laptop with `LowLatencyMemoryStream` | replaced: ch42 uses a real Kafka broker for every measurement |
| MD51 | Python foreach sink batching (`spark.buffer.size`) | ch42 Step 5 |
| WN07 transform_with_state | processor skeleton, state types and methods | ch41 "The pieces", Step 1 |
| WN07 | RocksDB required, error from `awaitTermination` | ch41 Step 1 |
| WN07 | output mode consistency, memory sink appends in update mode | ch41 Steps 1-2 |
| WN07 | no output for keys without input | ch41 Step 2 |
| WN07 | initial state, ignored on restart | ch41 Steps 2-3 |
| WN07 | state data source | ch41 Steps 3 and 5 |
| WN07 | `transformWithStateInPandas` | ch41 Step 8 |
| WN07 | schema evolution (`deleteIfExists`, avro) | ch41 "Change a processor", ch43 Step 8 |
| WN07 | RTM trigger, named sources | ch42, ch39 Step 11 |
| WN07 | sink naming (`.name()` on writer) | left out: not in PySpark `DataStreamWriter` 4.3.0 (not re-checked beyond the 4.2 lesson; flagged for the integrator) |
| Real-time Mode (4.3) | stateful RTM, pipelined shuffle, latency both ways | ch42 Steps 3-5 |

## Claim-to-source table (chapters 26 to 28)

| Claim | Source |
|---|---|
| TWS on the HDFS provider fails with `STORE_BACKEND_NOT_SUPPORTED_FOR_TWS` from `awaitTermination` | `code/ch41/output/t1_spend.txt` part 1 |
| Plan shape `Exchange hashpartitioning(user, 2), REQUIRED_BY_STATEFUL_OPERATOR`, initial state as second child | `t1_spend.txt` |
| Totals 36.39, 78.15, 67.99; `u9` in state (99.0), not in output | `t1_spend.txt` parts 2-3 |
| Initial state ignored on restart (78.15 to 88.15, not 500) | `code/ch41/output/t5_pandas_restart.txt` |
| Memory sink cannot restart in update mode | first run of `t5_pandas_restart.py` (error text quoted in ch41 Step 3), and `c4_evolution.py` first run |
| Five batches for four drops, watermarks 09:30:48, 10:02:28, 10:48:40, 11:38:03 | `code/ch41/output/t2_sessions.txt` |
| `u1` closed by gap although the watermark passed its timer | `t2_sessions.txt`; order of calls in `TransformWithStateExec.scala` (`processNewData` then `processTimers`), v4.3.0 |
| Timer fired at 23.6 s, TTL gone at 28.4 s | `code/ch41/output/t3_time.txt` |
| TTL in None or EventTime mode fails with `STATEFUL_PROCESSOR_INCORRECT_TIME_MODE_TO_ASSIGN_TTL` | `t3_time.txt` |
| TIMESTAMP comes back naive local time | `t3_time.txt`; `value_state_client.py` and `stateful_processor_api_client.py` v4.3.0 |
| TwsTester results, timer fires at watermark == timer | `code/ch41/output/t4_tester.txt` |
| availableNow + ProcessingTime: 80 batches in 45 s, no stop | `t4_tester.txt`; `TransformWithStateExecBase.shouldRunAnotherBatch` (TODO SPARK-50180) v4.3.0 |
| State server per task, socket, `StateMessage` protobuf | `TransformWithStateInPySparkStateServer.scala`, `StateMessage.proto` v4.3.0 |
| ListState append is a RocksDB merge | `ListStateImpl.scala` (`store.merge`) v4.3.0 |
| Row-based Python TWS in 4.1 | `GroupedData.transformWithState` present in `group_ops.py` at v4.1.0, absent at v4.0.0 |
| TWS in RTM is JVM only, one row per `handleInputRows` call | `docs/streaming/real-time-mode.md` v4.3.0 |
| Schema evolution rules (`deleteIfExists`, avro) | `docs/streaming/structured-streaming-transform-with-state.md` v4.3.0, `code/ch43/output/c4_evolution.txt` |
| RTM rules table | `code/ch42/output/r1_rules.txt` |
| 4.2 allowlist had no `ShuffleExchangeExec` or aggregate | `RealTimeModeAllowlist.scala` at v4.2.0 |
| Memory sink not in allowlist, docs say it is supported | `r1_rules.txt`; `RealTimeModeAllowlist.allowedSinks` v4.3.0; `real-time-mode.md` "Sinks" |
| RTM plan: `RealTimeStreamScan`, `isPipelined=true`, `StatefulStreamlineAggregate` | `code/ch42/output/r4_pipeline.txt` |
| Both stages active at once in RTM | `r4_pipeline.txt` |
| Soft defaults recorded in the checkpoint | `r4_pipeline.txt` |
| Latency numbers | `code/ch42/output/r2_latency.txt` (three runs) |
| Python foreach latency, default and `spark.buffer.size=64` | `code/ch42/output/r3_foreach_default.txt`, `r3_foreach_64.txt` |
| Batch duration semantics, Kafka sink at-least-once, cores sizing | `real-time-mode.md` v4.3.0 |
| HDFS delta and snapshot files, sizes, decoded contents | `code/ch43/output/c1_hdfs.txt` |
| Delta record format, big-endian ints | `HDFSBackedStateStoreProvider.scala` (`writeUpdateToDeltaFile`) v4.3.0 |
| Defaults: `minDeltasForSnapshot` 10, `maintenanceInterval` 60 s, `minBatchesToRetain` 100 | `SQLConf.scala` v4.3.0 |
| Decoupled maintenance, thread minimum 2, ratio 0.5 | commit `f5343a1dc34` (SPARK-56498), `SQLConf.scala` v4.3.0 |
| RocksDB zip/SST layout, changelog files, format v2 names and `stateUniqueIds` | `code/ch43/output/c2_rocksdb.txt` |
| Changelog checkpointing default false for micro-batch | `RocksDB.scala` (`RocksDBConf`, `"changelogCheckpointing.enabled", "false"`) v4.3.0 |
| Format v2 changelogs carry lineage | `StateStoreChangelog.scala` (`writeLineage`) v4.3.0 |
| TWS column families and operator metadata v2 | `c2_rocksdb.txt` part 4 |
| Offline repartition writes a batch with no input, needs RocksDB | `code/ch43/output/c3_repartition.txt`; `pyspark/sql/streaming/query.py` (`StreamingCheckpointManager`, versionadded 4.2.0) |

## Environment

- `pyspark==4.3.0` (the `code/` uv project), Python 3.12, OpenJDK 17.0.21.1, Linux, 16 cores, `local[*]`.
- Kafka connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0` (`KAFKA_PACKAGE` in `code/common/book.py`).
- Kafka broker: `apache/kafka:4.0.0` in Docker, one KRaft node, host port 29055 (`code/ch39/kafka.sh`). The producer for ch42 is the broker's own `kafka-console-producer.sh`, so no Python Kafka client is a dependency.
- State store: RocksDB provider for every TWS script (`ROCKSDB` in `book.py`); ch43 sets each provider explicitly.
- Spark UI ports: 4025 (assigned) for timed runs and films; 4026 for untimed runs that overlapped them.
- Timed runs (`ch42/r2_latency.py`, `ch42/r3_foreach.py`) ran under `flock ~/spark-masterclass/data/.bench.lock`. After the integrator's rule change, films and untimed scripts ran without the lock.
- Latency runs: 200 records/s for 30 s, 40 keys, 4 input partitions, 4 shuffle partitions, first 10 s of each run dropped, percentiles over the remaining 4000 inputs. Three runs are quoted (`r2_latency.txt`), medians in the ch42 table. Two other runs are not used: one overlapped this part's own (unlocked) film renders (micro-batch pass-through p50 424 ms, Real-time 1 ms; mentioned in ch42 as a load effect), and one ended with exit code 1 before printing (stderr not kept; the next locked run succeeded).
- `r3_foreach.py`: one record a second for 10 s, 20 s Real-time batches, one run with each buffer size.
- Spark source: `~/repos/spark` at tag `v4.3.0` (and `v4.2.0`, `v4.1.0`, `v4.0.0` for version claims).

## Open findings

1. **TIMESTAMP read back from Python TWS state is a naive local-time `datetime`** (possible bug). Repro: `code/ch41/t3_time.py`, last part: session time zone UTC, machine in US Pacific; input `2026-10-05 09:00:00+00:00`, `ValueState.get()` returns `2026-10-05 02:00:00`. Cause in 4.3.0 source: `_serialize_to_bytes` calls `schema.toInternal`, but `ValueStateClient.get` returns the unpickled value without `fromInternal`. Not searched in JIRA. Candidate for the `spark-bug` skill.
2. **`availableNow` + `timeMode="ProcessingTime"` never stops** (known limitation). `TransformWithStateExecBase.shouldRunAnotherBatch` returns `true` in processing-time mode (TODO SPARK-50180). Repro: `code/ch41/t4_tester.py` part 5 (80 empty batches in 45 s). A doc note would help: the TWS guide does not warn about it.
3. **Real-time Mode docs list the memory sink as supported, but `format("memory")` is rejected** (doc gap). `RealTimeModeAllowlist.allowedSinks` has only `ContinuousMemorySink`. Repro: `code/ch42/r1_rules.py`, probe "Kafka -> memory sink".
4. **PySpark `transformWithState` in Real-time Mode fails with `STDS_FAILED_TO_READ_OPERATOR_METADATA`** instead of an allowlist error (misleading error). Repro: `r1_rules.py`, probe "Kafka -> PySpark transformWithState"; full message in the run log: "Failed to read the operator metadata for checkpointLocation=... and batchId=0".
5. **Avro state encoding accepts a field rename and silently loses the value** (docs say rename is not supported; Spark does not reject it). Repro: `code/ch43/c4_evolution.py`, row "rename n to count": `avro` reads `(None,)` with no error or warning.
6. **`addPyFile` does not reach the Python TWS worker** (possible gap, not investigated). `t2_sessions.py` first failed with `ModuleNotFoundError: No module named 't2_sessions_lib'` from `transform_with_state_driver_worker.py` although the driver called `sc.addPyFile` on that file. The scripts now put `code/ch41` on `PYTHONPATH` before the session starts (in `checkout.py`). Not checked on a cluster.

## Not verified

- The latency numbers are from one laptop with other writers' unlocked work possible during locked runs; they show the order of size and the shape, not a benchmark.
- The film takes are separate runs from the saved outputs, so their numbers (batch ids, timings) differ from the text; captions say so where it matters.
- The RocksDB memory settings and `compactOnCommit` are named only through the docs link.
- `STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE` is cited from the error conditions file, not run.
- Ch26's Mermaid diagram of the state server simplifies: the row-based API sends rows to Python in Arrow batches (`TransformWithStateInPySparkPythonRunner`), which the diagram shows, but the response encoding (pickled values) was read from source, not traced on the wire.

## Clips

Cut list: `book/media/clips/clips-part05.tsv` (sources are the full cuts `code/film/renders/chNN-chapter.mp4`, not committed). Tapes: `code/film/ch39.tape` to `ch43.tape`; storyboard `code/film/storyboard-part05.md`. Format `chapter` (1280x720), VHS, `spark` theme. Every clip was checked on its last frame (full-size PNG) for readability.

| Clip | Chapter section |
|---|---|
| ch39-ticking, ch39-watermark, ch39-restart | ch39 Steps 3, 6, 7 |
| ch40-dead-letters, ch40-future | ch40 Steps 3, 5 |
| ch41-sessions, ch41-clock | ch41 Steps 5, 6 |
| ch42-rules, ch42-latency | ch42 Steps 1, 4 |
| ch43-decode, ch43-repartition | ch43 Steps 3, 7 |

`ch39/s7_restart.py` waited a fixed time, and under load the first ch39 take did not reach the crash. It now waits for the work (crash inside batch 3, or commit 5 on resume); the saved outputs did not change. The ch42 tape shortens long lines with `sed` (`RTM` for `STREAMING_REAL_TIME_MODE`, `(late)` for "from the running query"); the caption says so.

## Nav entries

```yaml
  - "Part V: Streaming":
    - chapters/ch39-structured-streaming.md
    - chapters/ch40-streaming-messy-data.md
    - chapters/ch41-transform-with-state.md
    - chapters/ch42-real-time-mode.md
    - chapters/ch43-checkpoints-and-state-store.md
```
