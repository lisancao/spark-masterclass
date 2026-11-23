# v3-streaming notes: Structured Streaming completeness (ch53 new, ch48 and ch50 extended)

Branch `v3-streaming`, worktree `~/spark-masterclass-parts/streaming`. Ports: `SPARK_UI_PORT=4156` (ch53, films), 4157 (ch48 runs that overlapped), 4158 (ch50). Connect port 15116 was not needed (all scripts run Classic). Kafka: own container `sm-v3stream-kafka` (PLAINTEXT `localhost:29116`, SASL_PLAINTEXT `localhost:29117`), removed at the end.

## Scope and changes

- **New: `book/chapters/ch53-kafka-in-depth.md`** (provisional number 89, Part VIII after ch52). Code `code/ch53/` (`kafka.sh`, `k89.py`, `k1`..`k9`, `figures.py`, `anim/replay/`), outputs `code/ch53/output/`, film tape `code/film/ch53.tape`, clips `book/media/clips/ch53-*`, figures `book/media/figures/ch53-*`, cut list `book/media/clips/clips-v3-streaming.tsv`.
- **Extended: ch48** (Steps 1 to 11 keep their numbers; no external anchors or step references changed):
  - Step 2: one sentence pointing to Step 12.
  - Step 8: a second triggers table (engine, sources, stops by itself, admission control, no-data batches, async progress tracking, delivery), with its sources.
  - Step 9: one paragraph pointing to Chapter 53.
  - Step 10: new subsection "An outer join, and the state that a join keeps" (`s10b_join_state.py`): left outer join with lost dropoffs, NULL-row timing, four state stores per partition (`state-metadata`), `joinSide` in the state data source, join-type matrix with and without watermarks.
  - New Step 12 stream-static joins in depth (`s12_stream_static.py`), Step 13 StreamingQueryManager (`s13_manager.py`), Step 14 foreachBatch patterns and `foreach` (`s14_foreach_batch.py`), Step 15 asynchronous progress tracking (`s15_async.py`).
  - Intro sentence (Steps 9 and 15 use Kafka), five new summary bullets.
  - `code/ch48/kafka.sh`: container name can be overridden with `KAFKA_CONTAINER` (default unchanged).
  - Figures `ch48-outer-join`, `ch48-static-refresh`, `ch48-async`; clip `ch48-manager`; film beat `manager` appended to `code/film/ch48.tape`.
- **Extended: ch50**: new Step 9 "The older APIs, and when each is still used" before "Change a processor and keep its state" (`t6_legacy.py`, `t7_groupstate.scala` + `t7_groupstate.sh`, `t6_array_state.py`); intro sentence points to Step 9; two summary bullets; two lines in the run list. Figure `ch50-migrate`; clip `ch50-legacy`; film beat `legacy` appended to `code/film/ch50.tape`.

## Technical names (TN) and technical verbs (TV)

- TN (new in this slice): broker, listener, topic, partition (Kafka), offset, consumer, producer, consumer group, group ID, header, record timestamp, retention, log start offset, delegation token, truststore, keystore, JAAS, SASL, TLS, ACL, admin client, producer pool, consumer pool, metadata, idempotent producer, transaction, log compaction, batch log, marker file, asynchronous progress tracking, StreamingQueryManager, listener (Spark), `GroupState`, timeout, initial state, starting options, ending options.
- TV: subscribe, assign, seek, commit, publish (only in code), resolve (offsets), split (a partition into tasks), log in, encrypt, replay, deduplicate (only as `dropDuplicates`).
- "tracking" (progress tracking), "starting"/"ending" (option names: the starting options) are TNs used as modifiers, never as verbs.

## STE checker results

`scripts/ste-check <file> description --allow <words>`:

| Chapter | Findings | Extra allowlist words |
|---|---|---|
| ch53 | 0 | processing Processing partitioning streaming Streaming monitoring tuning warning starting ending tracking |
| ch48 | 0 | Programming programming tumbling sliding processing Processing grouping encoding partitioning streaming Streaming pruning coalescing monitoring tuning warning checkpointing tracking starting ending |
| ch50 | 0 | processing Processing grouping encoding opening partitioning streaming Streaming pruning coalescing monitoring tuning warning |

Rules broken on purpose: as in `notes/part05.md` (admonition titles and table cells are phrases; quoted output, errors and config keys stay exact; "Thus" starts sentences).

## Environment

- pyspark 4.3.0 (uv project in `code/`), Java 17.0.21.1, Scala 2.13.18 (`spark-shell` from the pyspark package, `SPARK_HOME` set to the package folder in `t7_groupstate.sh`).
- Kafka connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0`, resolved with `kafka-clients` 3.9.2, `spark-token-provider-kafka-0-10_2.13` 4.3.0, `commons-pool2` 2.13.1 (full list in `code/ch53/output/k1_package.txt`).
- Broker: `apache/kafka:4.0.0`, KRaft, one node, SASL/PLAIN users `alice`/`bob` (passwords in `kafka.sh`, local test only).
- Bench lock: only `ch48/s15_async.py` (timings quoted). Three alternated runs per mode after a warm-up; medians. The machine was shared with other writers' Spark jobs during the run; the text says the ratio is the result. An earlier locked run (overlapping my own unlocked runs) gave sync 208 ms / async 49 ms median batch; it was discarded and rerun.
- Films: `film render code/film/ch53.tape --format chapter`; the ch48 and ch50 beats were rendered from scratch copies of the tapes holding only the new beat (`--demo code/`), because the other beats' renders are not in this worktree. VHS, `spark` theme, 1280x720. Clips are jump cuts (typed command, then the last seconds after the output appears, because Python's stdout to `head` is block-buffered); see the cut list. Encoding libx264 crf 28, yuv420p, faststart, no audio, poster 1.5 s before the end.
- Animation: HyperFrames (create-animation kit, paper theme, landscape), `code/ch53/anim/replay/`, 21 s, numbers from `k7_sink.txt` (`data.json`). Frames checked at 4, 9, 13, 19 s. Pacing not watched at 1x by a person.

## Claim-to-source

| Claim | Source |
|---|---|
| Without the package: `KAFKA_DATA_SOURCE_NOT_ENABLED`; resolved jars; schema with/without headers | `ch53/output/k1_package.txt` |
| `0-10` = oldest broker supported; headers need 0.11; timestamp options need 0.10.1 | Kafka integration guide v4.3.0 (title, Linking, timestamp section) |
| Starting option results, timestamp strategy, partitions added later start at earliest | `ch53/output/k2_start.txt` |
| Precedence startingTimestamp > ByTimestamp > startingOffsets | `KafkaSourceProvider.getKafkaOffsetRangeLimit` v4.3.0; guide notes |
| Pooled producer kept 4-partition metadata | first k2 run (all 1,089 trips in p0-p3, p4/p5 offsets 0); not saved as output; final script uses `kafka.client.id` (comment in `k2_start.py`); producer cache 10m from guide, `metadata.max.age.ms` 5 min from Kafka docs |
| Batch read ranges, re-resolution of latest/-1, mode errors | `ch53/output/k3_batch.txt` |
| Refused client options list | `KafkaSourceProvider.validateGeneralOptions` v4.3.0 lines 305-341 |
| Task splits, minPartitions, maxRecordsPerPartition batch vs stream | `ch53/output/k4_admission.txt`; `KafkaOffsetReaderAdmin.getOffsetRangesFromUnresolvedOffsets`; maxRecordsPerPartition first in v4.0.0 (git grep v3.5.0 vs v4.0.0) |
| minOffsetsPerTrigger / maxTriggerDelay timeline | `ch53/output/k4_admission.txt` |
| Sink columns, header filter, partition column, sink errors, save modes | `ch53/output/k5_headers.txt`; guide (topic option overrides column) |
| failOnDataLoss behavior, 600 + 400 | `ch53/output/k6_dataloss.txt` |
| Replay duplicates 1,894 / 1,445 / 449; transactional.id error | `ch53/output/k7_sink.txt` |
| `enable.idempotence` default true since Kafka 3.0 | Kafka documentation (producer configs, KIP-679); Spark's `kafkaParamsForProducer` does not set it (v4.3.0) |
| No consumer groups listed; console consumer lag; source metrics | `ch53/output/k8_groups.txt` |
| AdminClient in driver since 3.1; executor params (auto commit off, group id `-executor`); group id formats | guide "Offset fetching"; `KafkaSourceProvider` v4.3.0 lines 721-780 |
| Pool configs and defaults | guide "Consumer Caching", "Producer Caching" |
| SASL results, disallowed option, static conf | `ch53/output/k9_security.txt` |
| `spark.sql.kafka.disallowedOptions` new in 4.3.0 (SPARK-59328), internal static | commit 9f652833af2 (in v4.3.0, not v4.2.0) |
| `partition.metadata.cache.ttl.ms` new in 4.3.0 (SPARK-49442), default -1 | `KafkaSourceProvider` v4.3.0 line 590; absent in v4.2.0; guide v4.3.0 |
| Delegation token settings, default SASL_SSL | guide "Security" v4.3.0 |
| SSL/SASL_SSL rows | guide and Kafka docs; **not run** |
| Default trigger is `ProcessingTime(0)` | `classic/DataStreamWriter.scala` v4.3.0 line 442 |
| Continuous: Kafka and rate sources | `docs/streaming/performance-tips.md` v4.3.0 |
| Stream-stream outer join timings, state stores, joinSide, join matrix | `ch48/output/s10b_join_state.txt` |
| Stream-static join types, plan, refresh behavior | `ch48/output/s12_stream_static.txt` |
| StreamingQueryManager behavior | `ch48/output/s13_manager.txt` |
| foreachBatch batch log, retry same batch_id, foreach epochs | `ch48/output/s14_foreach_batch.txt` |
| Async progress tracking medians, file counts, limits, turn-off procedure | `ch48/output/s15_async.txt`; `AsyncProgressTrackingMicroBatchExecution.scala` v4.3.0 (sinks, triggers, interval 1000 ms); `docs/streaming/performance-tips.md` v4.3.0 |
| APT on by default in RTM for stateless queries, version 4.3.0 | `SQLConf.STREAMING_ASYNC_PROGRESS_TRACKING_REAL_TIME_MODE_ENABLED_BY_DEFAULT` v4.3.0 lines 4053-4062; `StreamingQueryManager.useAsyncProgressTracking` |
| applyInPandasWithState sessions, state schema, migration results, operator mismatch error | `ch50/output/t6_legacy.txt` |
| ARRAY in applyInPandasWithState state fails | `ch50/output/t6_array_state.txt` |
| mapGroupsWithState / flatMapGroupsWithState output | `ch50/output/t7_groupstate.txt` |
| API versions (2.2, 3.2 initial state, 3.4, 4.0, 4.1) | `KeyValueGroupedDataset.scala` @since tags; `group_ops.py` versionadded, v4.3.0 |

## Open findings

1. **`maxRecordsPerPartition` ignored by Kafka batch reads with `earliest`/`latest`.** The guide says "streaming and batch". A batch read with default offsets and `maxRecordsPerPartition=400` makes 4 tasks (one per Kafka partition); with explicit JSON start and end offsets it makes 9; a stream makes 9. Cause in v4.3.0: `KafkaOffsetReaderAdmin.getOffsetRangesFromUnresolvedOffsets` calls `shouldDivvyUpLargePartitions(offsetRangesBase)` before resolving the sentinel offsets (-2/-1), so each range size is 1. Repro: `ch53/k4_admission.py` part 1, or
   ```python
   spark.read.format("kafka").option("kafka.bootstrap.servers", B).option("subscribe", "t") \
        .option("maxRecordsPerPartition", "400").load().rdd.getNumPartitions()   # = number of Kafka partitions
   ```
   Not checked against master or the 4.2 line. Candidate for the spark-bug skill.
2. **`applyInPandasWithState` fails with a state schema that has an ARRAY (or other camelCase JSON keys).** `KeyError: 'elementType'` in the Python worker. Cause in v4.3.0 (also v4.2.0): `worker_util.Conf.get` lowercases string values by default (`lower_str=True`), and `EvalConf.state_value_schema` reads `state_value_schema` with the default, so the schema JSON arrives lowercased (`elementtype`, and upper-case field names lowercased too). Repro: `ch50/t6_array_state.py` (state `n LONG` runs, `ids ARRAY<STRING>` fails). Probably also MapType (`keyType`) and struct fields with capital letters. Not checked on master.
3. **`spark-shell -i` deadlock with a case class in a streaming `mapGroupsWithState`.** The executor task waits on the class initialization of the REPL line (`$lineN.$read$`, from `OuterScopes.getOuterScope`) while the driver thread runs the query in that line. A REPL limitation more than a Spark bug; documented in ch50 as a note, worked around with tuples. Thread dump taken (not saved).
4. Kafka producer pool keeps stale topic metadata after a partition increase (up to `metadata.max.age.ms`). Expected Kafka client behavior, documented in ch53 Step 2, not a Spark issue.

## Not verified

- `SSL`, `SASL_SSL`, delegation tokens (no Kerberos realm), `partition.metadata.cache.ttl.ms` (not measured), Delta/Iceberg as the static side of a stream-static join (table row says not tested), the two continuous-trigger rows of the ch48 triggers table.
- The cause of the timeout without credentials on the SASL port (text says "probable cause, not verified").
- Pacing of the clips and the animation at 1x.

## Visual assets

| File | Where |
|---|---|
| `book/media/figures/ch53-start-positions-{light,dark}.svg` | ch53 Step 2 |
| `book/media/figures/ch53-tasks-{light,dark}.svg` | ch53 Step 4 |
| `book/media/clips/ch53-dataloss.mp4/.webp` | ch53 Step 6 |
| `book/media/clips/ch53-anim-replay.mp4/.webp` | ch53 Step 7 (animation) |
| `book/media/clips/ch53-sink.mp4/.webp` | ch53 Step 7 |
| `book/media/clips/ch53-security.mp4/.webp` | ch53 Step 9 |
| `book/media/figures/ch48-outer-join-*.svg` | ch48 Step 10 |
| `book/media/figures/ch48-static-refresh-*.svg` | ch48 Step 12 |
| `book/media/clips/ch48-manager.mp4/.webp` | ch48 Step 13 |
| `book/media/figures/ch48-async-*.svg` | ch48 Step 15 |
| `book/media/figures/ch50-migrate-*.svg` | ch50 Step 9 |
| `book/media/clips/ch50-legacy.mp4/.webp` | ch50 Step 9 |

Mermaid: ch53 has three (flow of driver/executors/broker, start-position decision, replay sequence). ch48 gains one (async sequence). Figures rendered with headless Chrome and checked in light and dark.

## Nav entries and roadmap rows (for the integrator)

```yaml
  - "Part VIII: Streaming":
    - chapters/ch48-structured-streaming.md
    - chapters/ch49-streaming-messy-data.md
    - chapters/ch50-transform-with-state.md
    - chapters/ch51-real-time-mode.md
    - chapters/ch52-checkpoints-and-state-store.md
    - chapters/ch53-kafka-in-depth.md
```

Roadmap rows:

```markdown
| 39 | [Structured Streaming fundamentals](chapters/ch48-structured-streaming.md) | Sources and sinks, output modes, triggers, windows, watermarks and checkpoints; stream-static and stream-stream joins and their state, the StreamingQueryManager, foreachBatch patterns, asynchronous progress tracking |
| 41 | [Stateful processing with transformWithState](chapters/ch50-transform-with-state.md) | Timers, TTL, state inspection; the older mapGroupsWithState, flatMapGroupsWithState and applyInPandasWithState, and moving their state |
| 44 (new, after 43) | [Kafka with Spark in depth](chapters/ch53-kafka-in-depth.md) | The connector package, starting and ending offsets, batch reads, batch sizes and task splits, headers and the sink columns, failOnDataLoss, at-least-once sink and idempotent readers, consumer groups and lag, SASL/SSL. **4.3:** `spark.sql.kafka.disallowedOptions`, `partition.metadata.cache.ttl.ms` |
```

Gap-audit rows closed: Kafka integration (offsets options, headers, security, consumer caching, sink semantics) by ch53; Joins stream-static (mention -> full) and Monitoring `spark.streams` by ch48 Steps 12-13; Async progress tracking (none -> full) by ch48 Step 15; Arbitrary stateful legacy APIs (mention -> comparison) by ch50 Step 9.

## Commits

See `git log book-v2..v3-streaming`.
