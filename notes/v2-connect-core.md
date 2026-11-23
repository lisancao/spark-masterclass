# v2 Connect core: notes (branch v2-connect-core)

Scope: the core of the new Spark Connect part. ch18 rewritten as "Why Spark Connect should be your default" (stays the first chapter of the Connect part); new ch19 (the protocol) and ch20 (session observability). Clients in other languages, building an app on a client and writing a client are ch21 to ch23 (other writer, `connect-clients` worktree).

| File | Provisional number | Title |
|---|---|---|
| `book/chapters/ch18-spark-connect.md` | 4 (kept) | Why Spark Connect should be your default |
| `book/chapters/ch19-connect-protocol.md` | 80 | Inside the Spark Connect protocol |
| `book/chapters/ch20-connect-observability.md` | 81 | Session observability: what does this client run, and how do I stop it? |

## What changed in existing files

- `book/chapters/ch18-spark-connect.md`: rewritten. Kept: the architecture mermaid, Step 1 (server), Step 2 (the unresolved plan), Step 3 (run it and break it, the failure walkthrough), the "not available" material (now Step 7, with a table of replacements), the 4.3 operation ID and REST material (now Step 8), the closing paragraph of Part I. New: the default note (`spark.api.mode` defaults to `classic` in 4.3), an architecture figure, Step 4 (what Connect fixes, as a table), Step 5 (Classic vs Connect measured, with a kill test and install sizes), Step 6 (costs: round trips, lazy errors, temp views by name, UDF closures, Python version coupling), Step 9 (three ways to make Connect the default, with a caution about port 15002). No claim about future defaults; the chapter argues for the reader's default today.
- `code/ch18/server.sh`: ports are now `CONNECT_PORT` (default 15002) and `CONNECT_UI_PORT` (default 4041), and extra `--conf` arguments pass through to the server. Defaults unchanged, so ch59/ch24 users of this script are unaffected (they use `code/ch59/server.sh` anyway).
- `code/ch18/connect.py`: reads the two port variables; output regenerated (`code/ch18/output/connect.txt`).
- `code/film/ch18.tape`: setup exports `BOOK_DATA`, `CONNECT_PORT=15105`, `CONNECT_UI_PORT=4145`; the REST beat uses `$CONNECT_UI_PORT`; new beat `thin`. All three ch18 clips were re-filmed (`ch18-connect`, `ch18-rest`, new `ch18-thin`).
- `.gitignore`: added `code/**/anim/**/renders/` and `code/film/renders/`.
- Not edited: ch61, ch24, ch72 (overlaps below).

New code: `code/ch18/{compare.py,sizes.sh,thin_client.py,api_mode.py}`, `code/ch19/{proxy.py,protocol.py,wire.py,catalog.py,reattach.py,bench.py}`, `code/ch19/anim/round-trip/` (HyperFrames source, storyboard, data.json), `code/ch20/observe.py`, tapes `code/film/ch19.tape`, `code/film/ch20.tape`. No helper added to `code/common/book.py` (each script reads `CONNECT_PORT` itself).

## Cross-references (for the integrator)

- ch18, ch19 and ch20 link each other as `ch19-connect-protocol.md` and `ch20-connect-observability.md`. If the integrator renumbers the Connect part, run `scripts/renumber.py` over these three files.
- **Chapter 21 and Chapter 23 are plain text, not links**, because the connect-clients writer had not created those files when this branch was written. Places: ch18 Step 4 table ("Chapter 21"), ch19 intro ("Chapter 21 ... Chapter 23") and ch19 Step 8 ("Chapter 21 shows these clients"). Turn them into links when ch21/ch23 land.
- Links to existing chapters use current numbers: 2, 3, 43 (Step 10), 44, 45 (Steps 7, 11), 46 (Steps 1, 2, 4, 5, 6), 49, 57 (Step 3).

## Overlaps with chapters I did not edit

- **ch61 Step 7** introduces the Connect REST endpoints with a table and two clients (analyst/agent, `daily-report` tag). ch20 does not repeat the table; it links to it and focuses on "what runs now" (state filter `STARTED/COMPILED/READY`) and stopping. ch20 Step 1 also uses an analyst and an agent; the integrator may want to rename one pair. ch61 Step 7 says "In Chapter 18, the same error had no jobs": still true in the new ch18 output (`jobIds: []`).
- **ch61 Step 8** (History Server) and **Step 11** (structured logging): ch20 links, does not repeat.
- **ch24 Step 6** (life of a session, `interruptTag` from a supervisor thread, the per-thread tag trap): ch20 Step 4 links to it and adds the cross-process attach with `session_id=`, which ch24 does not cover. Both chapters show an interrupt; ch24's is by tag in-process, ch20's by operation ID from a second process.
- **ch24 Steps 4-5** (token, TLS proxy): ch20 has a CAUTION that `user_id` is unauthenticated and links there.
- **ch59 Step 9** ("Spark Connect analyzes each action again") overlaps ch18 Step 6 (round trip per action) in spirit; **ch59 Step 10** (reattach) is linked from ch19 Step 7 and ch20 Step 5. ch19 Step 7 forces reattach by a small `senderMaxStreamSize`, ch59 by cutting a connection; complementary.
- **ch72 Step 3** decodes the protobuf wire format by hand (an `Any`). ch19 Step 4 walks a whole plan with descriptors and links to ch72 Step 3 for the hand decode.
- **ch72 Step 11** says "Chapter 18 showed the 4.3 REST endpoint connect/operations": still true (ch18 Step 8).
- **ch60 Step 9** (artifacts): ch18 Step 7 table links to it for `addArtifacts`.

## Technical names and verbs

- Names: Spark Connect, Connect server, client, session, `SparkSession`, operation, operation ID, job tag, operation tag, session tag, `user_id`, `session_id`, `user_agent`, `client_type`, plan, `Plan`, `Relation`, `Expression`, `Command`, `CompressedOperation`, protobuf, gRPC, RPC, `ExecutePlan`, `AnalyzePlan`, `Config`, `AddArtifacts`, `ArtifactStatus`, `Interrupt`, `ReattachExecute`, `ReleaseExecute`, `ReleaseSession`, `FetchErrorDetails`, `CloneSession`, `GetStatus`, response stream, `ExecutePlanResponse`, `arrow_batch`, `execution_progress`, `result_complete`, metrics, Arrow, Arrow IPC stream, record batch, validity bitmap, varint, wire type, key, call site (`origin`), descriptor, logging proxy, Py4J, JVM, `pyspark-client`, `spark.api.mode`, REST API, Connect tab, listener, listener bus, event log, History Server, progress handler, kill test, RSS, benchmark lock.
- Verbs: send, stream, serialize, decode, analyze, resolve, interrupt, reattach, release, attach, collect, pickle, unpickle, log, render.

## STE check results

`scripts/ste-check <file> description --allow <words>`:

| Chapter | Findings | Allow words |
|---|---|---|
| ch18 | 0 | none |
| ch19 | 0 | `logging` (modifier in "logging proxy"), `timing` (in a caption, "The timing is schematic"), `padding` (Arrow term), `encoding` (doc title "Protocol Buffers encoding"), `Building` (post title) |
| ch20 | 0 | `logging` ("structured logging", "logging proxy") |

Rules broken on purpose: none beyond the allow words. Several sentences that start with a link (`[Chapter 19](...) opens ...`) were rewritten because the lint joins them with the previous sentence. The chapter titles of ch20 and ch18 contain a colon and a question; they are titles.

## Environment

- pyspark 4.3.0 (`pyspark[connect]==4.3.0` in `code/pyproject.toml`), grpcio 1.85.3, pyarrow 26.0.1, Python 3.12.3, OpenJDK 17.0.21.1, 16-core Linux, 156 GB RAM.
- `pyspark-client==4.3.0` in a scratch venv for `sizes.sh` (`/tmp/claude-1000/.../scratchpad/connect-core/thin`).
- Ports: Connect gRPC 15105, UI 4145 (`CONNECT_PORT`, `CONNECT_UI_PORT`); the logging proxy on 25105 (`PROXY_PORT`; 15106 was taken by another writer's server).
- Server configs per script: `compare.py` and `bench.py` ran on a server with `spark.driver.memory=4g`; `reattach.py` with `spark.connect.execute.reattachable.senderMaxStreamSize=16m`; `observe.py` with `spark.eventLog.enabled=true` and `spark.eventLog.dir=file://.../data/ch20-events`.
- Locked runs (bench lock): `ch19/bench.py` (5 runs per cell after a warmup, medians) and `ch18/compare.py` (10 query runs and 20 schema lookups, medians; it was run a second time with nothing else on the machine, and the text quotes that run).
- Animation: `anim` (create-animation skill), HyperFrames backend, theme `paper`, landscape, re-encoded to 1280x720 with libx264 CRF 30 (236 KB).
- Films: `film render code/film/chNN.tape --format chapter --demo code`.

## Claim-to-source table

| Claim | Source |
|---|---|
| `spark.api.mode` default `classic` (env `SPARK_CONNECT_MODE=1` makes it `connect`) | `core/src/main/scala/org/apache/spark/internal/config/package.scala` at v4.3.0, line 3188 |
| `spark.api.mode=connect` gives a Connect session locally | `code/ch18/output/api_mode.txt` |
| Classic 3.83 s / Connect 0.28 s start; 91/168 ms query; 1.0/2.7 ms `df.columns`; 864/136 MB RSS; kill results; temp view 3/7; UDF 123/456 | `code/ch18/output/compare.txt` |
| pyspark 484 MB, jars 462 MB in 280 files; pyspark-client 11 MB; client venv 324 MB; no jars, no py4j | `code/ch18/output/sizes.txt` |
| Context manager SPARK-58159, client operation ID SPARK-58547, REST endpoints SPARK-57941 | kept from the v1 ch18 (checked by the Part I writer) |
| `PYTHON_VERSION_MISMATCH` error | `python/pyspark/errors/error-conditions.json` at v4.3.0 |
| Eager vs Lazy doc page exists in 4.3 | `docs/spark-connect-gotchas.md` at v4.3.0 |
| Relation 62 kinds, Expression 22, Command 20, Plan 3; 12 RPCs and their streams; response_type list | `code/ch19/output/catalog.txt` (descriptors of pyspark 4.3.0) and `base.proto` lines 1311-1366 |
| RPC table of one session; response stream of `df.collect()` | `code/ch19/output/protocol.txt` |
| `create()` sends no RPC; first Config reads `spark.python.sql.dataFrameDebugging.enabled`; plan compression keys | `data/ch19-wire/rpc.jsonl` of that run (not in git); keys summarized by the proxy |
| planCompression threshold 10 MB, progress reportInterval 2 s, detachedTimeout 5m, senderMaxStreamSize 1g, senderMaxStreamDuration 2m, arrow.maxBatchSize 128 MB | `sql/connect/server/src/main/scala/org/apache/spark/sql/connect/config/Connect.scala`, `ConnectCommon.scala` (`CONNECT_GRPC_MAX_MESSAGE_SIZE = 128 * 1024 * 1024`) at v4.3.0 |
| 70% of maxBatchSize; batch rows from `arrowMaxRecordsPerBatch` | `SparkConnectPlanExecution.scala` lines 137-141 at v4.3.0 |
| `spark.sql.execution.arrow.maxRecordsPerBatch` default 10000 | `SQLConf.scala` at v4.3.0 |
| Plan of 592 bytes, 187 without `origin`; field walk; ExecutePlanRequest fields; response of 662 bytes; Arrow stream 496 bytes, 232/184/56 byte parts, body bytes | `code/ch19/output/wire.txt` |
| 385 batches of 10,000 rows, 327,056 bytes median, 120 MiB Arrow vs 117 MiB pandas, 392 ReleaseExecute, 7 ReattachExecute | `code/ch19/output/reattach.txt` |
| toPandas 20.38 / 0.60 / 0.64 s; collect 3.60 / 3.48 / 3.17 s | `code/ch19/output/bench.txt` |
| `GetStatus` only through `client._get_operation_statuses()` in the 4.3 Python client; `ArtifactStatus` for cached artifacts | `pyspark/sql/connect/client/core.py`, `client/artifact.py` in pyspark 4.3.0 |
| Interrupt acts in one session found by (user_id, session_id) | `SparkConnectInterruptHandler.scala` at v4.3.0 |
| Execution states and the `isExecutionActive` test; UI shows `RUNNING` | `SparkConnectServerAppStatusStore.scala` lines 133-151, `SparkConnectServerPage.scala` line 336 |
| State per event | `SparkConnectServerListener.scala` lines 171-283 |
| Session count lags: `updateLiveStore` writes at most once per `spark.ui.liveUpdate.period` (100ms) | `SparkConnectServerListener.scala` lines 310-318, `core/.../config/Status.scala` line 29 |
| Connect listener events and fields (`errorMessage` on Failed) | `ExecuteEventsManager.scala`, `SessionEventsManager.scala` at v4.3.0 |
| All ch20 IDs, states, log lines, events, timings (475 ms, 206 ms, 7.8 s) | `code/ch20/output/observe.txt` |
| Connect tab screenshot | `book/media/figures/ch20-connect-tab.png`, taken by the same run of `observe.py` as `observe.txt` |

## Figures, animation and clips

- Figures: `ch18-architecture.svg`, `ch18-measured.svg`, `ch19-plan-tree.svg`, `ch19-arrow-batch.svg`, `ch20-ids.svg` (transparent background, mid-tone colours, checked rendered on white and on #1e1e1e), `ch20-connect-tab.png` (UI screenshot).
- Animation: `book/media/clips/ch19-anim-round-trip.mp4` + `.webp` (32 s, sizes from `protocol.txt`, timing labelled schematic). Pacing not reviewed by Lisa at 1x.
- Film clips: `ch18-connect`, `ch18-rest`, `ch18-thin`, `ch19-proxy`, `ch19-arrow`, `ch20-stop` (cut list in `book/media/clips/clips-connect-core.tsv`). Film runs are separate runs, so IDs and small counts in the films differ from `output/`; the captions say so where it matters.

## Open findings

1. **Session `totalExecution` lags while an operation runs (UI and REST).** Repro: start a Connect server; open a new session and immediately run a long query (`spark.range(0, 10**11, numPartitions=64).where("id % 7 = 3").count()` in a thread); within the query, `GET connect/sessions` shows `totalExecution: 0` for that session, and the Connect tab shows "Total Execute 0", while `connect/operations` shows the operation active. Cause, from source: `onOperationStarted` increments the count through `updateLiveStore`, which skips the KV store write when the session entity was written less than `spark.ui.liveUpdate.period` (100ms) ago; the session was created by the same `ExecutePlan` 29 ms earlier, so the new count is written only at the next session event. Possible improvement or doc note; not called a bug in the book.
2. **Any client that knows `(user_id, session_id)` can attach to and control a session** (interrupt, release) on a server without authentication. By design (the client asserts `user_id`), shown in ch20 Step 4 with a CAUTION. Possibly worth a sentence in the official Connect docs.
3. **`ExecutionInfo.state` stays at the last state `CLOSED`;** the REST record does not keep whether the operation finished, failed or was cancelled, except through `detail` for errors. The event log keeps the `Canceled` event. Doc gap, not verified as intended or not.

## Not verified

- Network effects: all measurements ran with client and server on one host (stated in ch18 and ch19).
- Version skew between client and server on 4.3 was not tested (ch18 Step 4 says the protocol is the contract, not that every pair works).
- `CloneSession` and `GetStatus` were listed from the proto and client source, not run.
- The mermaid state diagram in ch20 Step 5 shows the transitions that the listener allows; only `READY -> CANCELED -> CLOSED` and `READY -> FINISHED -> CLOSED` (analyst query) were observed.

## Nav and roadmap rows (for the integrator)

Nav (a new Connect part; ch18 also stays the last chapter of Part I in the current nav unless the integrator moves it):

```yaml
  - "Part ?: Spark Connect":
    - "4. Why Spark Connect should be your default": chapters/ch18-spark-connect.md
    - "80. Inside the Spark Connect protocol": chapters/ch19-connect-protocol.md
    - "81. Session observability": chapters/ch20-connect-observability.md
```

Roadmap rows:

```markdown
| 4 | [Why Spark Connect should be your default](chapters/ch18-spark-connect.md) | Thin client and unresolved plan, what it fixes and costs, Classic vs Connect measured, how to make it the default. **4.3:** operation IDs and REST endpoints |
| 80 | [Inside the Spark Connect protocol](chapters/ch19-connect-protocol.md) | Relation, Expression, Command; the twelve RPCs captured with a logging proxy; a plan and an Arrow batch decoded byte by byte; reattach; Arrow vs pickled rows measured |
| 81 | [Session observability](chapters/ch20-connect-observability.md) | User, session and operation IDs, tags, the Connect tab, what runs now, interrupt from a second process, server logs and Connect listener events |
```
