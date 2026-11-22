# v2-connect-clients: Chapters 82, 83, 84 (Spark Connect part)

Worktree `~/spark-masterclass-parts/connect-clients`, branch `v2-connect-clients`. Ports: Connect 15106, UI 4146.

## Scope

New chapters (provisional numbers):

- `book/chapters/ch82-connect-clients.md`: Spark Connect clients beyond Python (Scala, Go, Rust, Swift, with Python as the reference).
- `book/chapters/ch83-connect-application.md`: Building an application on a Connect client (Rust, Polars, DuckDB; crossover; errors, retries, reattach, configuration).
- `book/chapters/ch84-writing-a-connect-client.md`: Writing a Spark Connect client (Python, no pyspark).

No existing chapter changed. Shared files added:

- `code/common/svgfig.py` and `code/common/build_anim.sh`: byte-for-byte copies of the files on `v2-vis-a`, so the merge sees identical additions.
- `code/common/cut_clips.sh`: new. Cuts book clips from per-beat film renders with a clips TSV (the `cut.sh` that `mkdocs.yml` names is not in this worktree).

Cross-references point to the sibling chapters of `v2-connect-core` (`ch80-connect-protocol.md`, `ch81-connect-observability.md`). `mkdocs build --strict` fails only on these links until that branch merges (7 warnings, all of that kind).

OCaml is not mentioned in the three chapters.

## Technical names and verbs used

Spark Connect, Connect server, client, session, session ID, operation ID, plan, `Relation`, `Expression`, `UnresolvedFunction`, `LocalRelation`, `Command`, `ExecutePlan`, `AnalyzePlan`, `ReattachExecute`, `ReleaseExecute`, `ReleaseSession`, `FetchErrorDetails`, `Config`, `Interrupt`, `AddArtifacts`, `ResultComplete`, `ArrowBatch`, response stream, reattachable execution, gRPC, protobuf, `protoc`, wire type, tag, Arrow, Arrow IPC, record batch, validity bitmap, arrow-rs, polars-arrow, Polars, DuckDB, `ArrowBatchRegistration`, crate, module, pseudo-version, package, JAR, Scala CLI, SwiftPM, Docker image, error class, SQLSTATE, `ErrorInfo`, retry policy, jitter, crossover, break-even, tag (`add_tag`), benchmark lock. Verbs: compile, build, deploy (not used), decode, encode (not used as a verb), reattach, release, retry, serialize (not used), stream (noun only).

## Environment and versions

| Item | Version / coordinates |
|---|---|
| Server | pyspark 4.3.0 pip install, OpenJDK 17.0.21.1, `code/ch82/server.sh` (driver 8g, `spark.driver.maxResultSize=4g`; `small-streams` adds `spark.connect.execute.reattachable.senderMaxStreamSize=1m`) |
| Python client | `pyspark==4.3.0` (code venv) for the taxi query and probe; `pyspark-client==4.3.0` (PyPI, 2026-10-30, requires Python >=3.11) for the matrix run |
| Scala client | `org.apache.spark:spark-connect-client-jvm_2.13:4.3.0`, Scala 2.13.17, Scala CLI 1.19.0 (`virtuslab/scala-cli:latest`), `--jvm 17`, `--add-opens=java.base/java.nio=ALL-UNNAMED` |
| Go client | `github.com/apache/spark-connect-go/v40` at `v40.0.0-20250609164624-defb85250881` (the commit of tag v0.1.0, 2025-06-17); Go 1.27.4 (`golang:latest`); grpc-go 1.72.2, protobuf-go 1.36.6, arrow-go 18.3.0 |
| Rust client | `apache-spark-connect = "=4.3.0"` (crates.io 2026-11-16); Rust 1.99.1 (`rust:bookworm` + `protobuf-compiler libprotobuf-dev cmake`, `code/ch82/docker/rust.Dockerfile`); tonic 0.14.9, prost 0.14.6, arrow-rs 59.3.3 |
| Swift client | `spark-connect-swift` 0.8.0 (2026-11-13), Swift 6.4.3 (`swift:latest`); grpc-swift-2 2.4.4, swift-protobuf 1.41.0, grpc-swift-nio-transport 2.12.0 |
| ch83 app | apache-spark-connect 4.3.0, arrow 60, duckdb 1.20000.2 (DuckDB 2.0, bundled, vtab-arrow), polars 0.55.2 |
| ch84 env | grpcio 1.85.3, grpcio-tools 1.85.3, protobuf 7.36.2, pyarrow 26.0.1, googleapis-common-protos 1.76.0 (`uv run --isolated --no-project`, pyspark not importable) |
| Data | `yellow` (June 2026) for ch82/ch84; `yellow-h1` (Jan to Jun 2026, 22,836,515 trips) for ch83 |
| Machine | 16 cores, Linux, server and clients on the same host |

Build caches (cargo, Go, Swift, Scala) live in the scratchpad (`SCRATCH`), never in the repo. The scala-cli image needs `COURSIER_CACHE` and `COURSIER_ARCHIVE_CACHE` set when it runs as a non-root user; without them it writes a JDK into `./?/` in the working directory (that happened once, see "History" below).

## Claim-to-source

| Claim | Source |
|---|---|
| Five clients return the same five rows (237 177610 14.15 ...) | `code/ch82/output/{python,scala,go,rust,swift}-taxi.txt` |
| Go tag v0.1.0 does not resolve; commit defb8525 does | `code/ch82/output/go-install.txt`; `go.mod` of the repo (module `.../v40`) |
| Go gaps (no `And`, no `Version`, int not int64 in `CreateDataFrame`, `*arrow.Table`) | compile/run errors during this work; `spark/sql/column/column.go`, `sparksession.go:230-260` at v0.1.0 |
| Go executions not reattachable by default; reader errors "the result is not complete" | `spark/client/options/options.go`, `spark/client/client.go:426` at v0.1.0; `code/ch82/output/reattach-small-streams.txt` |
| Rust build needs protoc and libprotobuf-dev | observed build failure `google/protobuf/any.proto: File not found` with `protobuf-compiler` only (not saved to output) |
| Scala needs `--add-opens` on Java 17 | `code/ch82/output/scala-without-add-opens.txt` |
| Swift "still experimental", Swift tools 6.4.0, own Arrow reader | GitHub release 0.8.0 notes; `Package.swift`; `Sources/SparkConnect/Arrow*.swift` |
| Probe results | `code/ch82/output/*-probe-default.txt` |
| 16 ReattachExecute calls each (Python, Scala, Rust, Swift), Go 0 | `code/ch82/output/reattach-small-streams.txt` (server log counts) |
| Matrix: pyspark-client 43 pass / 2 skipped; Scala 39 pass / 2 fail (error names) / 4 skipped | `code/ch82/output/matrix-summary.txt`, `output/matrix/*.json` |
| Build times 62 / 25 / 215 s, sizes 20 / 28 / 101 MB | `code/ch82/output/build-facts.txt` (single clean build each) |
| Time to result medians 1.27 / 1.91 / 0.34 / 0.26 / 0.34 s | `code/ch82/output/taxi-timings.txt` (bench lock, 5 runs after a warmup) |
| Stream limits 2 min / 1 GB, static, internal | `Connect.scala` at v4.3.0 (`senderMaxStreamDuration`, `senderMaxStreamSize`) |
| App: 6,145 rows, 145.4 KiB, borough shares, DuckDB top 5, error fields | `code/ch83/output/app.txt` (bench lock, single run) |
| Tags and user visible in REST | `code/ch83/output/rest-tags.txt` |
| Crossover table and break-even | `code/ch83/output/crossover.txt` (bench lock, median of 3 after a warmup) |
| 62 ReattachExecute calls for 71.1 MiB; server back after 8 s, session at 18.1 s | `code/ch83/output/resilience.txt` |
| Rust default retry policy (50 ms, x4, 60 s, 15 retries, jitter) | `crates/spark-connect-core/src/retries.rs` at v4.3.0 |
| Rust client does not read `SPARK_REMOTE`; reads `SPARK_CONNECT_AUTHENTICATE_TOKEN`, `SPARK_CONNECT_USER_AGENT` | grep of the crates at v4.3.0 (`channel.rs`) |
| Client polars feature uses polars 0.54, arrow 59 | workspace `Cargo.toml` of spark-connect-rust v4.3.0 |
| 84-byte request, field offsets | `code/ch84/output/step1_bytes.txt` |
| Response stream shape | `code/ch84/output/step2_execute.txt` (order of responses varies between runs) |
| Arrow batch of 328 bytes, body bytes | `code/ch84/output/step3_arrow_bytes.txt` |
| 21 fields, 320-byte plan, node chain, five rows | `code/ch84/output/step4_dataframe.txt` |
| ErrorInfo metadata, gRPC INTERNAL, 67 stack frames | `code/ch84/output/step5_errors.txt` |
| 16 ReattachExecute calls in the hand-written client | `code/ch84/output/step6_reattach.txt` |
| 11 proto files, 376 message types, 212 lines | `code/ch84/figures/make_figures.py` computes them from `build/proto` and `miniconnect.py` |

## Open findings (not filed, not called bugs in the book)

1. **Go client v0.1.0: tag cannot be fetched.** Module path `.../v40` with tag `v0.1.0`: `go get ...@v0.1.0` fails with "module path includes a major version suffix, so major version must match". Repro: `code/ch82/go_install.sh`.
2. **Go client v0.1.0: `CreateDataFrame` panics on `int64` for a LONG column** (`interface conversion: interface {} is int64, not int`, `sparksession.go:253`). Repro: `[][]any{{int64(1), "x"}}` with `types.LONG`.
3. **Go client v0.1.0: no reattach.** `ReattachExecution` defaults to false; when true, an early stream end returns "the result is not complete" instead of calling `ReattachExecute` (`client.go:426`). Not run with true.
4. **Scala client error class names differ from PySpark** for the RDD access case (`UNSUPPORTED_CONNECT_FEATURE.RDD` vs `JVM_ATTRIBUTE_NOT_SUPPORTED`) and session mismatch (`CONNECT.SESSION_NOT_SAME` vs `SESSION_NOT_SAME`). Also present in the matrix's existing 4.2.0-client runs.
5. **Rust client docs:** the installation page does not say that Debian needs `libprotobuf-dev` next to `protobuf-compiler`.
6. **Full pyspark vs pyspark-client:** with the full `pyspark` 4.3.0 package, the matrix case `api.jvm_attribute.rdd` gets `NOT_IMPLEMENTED`; with `pyspark-client` it passes. Not investigated.

## Not verified

- Swift and Scala features beyond the probe (streaming, UDFs). The chapters say so.
- The `pyspark-client-rust` PyPI package (mentioned only as the project's own claim).
- The Go client with `ReattachExecution=true` (source reading only).
- Crossover on a real cluster with a network (stated as a direction, not measured).

## STE

`scripts/ste-check` with the shared allowlist only, no chapter `--allow` words:

| Chapter | Findings |
|---|---|
| ch82 | 0 |
| ch83 | 0 |
| ch84 | 0 |

Rules broken on purpose: none flagged. Some code identifiers sit in long table cells; tables are outside the sentence checks.

## Visuals and clips

| File | Kind | Source of the numbers |
|---|---|---|
| `ch82-landscape` | SVG figure, light/dark | build files of each client |
| `ch82-probe` | SVG figure | probe outputs, reattach check |
| `ch82-measured` | SVG figure | `taxi-timings.txt`, `build-facts.txt` |
| `ch82-anim-clients` | HyperFrames animation | `*-taxi.txt` (timing schematic) |
| `ch83-architecture` | SVG figure | `app.txt`, `crossover.txt` |
| `ch83-crossover` | SVG chart | `crossover.txt` |
| `ch83-anim-batches` | HyperFrames animation | `app.txt` (task and batch count schematic) |
| `ch84-layers` | SVG figure | `miniconnect.py`, `env.txt`, proto files |
| `ch84-request-bytes` | SVG figure | `step1_bytes.txt` |
| `ch84-anim-layers` | HyperFrames animation | `step4_dataframe.txt` (timing schematic) |

Clips (`code/film/ch82.tape`, `ch83.tape`, `ch84.tape`, `--format chapter`; cut list `book/media/clips/clips-connect-clients.tsv`; cut with `code/common/cut_clips.sh`): `ch82-rust`, `ch82-swift`, `ch82-probe`, `ch83-heavy`, `ch83-local`, `ch83-retry`, `ch84-bytes`, `ch84-stream`, `ch84-reattach`. The films ran without the bench lock; the captions quote no timing.

| Clip | On screen | VO (draft) |
|---|---|---|
| ch82-rust | Rust query code, then the run in Docker | The Rust client builds the plan; the 4.3.0 server runs it and the rows come back. |
| ch82-swift | Swift query code, then the run | The same plan from Swift, with Swift operators. |
| ch82-probe | Go probe | The same checks in Go; only zip has no API. |
| ch83-heavy | App sections 1 to 3 | Six months go to Spark as a plan; 145 KiB of Arrow comes back into Polars. |
| ch83-local | Polars borough shares, DuckDB QUALIFY | Follow-up questions in Polars and DuckDB, with no round trip. |
| ch83-retry | `resilience.sh wait` | The server is down at the start; the client retries until it is back. |
| ch84-bytes | `step1_bytes` | 84 bytes of request, field by field, without pyspark. |
| ch84-stream | `step2_execute` | One ExecutePlan, a stream of responses, a pyarrow table. |
| ch84-reattach | `step6_reattach` | The server ends each stream early; the client reattaches, then releases. |

## History (needs Lisa)

Commit `ad8f6d8` ("WIP: ch82 clients and probes, ch84 minimal client code") accidentally contains a JDK download cache of about 595 MB under `code/ch82/scala/?/` (scala-cli in Docker, run as a non-root user, used `?` as its home). A later commit removes it and adds `code/ch82/scala/.gitignore`, but the objects stay in the branch history. A hook blocked a history rewrite. Before this branch is pushed or merged, either rewrite it (`git filter-branch --index-filter 'git rm -r --cached --ignore-unmatch "code/ch82/scala/?"' 3f6ec08..HEAD`) or squash-merge it.

## Nav entries and roadmap rows

```yaml
      - "82. Spark Connect clients beyond Python": chapters/ch82-connect-clients.md
      - "83. Building an application on a Connect client": chapters/ch83-connect-application.md
      - "84. Writing a Spark Connect client": chapters/ch84-writing-a-connect-client.md
```

| # | Chapter | Covers |
|---|---|---|
| 82 | [Spark Connect clients beyond Python](chapters/ch82-connect-clients.md) | The Scala, Go, Rust and Swift clients: releases, install, the same taxi query against 4.3.0, a capability probe, reattach, the matrix, build size and time |
| 83 | [Building an application on a Connect client](chapters/ch83-connect-application.md) | A Rust app with Polars and DuckDB on Arrow results; where the crossover between local and Spark is; errors, retries, reattach, configuration |
| 84 | [Writing a Spark Connect client](chapters/ch84-writing-a-connect-client.md) | A client from the 4.3.0 protos in Python without pyspark: the bytes of a request, the response stream, Arrow, AnalyzePlan, a DataFrame builder, errors, reattach, and what a full client needs |
