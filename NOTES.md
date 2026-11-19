# Notes: where the numbers come from

Every number in the book traces to a script under `code/` and its saved output under `code/chNN/output/`.
This file records the environment, the runs behind the measured numbers, and findings still to check.

## Environment (2026-11-18)

- Linux 7.0.0, 16 cores, `local[*]`
- OpenJDK 17.0.21.1 (host); the Docker image runs Java 21.0.13
- Python 3.12.3; `pyspark[connect]==4.3.0` (pyarrow 26.0.1, pandas 2.3.3); pyspark 4.3.0 requires Python >= 3.11
- Data: NYC TLC yellow trips, June 2026 (`yellow_tripdata_2026-06.parquet`, sha256 f340c62a...088b, 65,465,637 bytes,
  4 row groups, 3,837,248 rows) and `taxi_zone_lookup.csv` (sha256 1a99e105...c8ed, 265 zones)

## Part I

| Chapter | Claim | Source |
|---|---|---|
| 1 | one `collect()` -> 4 jobs, 8 stages listed, 4 run; 16 scan tasks, 4 read rows; 96 then 24 shuffle rows | `ch01/output/anatomy.txt` |
| 1 | single-task execution: 82.8 vs 49.3 ms per query, 5 vs 2 jobs (three runs: 81.6-86.7 ms vs 48.1-50.2 ms) | `ch01/output/feather.txt`, runs in this session |
| 1 | a `header=true` CSV read alone runs 1 job | scratch probe, 2026-11-18 |
| 1 | Python UDF: `ArrowEvalPython`, 0 Python processes before, 17 at peak, 2.0 s | `ch01/output/python_workers.txt` |
| 1 | `spark.sql.execution.pythonUDF.arrow.enabled` is `true` by default | `spark.conf.get` on 4.3.0 |
| 2 | pip `pyspark/` 484 MB, 280 jars; tarball 599 MB unpacked, sha512 OK; image 1.33 GB, Python 3.10.12, Java 21 | du, sha512sum, docker images, this session |
| 2 | `pyspark-client` 11 MB; works from a file or stdin, exits 255 under `python -c` | scratch venv, this session |
| 2 | Docker Connect server with `SPARK_NO_DAEMONIZE=1` answers a 4.3.0 thin client | this session, port 15003 |
| 3 | cleaning counts (17 / 16,815 / 160,940 / 1,013,500 / 1,404,377) | `ch03/output/what_cleaning_drops.txt` |
| 3 | top-10 zones table; analyzed plans differ, optimized plans `sameResult` | `ch03/output/quickstart.txt` |
| 4 | protobuf plan, operation IDs, lazy analysis, `JVM_ATTRIBUTE_NOT_SUPPORTED`, `/connect/operations` record | `ch04/output/connect.txt` |
| 4 | Classic raises `UNRESOLVED_COLUMN` at `select` | scratch probe, this session |

## Open questions and candidate bugs (check with the spark-bug routine before writing them up)

- **Task `bytesRead` for Parquet scans.** In `ch01/anatomy.py` every scan task reported `inputMetrics.bytesRead = 12,349`,
  including the four tasks that decoded a full row group (about 16 MB each). Looks like only the footer read is counted.
  Not yet reproduced against 4.2 or master. The chapter doesn't quote bytes read until this is understood.
- **`pyspark-client` under `python -c`.** `pyspark/sql/connect/utils.py:check_dependencies` treats a `__main__` with no
  `__spec__`, no `__file__` and no `ps1` as a doctest run, imports `pyspark.testing.connectutils`, whose `sqlutils`
  calls `_find_spark_home()` at import time and `sys.exit(-1)`s in the thin client. Check 4.2 and master; the chapter
  documents it as a gotcha.
- **Docker image Python vs PyPI.** `apache/spark:4.3.0-python3` ships Python 3.10.12, while `pyspark==4.3.0` on PyPI
  requires >= 3.11. Probably intentional (the image predates the bump, or keeps Ubuntu 22.04's Python); confirm before
  calling it anything more than a mismatch to be aware of.
