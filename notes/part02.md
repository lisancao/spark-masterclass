# Part II notes: DataFrames and SQL (chapters 5 to 10)

## Technical names and technical verbs

STE technical names used in Part II (nouns or modifiers, in code format where they are identifiers): DataFrame, Column, schema, plan, logical plan, analyzed plan, optimized plan, physical plan, analyzer, optimizer, Catalyst, Project, Filter, Exchange, Window, Sort, SortMergeJoin, BroadcastHashJoin, ShuffledHashJoin, BroadcastNestedLoopJoin, CartesianProduct, AQE, partition, shuffle, task, stage, job, driver, executor, session, view, temp view, row group, footer, struct, array, map, VARIANT, decimal, double, timestamp, interval, collation, nullability, null, NaN, ANSI mode, SQLSTATE, error class, UDF, SQL UDF, cursor, handler, variable, path, CTE, window function, frame, pipe operator, hint, skew, salt, bucket, WKB, SRID, GEOMETRY, GEOGRAPHY, bin, bucket origin.

STE technical verbs: run, read, write, collect, cast, parse, analyze, optimize, plan, shuffle, broadcast, join, aggregate, partition, filter, project, cache, spill, commit, infer, resolve, collapse, inline, register, declare, fetch.

(The list grows per chapter; see each chapter section below.)

## Coverage

See the per-chapter tables below.

## Claim-to-source

See the per-chapter tables below.

## Environment

- Linux 7.0.0, 16 cores, `local[*]` unless a script says otherwise (`ch05/drift.py` uses `local[4]` so the `samplingRatio` sample is drawn over four partitions)
- OpenJDK 17.0.21.1, Python 3.12.3, `pyspark[connect]==4.3.0`, pyarrow and pandas from `code/uv.lock`
- `SPARK_UI_PORT=4052` (helper subprocesses use 4053), Connect port 15052
- Data: the June 2026 yellow-taxi file and zone lookup from Part I; SparkTutor messy data copied to `code/data/events`, `code/data/cleaning`, `code/data/geo_deliveries`
- Timings: wrapped in `flock ~/spark-masterclass/data/.bench.lock`, medians after a warmup, as stated per measurement

## Open findings

## Nav entries
