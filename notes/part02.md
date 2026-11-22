# Part II notes: DataFrames and SQL (chapters 5 to 10)

File names use the old numbers (5 to 10 are unchanged by `notes/renumber.md`). Cross-references to other parts use the final numbers of `book/roadmap.md` (Chapter 18, 20, 25, 30, 41).

## Technical names and technical verbs

STE technical names used in Part II (nouns or modifiers, in code format where they are identifiers): DataFrame, Column, schema, plan, logical plan, parsed plan, analyzed plan, optimized plan, physical plan, analyzer, optimizer, planner, Catalyst, Project, Filter, Exchange, Window, WindowGroupLimit, Sort, SortAggregate, HashAggregate, Expand, Generate, SortMergeJoin, BroadcastHashJoin, ShuffledHashJoin, BroadcastNestedLoopJoin, CartesianProduct, SortMergeAsOfJoin, UnionLoop, AQE, partition, shuffle, task, stage, job, driver, executor, session, view, temp view, row group, footer, struct, array, map, VARIANT, decimal, double, timestamp, interval, collation, collation key, nullability, null, NaN, ANSI mode, SQLSTATE, error class, error condition, UDF, SQL UDF, table function, cursor, handler, condition handler, variable, session variable, SQL path, CTE, recursive CTE, anchor, window function, window specification, frame, peer, ranking function, running total, moving window, tumbling window, sliding window, session window, pipe operator, hint, skew, salt, bucket, bucketing, WKB, SRID, GEOMETRY, GEOGRAPHY, great circle, haversine formula, grid, cell, service area, null island, bucket origin, constant folding, parameter marker, script, compound statement.

STE technical verbs: run, read, write, collect, cast, parse, analyze, optimize, plan, shuffle, broadcast, join, aggregate, partition, filter, project, cache, spill, commit, infer, resolve, collapse, inline, register, declare, fetch, fold, materialize, explode, salt, bucket, validate.

Words allowlisted on purpose for `scripts/ste-check` (all are technical names or modifiers, never verbs): running (running total), ranking (ranking function), moving (moving window), folding (constant folding), grouping (grouping column), warning (the noun), scripting (SQL scripting), streaming (Structured Streaming), sliding, tumbling (window kinds), docstring, trailing, leading (spaces), spelling, cleaning, encoding, mapping (mapping table), floating (floating point), breaking (non-breaking space), combining (Unicode combining accent).

## STE checker results

`scripts/ste-check <file> description --allow <words above>`:

| Chapter | Findings | Notes |
|---|---|---|
| ch05 | 4 | `Learning` is the book title *Learning Spark*. `missing` is a code literal in the drift output (`'missing'`) quoted in prose. `docstring` twice is a TN. |
| ch06 | 0 | |
| ch07 | 0 | |
| ch08 | 0 | |
| ch09 | see below | |
| ch10 | 0 | |

Rules broken on purpose:

- Step headings keep the names of SQL clauses and functions (`QUALIFY`, `EXECUTE IMMEDIATE`, `ASOF JOIN`).
- Some output descriptions quote SQL keywords that are English words (`LIMIT`, `FOR`, `INTO`). They are code, in code format.
- "Spark" is the subject of many sentences that describe what the engine does. STE allows a product name as a TN.

## Coverage

### LS02 dataframes_schemas, MD03 schemas: chapter 5

Written by the earlier writer (commit `0a6f0b1`), not changed here except the checker pass. The chapter covers: DataFrame as schema + plan (Step 1), everyday methods (Step 2), column references (Step 3), `withColumn` chains and their analyzed plans (Steps 4, 5), the type table, numbers, decimals and the two timestamp types (Steps 6 to 8), DDL / `StructType` / JSON contracts (Step 9), nested paths (Step 10), nullability (Step 11), inference and `samplingRatio` (Step 12), schema contracts (Step 13), drift detection (Step 14), `union` / `unionByName` / `DataFrame.to` (Step 15), awkward names (Step 16), Python data in and out (Step 17), `DataFrame.zip` (Step 18). VARIANT and collations are previewed in Step 2 and Chapter 6.

### MD04 cleaning_values, MD21 (collations, try_, TIME): chapter 6

Written mostly by the earlier writer. Finished here: Step 9 (collation cost re-measured, with the plan reason), STE pass. Covered: ANSI errors and where they occur (Steps 1 to 3), the try_ family (4), what a cast accepts (5), whitespace and case (6), regex, LIKE / ILIKE / RLIKE, translate and accents (7), collations including bytes, RTRIM and language collations (8), collation limits and cost (9), invalid UTF-8 (10), money (11), dates (12), timestamps with offsets (13), NULL / empty / markers / NaN (14), mapping tables and reject reasons (15), nanosecond timestamps (16, new in 4.3).

### LS05 spark_sql_deep_dive, WN03 pipe_syntax_qualify, MD21 (exploring SQL): chapter 7

| Topic | Where |
|---|---|
| Catalyst phases, parse / analyze / optimize / physical | Step 2 (parsed trees, mermaid), Step 4 |
| one query in DataFrame, SQL and pipe syntax, plan equality | Step 1 |
| explain modes (simple, extended, formatted, cost, codegen) | Step 4 |
| constant folding, `1/0` under ANSI | Step 4 |
| CBO, `ANALYZE TABLE`, statistics | Step 4 (described, with a pointer to Chapter 18; not run) |
| parameterized SQL: named, positional, `IDENTIFIER`, DataFrame `{t}` | Step 5 |
| pipe operators (WHERE, SELECT, EXTEND, SET, DROP, AS, AGGREGATE, JOIN, ORDER BY, LIMIT, set operations, PIVOT, UNPIVOT, TABLESAMPLE) | Steps 1, 3, 16, 17 (table) |
| every prefix is a query | Step 3 |
| `|` single-character pipe (4.2) | Step 17 |
| aggregates in pipe SELECT (4.2), AGGREGATE column order | Step 17 |
| pipes in CTEs and after queries | Step 17 |
| window functions taught from the start (partition, order, frame) | Steps 6 to 12 |
| ranking functions and ties | Step 7 |
| lag / lead | Step 8 |
| default frame trap, ROWS vs RANGE, last_value | Steps 9, 10 |
| named WINDOW clause, window errors | Step 11 |
| window physical plan, shared specs, no PARTITION BY | Step 12 |
| QUALIFY (4.2), rules, errors, ties | Step 13 |
| WindowGroupLimit (top-N optimization) | Step 14 |
| `max_by` with k (4.2) | Step 15 |
| `listagg`, `approx_top_k` | Step 18 |
| session variables, `EXECUTE IMMEDIATE`, scripting | Chapter 8 |
| `time_bucket` | Chapter 10 |
| collations, `SHOW COLLATIONS` | Chapter 6 |
| UDFs (Arrow, pandas, arrow_udf), DataFrames vs RDDs | left out: Part IV (Chapter 20) owns Python UDFs. SQL UDF vs Python UDF plan comparison is in Chapter 8 Step 9. |
| AQE | left out here: Chapter 18 owns the one AQE explanation; Chapter 9 Step 6 shows the join rewrite. |

### WN04 sql_scripting: chapter 8

| Topic | Where |
|---|---|
| session variables (`DECLARE OR REPLACE VARIABLE`, `SET VAR`, `session.` names, column precedence) | Step 1 |
| BEGIN ... END, DECLARE, SET, tuple SET, parameter markers, result of the last statement | Step 2 |
| IF / ELSEIF / ELSE, CASE statement, WHILE, REPEAT, LOOP, LEAVE, ITERATE, labels | Steps 2, 3 |
| FOR over a query | Step 4 |
| how a script runs (driver interpreter, one job per statement) | Step 5 (measured: 20 SQL executions, 27 jobs) |
| EXIT handlers, condition / SQLSTATE / SQLEXCEPTION / NOT FOUND, specificity | Step 6 |
| DECLARE CONDITION | Step 6 (open finding 1) |
| no transaction | Step 6 |
| CONTINUE handlers (flag), cursors (flag), CURSOR_NO_MORE_ROWS, infinite loop without NOT FOUND | Step 7 |
| no SIGNAL / no SELECT INTO | the 4.3.0 grammar has no SIGNAL statement (checked in `SqlBaseParser.g4`); `EXECUTE IMMEDIATE ... INTO` is shown in Step 8. Not stated in the chapter. |
| EXECUTE IMMEDIATE, USING, INTO | Step 8 |
| SQL UDFs, scalar and table, inlining proved in plans | Steps 9, 10 |
| SQL UDF errors (built-in name, non-constant LIMIT, IDENTIFIER) | Step 11 |
| persistent functions, DESCRIBE FUNCTION | Step 12 |
| SQL path, `current_path()`, `SET PATH`, `DEFAULT_PATH`, `spark.sql.path.enabled` | Step 13 |
| recursive CTEs, UnionLoop, limits, MAX RECURSION LEVEL | Steps 14, 15 |

### LS07 join_strategies, WN08 (NEAREST BY), MD21 (NEAREST BY), ASOF JOIN (4.3): chapter 9

See the chapter section list; filled in after the chapter (below).

### WN05 geospatial, MD21 (geo half), WN08 (time_bucket): chapter 10

| Topic | Where |
|---|---|
| `time_bucket` (4.2), origin, vs `window()` and `date_trunc` | Steps 1, 2, 4 |
| TIMESTAMP vs TIMESTAMP_NTZ zone semantics | Step 3 |
| gap filling, moving averages | Step 5 |
| session windows | Step 6 |
| percentiles per bucket | Step 7 |
| nanosecond timestamps with time_bucket | Step 7 (fails in 4.3.0) |
| WKB layout, 21-byte point, byte order | Step 8 |
| GEOMETRY / GEOGRAPHY, SRID 4326 / 0 / 3857, st_setsrid, casts | Steps 8, 9 |
| WKB_PARSE_ERROR, GEOGRAPHY accepts NaN, GEOMETRY accepts lat 123 | Step 10 |
| try_cast surprises (NaN, Infinity, 1.5d, spaces) | Step 10 |
| validation ladder with service areas | Step 10 |
| building points: struct.pack UDF and big-endian bits with reflect | Step 11 (SQL-only bytes, little-endian) |
| five ST functions in 4.3.0, no ST_Distance | Step 12 |
| haversine from built-ins | Step 12 |
| planar vs spherical | Steps 9, 12 |
| nearest store: all pairs, grid, NEAREST BY | Step 13 |
| Parquet logical type, GEOMETRY(ANY) not storable | Step 14 |
| no ordering, GROUP BY crash, no cache | Step 15 (open finding 3) |
| point-in-polygon ray casting UDF | left out: no ST_Contains in 4.3.0; the chapter keeps to points and distance. A ray-casting UDF is a Python UDF topic (Part IV). |
| Apache Sedona | named, version for 4.3 not verified |

## Claim-to-source

| Claim | Source |
|---|---|
| ch06: collation grouping 0.35 / 9.54 / 17.74 / 13.47 s | `code/ch06/output/collation_cost.txt` (20,000,000 cached rows, median of 5 after a warmup, locked) |
| ch06: non-binary collations use SortAggregate | `code/ch06/output/collation_plan.txt`; `Aggregate.supportsHashAggregate` in `basicLogicalOperators.scala` (v4.3.0) |
| ch07: three syntaxes, same optimized plan | `code/ch07/output/syntaxes.txt` |
| ch07: 17 rows removed by the date filter, 174,040 by fare/distance | `syntaxes.txt` step 3 (3,837,248 to 3,837,231 to 3,663,191) |
| ch07: cost estimate 35.5 GiB | `syntaxes.txt` step 4 |
| ch07: constant folding fails in `ConstantFolding.tryFold` | `syntaxes.txt` step 5 |
| ch07: three window specs give 3 Window, 3 Sort, 2 Exchange | `windows.txt` step 8 |
| ch07: "No Partition Defined for Window operation" warning | `WindowExecBase.scala` line 38 (v4.3.0) |
| ch07: median pickup gap 7 to 11 s | `windows.txt` step 10 |
| ch07: QUALIFY in 4.2 | `qualifyClause` absent from the v4.1.0 grammar, present in v4.2.0 |
| ch07: WindowGroupLimit rule conditions, threshold 1000 | `InferWindowGroupLimit.scala`, `SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD` (v4.3.0) |
| ch07: Partial and Final WindowGroupLimitExec | `WindowGroupLimitExec.scala` (v4.3.0); `qualify.txt` step 3 counts 2 |
| ch07: `max_by` k in 4.2 | PySpark 4.3.0 docstring `versionchanged:: 4.2.0` |
| ch07: `approx_top_k` since 4.1 | `ApproxTopKAggregates.scala` `since = "4.1.0"` |
| ch07: `|` pipe config, default true, 4.2 | `SQLConf.scala` `spark.sql.parser.singleCharacterPipeOperator.enabled` |
| ch07: tip column only card tips | TLC yellow trip data dictionary (tip_amount: "Cash tips are not included") |
| ch08: scripting on by default | `SQLConf.scala` `spark.sql.scripting.enabled` default true |
| ch08: 20 SQL executions, 27 jobs, 2 jobs for one query | `code/ch08/output/scripting.txt` step 4 |
| ch08: handler search order | `SqlScriptingExecutionScope.findHandler` in `SqlScriptingExecutionContext.scala` |
| ch08: declared condition not matched by SQLSTATE | `scripting.txt` step 5; `SqlScriptingInterpreter.scala` lines 98-104 |
| ch08: unhandled 02 condition continues | `SqlScriptingExecution.scala` lines 257-269 |
| ch08: CONTINUE handlers and cursors behind internal flags, default false | `SQLConf.scala` |
| ch08: SQL UDF body in the optimized plan | `functions.txt` step 1 |
| ch08: SQL UDF vs Python UDF timing | `functions.txt` step 2 (median of 3 after a warmup, locked) |
| ch08: SET PATH needs `spark.sql.path.enabled` (4.2, default false) | `functions.txt` step 6; `SQLConf.scala` |
| ch08: recursive CTE limits 100 levels / 1,000,000 rows | `SQLConf.scala` `spark.sql.cteRecursionLevelLimit`, `cteRecursionRowLimit` |
| ch08: UnionLoopExec runs one pass at a time, local relation for small results | `UnionLoopExec.scala` |
| ch08: column wins over a variable | `docs/sql-ref-syntax-ddl-declare-variable.md` (v4.3.0) |
| ch10: time_bucket rules for NTZ and LTZ | `TimeBucketExpressionBuilder` usage text in `datetimeExpressions.scala` (v4.3.0) |
| ch10: geospatial default true from 4.2, flag since 4.1 | `SQLConf.scala` v4.1.0 (default `Utils.isTesting`) and v4.2.0 (default true) |
| ch10: five ST functions in 4.2.0 and 4.3.0 | `FunctionRegistry.scala` in both tags; `geo.txt` step 7 |
| ch10: all other numbers | `code/ch10/output/timeseries.txt`, `code/ch10/output/geo.txt` |

## Environment

- Linux 7.0.0, 16 cores, `local[*]` unless a script says otherwise (`ch05/drift.py` uses `local[4]`).
- OpenJDK 17.0.21.1, Python 3.12.3, `pyspark[connect]==4.3.0`, pyarrow and pandas from `code/uv.lock`. No external packages in Part II.
- Ports: `SPARK_UI_PORT=4022` for the book runs, 4122 for films and side runs. (Chapter 7 outputs ran on 4023 by mistake, which is part03's port; no output depends on the port.)
- Data: the June 2026 yellow-taxi file and zone lookup from Part I; SparkTutor messy data copied to `code/data/events`, `code/data/cleaning`, `code/data/geo_deliveries`.
- Timings: only the timings that the text quotes ran under the bench lock. The Chapter 8 and 9 scripts take the lock themselves, only around each timing loop (`bench_lock()` with `fcntl.flock` on `~/spark-masterclass/data/.bench.lock`). `BOOK_NO_LOCK=1` skips it for films. Medians of 3 after one warmup unless the output says otherwise. Each timed run builds a new DataFrame, because a second `collect()` on the same DataFrame reuses its shuffle files (found while fixing `ch06/collation_cost.py`; the first version measured 0.02 s for every collation).

## Open findings

1. **SQL scripting: a handler for a declared condition never fires for a real error.** `DECLARE bad_cast CONDITION FOR SQLSTATE '22018'` and `DECLARE EXIT HANDLER FOR bad_cast` do not catch `CAST_INVALID_INPUT` (SQLSTATE 22018). A handler `FOR SQLSTATE '22018'` does. `SqlScriptingInterpreter` puts the handler in `conditionToExceptionHandlerMap` under `BAD_CAST`, and `findHandler` compares that key with the error condition name only; the SQLSTATE of the declared condition (kept in `CompoundBody.conditions`) is never used at run time. With no SIGNAL statement in 4.3.0, such a handler cannot fire. Repro:
   ```sql
   BEGIN DECLARE r STRING; DECLARE bad_cast CONDITION FOR SQLSTATE '22018';
     BEGIN DECLARE EXIT HANDLER FOR bad_cast SET r = 'caught'; SELECT CAST('n/a' AS INT); END;
     SELECT r; END
   ```
   Expected `caught`; 4.3.0 raises `CAST_INVALID_INPUT`. Not checked against master.
2. **PySpark: `SqlScriptingException` and some `SparkException`s are not converted.** `UNSUPPORTED_FEATURE.CONTINUE_EXCEPTION_HANDLER`, `UNSUPPORTED_FEATURE.SQL_CURSOR`, `RECURSION_LEVEL_LIMIT_EXCEEDED` and `ROW_SUBQUERY_TOO_MANY_ROWS` reach Python as `Py4JJavaError`, so `getCondition()` is not available. Repro: `spark.sql("BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SELECT 1; SELECT 1/0; END")` on Classic 4.3.0.
3. **GEOGRAPHY: `GROUP BY` passes analysis and fails in the tasks with `scala.MatchError: ...BinaryView`.** `ORDER BY` and `=` fail cleanly in the analyzer with `INVALID_ORDERING_TYPE`. Repro: `spark.sql("SELECT st_geogfromwkb(X'0101000000000000000000F03F0000000000000040') g").groupBy("g").count().collect()`. The 4.2 inventory noted the same crash; still present in 4.3.0.
4. **ASOF JOIN (4.3) rescans the right group for each left row.** `SortMergeAsOfJoinScanner.findBestBackwardForward` calls `rightGroupBuffer.generateIterator()` from the start for each left row, so the cost per equi-key group is O(L x R). The `spark.sql.join.sortMergeAsOfJoin.enabled` doc says "O(N log N) ... scanning in a single pass". Measured in `code/ch09/output/asof.txt` step 5b. Repro: the step 5b query.
5. **GEOGRAPHY and the in-memory cache**: `cache()` fails with `not support type: GeographyType(4326)` (`_LEGACY_ERROR_TEMP_2100` in the 4.2 inventory). Still present in 4.3.0. Doc gap more than a bug.

## Clips

Tapes: `code/film/ch05.tape` to `ch10.tape`, rendered with `film render code/film/chNN.tape --format chapter` (no bench lock, `BOOK_NO_LOCK=1`, Spark UI on 4122). Each clip is the last 16 seconds of its beat (the command line stays on screen while the script runs), cut by ffmpeg; the cut list is `book/media/clips/clips-part02.tsv`. Timings that appear on screen in a film are not quoted in the text.

| Clip | Beat | Draft VO |
|---|---|---|
| ch05-drift | drift.py step 5 | Each day of events is compared with the contract. Day 3 renamed a field and changed the type of amount. |
| ch05-zones | type_system.py step 3 | The same timestamp text is two instants in two zones. The NTZ text is one wall-clock time. |
| ch06-ansi | ansi.py steps 1-2 | One zero fare stops the average. Each error has its own condition, and ANSI off gives wrong numbers. |
| ch06-nanos | nanos.py step 2 | New in 4.3: two events one nanosecond apart stay different. |
| ch07-frames | windows.py step 4 | The default frame is a range, so the two rows with 30 trips both get 90. |
| ch07-topn | qualify.py steps 3-4 | Upper limits on rank functions get a WindowGroupLimit. QUALIFY needs a window function. |
| ch08-loop | scripting.py steps 4-5 | One script with five passes is twenty queries. Handlers catch errors by name and SQLSTATE. |
| ch08-udf | functions.py steps 1-2 | The SQL function becomes a CASE WHEN in the plan. The Python function adds ArrowEvalPython. |
| ch09-types | strategies.py steps 1-2 | Join types on a tiny table, then trips by zones: a broadcast hash join. |
| ch09-asof | asof.py step 2 | New in 4.3: ASOF JOIN finds the last drop-off before each pickup. |
| ch10-gaps | timeseries.py step 5 | A quiet zone has three rows for 24 hours. The fill makes the rest. |
| ch10-nearest | geo.py step 8 | All pairs and NEAREST BY compare every pair. The grid compares fewer, and misses four. |

## Nav entries

```yaml
      - "DataFrames, columns and schemas": chapters/ch05-dataframes-columns-schemas.md
      - "Cleaning values: ANSI, try_ functions, strings and time": chapters/ch06-cleaning-values.md
      - "Spark SQL in depth: windows, pipe syntax and QUALIFY": chapters/ch07-spark-sql-in-depth.md
      - "Procedural SQL: scripting, SQL UDFs and recursive CTEs": chapters/ch08-procedural-sql.md
      - "Joins: strategies, skew and the time-aware joins": chapters/ch09-joins.md
      - "Time series and geospatial data": chapters/ch10-time-series-geospatial.md
```
