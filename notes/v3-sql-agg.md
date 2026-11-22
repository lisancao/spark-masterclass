# v3-sql-agg notes: Chapter 83 (Aggregation), UNNEST in Chapter 28, UNNEST in pipes and TABLESAMPLE in Chapter 13

Branch `v3-sql-agg`, worktree `~/spark-masterclass-parts/sql-agg`. Ports: `SPARK_UI_PORT=4152` (some runs used 4153 to 4156 when another run held 4152), Connect 15112 (not used: every script runs on Spark Classic).

## Scope and changes

- **New:** `book/chapters/ch83-aggregation.md`, "Aggregation: GROUP BY, grouping sets, ROLLUP, CUBE and pivots" (provisional number 83, for Part IV after Chapter 13). Code `code/ch83/` (`aggbook.py` helper, `groupby.py`, `sets.py`, `pivots.py`, `approx.py`, `bench.py`, `plan42.py`), outputs `code/ch83/output/`, film tape `code/film/ch83.tape`, figures `code/figures/ch83.py` -> `book/media/figures/ch83-*.svg`, animation `code/ch83/anim/expand/` (renders ignored).
- **Changed, Chapter 28:** new `## Step 10: New in 4.3: the UNNEST clause` after Step 9 (generators). Old Steps 10 to 21 are now 11 to 22. Two in-text references fixed: "Step 13 uses this rule" -> "Step 17" and "`transform` (Step 9)" -> "(Step 12)". Both pointed to the wrong step before this change too (they were stale from an earlier renumber). Intro line, script list and the closing bullet list got one sentence each. New code `code/ch28/unnest.py`, `code/ch28/unnest_bench.py`, outputs `unnest.txt`, `unnest_bench.txt`, figure `code/figures/ch28_unnest.py` -> `ch28-unnest-parallel.svg`, clip `ch28-unnest`, new beat `ch28-unnest` at the end of `code/film/ch28.tape`.
- **Changed, Chapter 13:** new `## Step 19: UNNEST in pipe syntax` and `## Step 20: TABLESAMPLE, sample and sampleBy, and what they read`, before "The whole chapter, on one page". Steps 1 to 18 keep their numbers. Script list, closing bullets and further reading extended. One existing sentence in Step 15 split (29 words, an STE length finding). New code `code/ch13/sampling.py`, output `sampling.txt`, figure `code/figures/ch13_sampling.py` -> `ch13-tablesample.svg`.
- The gap audit put TABLESAMPLE under Chapter 59 Step 7. The assignment for this slice put it in Chapter 13, so Chapter 59 is unchanged. The audit row for Chapter 83 also lists "set operations in depth"; the assignment does not, so set operations are not in this slice.

## Technical names (TN) and technical verbs (TV)

- TN: `GROUP BY`, `GROUPING SETS`, `ROLLUP`, `CUBE`, `PIVOT`, `UNPIVOT`, `UNNEST`, `WITH ORDINALITY`, `LATERAL`, `TABLESAMPLE`, `REPEATABLE`, `BERNOULLI`, `SYSTEM`, grouping set, grouping key, grouping column, grouping ID, aggregation buffer, partial aggregate, final aggregate, `Complete` mode, `HashAggregate`, `ObjectHashAggregate`, `SortAggregate`, `Expand`, `Generate`, `Exchange`, `PivotFirst`, `TypedImperativeAggregate`, sort fallback, HyperLogLog++, KLL, sketch, ordinality, stratified sample, Poisson sampler, whole-stage code, SQL metric, row group.
- TV: aggregate, group, shuffle, pivot, unpivot, unnest, explode, sample, cache, broadcast, join, partition, sort, parse, plan, push (a filter), fall back.
- `scripts/ste-check book/chapters/ch83-aggregation.md --allow grouping`: **0 findings**.
- `scripts/ste-check book/chapters/ch28-complex-types.md --allow $(cat notes/.ste-allow-part03) grouping`: **0 findings** (the Part III list covers `missing` and `tracing` in the old text).
- `scripts/ste-check book/chapters/ch13-spark-sql-in-depth.md --allow running ranking moving folding grouping warning scripting streaming sliding tumbling docstring trailing leading sampling`: **0 findings** (the Part II list plus `sampling`, which is in the file names `sampling.py` and `sampling.txt`).
- Deliberate rule breaks, as in the rest of the book: "Thus" starts some sentences, parentheses hold JIRA numbers and config defaults, admonition titles use the STE caution form, and the bold thesis of Chapter 83 is four sentences.

## Claim-to-source

| Claim | Source |
|---|---|
| `june` view: 3,663,191 rows, 3,837,248 trips in the file, 16 scan tasks | `ch83/output/groupby.txt` §1 |
| Two phases: 14 rows to the shuffle, 5 result rows | `groupby.txt` §1 (SQL metrics from `/api/v1/.../sql?details=true`) |
| Operator per aggregate (seven cases, and `useObjectHashAggregateExec=false`) | `groupby.txt` §2; rules in `AggUtils.createAggregate`, `Aggregate.supportsHashAggregate`, `supportsObjectHashAggregate` (`basicLogicalOperators.scala`) at v4.3.0 |
| ObjectHashAggregate fallback threshold 128, 4 fallback tasks for PULocationID | `SQLConf.scala` v4.3.0 (`spark.sql.objectHashAggregate.sortBased.fallbackThreshold`, default 128); `groupby.txt` §2b |
| `CombineAdjacentAggregation` new in 4.3.0, default true, internal | `SQLConf.scala` v4.3.0 `.version("4.3.0")`; SPARK-43317 and SPARK-58210 not in `git log v4.2.0`; `CombineAdjacentAggregation.scala` |
| `ReplaceHashWithSortAgg` since 3.3.0, default false in 4.2.0, true in 4.3.0 (SPARK-58210) | `SQLConf.scala` at v4.2.0 (`createWithDefault(false)`) and v4.3.0 (true); commit 00f3be78f2e |
| Plan with both rules off equals the 4.2.0 plan | `groupby.txt` §3; `ch83/output/plan_4.2.0.txt` (`plan42.py` run with pyspark 4.2.0 in a scratch venv) |
| SortAggregate codegen with keys new in 4.3.0 (SPARK-32750), only for mutable buffers | `SQLConf.scala` v4.3.0 `spark.sql.codegen.aggregate.sortAggregate.withKeys.enabled` version 4.3.0; `SortAggregateExec.supportCodegen`, `AggregateCodegenSupport.supportCodegen`; `groupby.txt` §3b |
| `bypassPartialAggregation` new in 4.3.0, default false, session config, not for DISTINCT, needs grouping keys and no session window | `SQLConf.scala` v4.3.0 lines 4323-4333; `AggUtils.planAggregateWithoutDistinct`; SPARK-57688 not in v4.2.0 |
| 1,763,643 distinct pickup times; partial out 2,228,358 (39% removed) | `groupby.txt` §4; `bench.txt` |
| Bypass timings 1.248 / 0.744 / 0.251 / 0.794 s | `bench.txt` (bench lock, median of 5 after warmup) |
| FILTER in the partial aggregate only | `groupby.txt` §5; `AggUtils.mayRemoveAggFilters` |
| DISTINCT plans, Expand 7,326,382 and 3,663,191 rows | `groupby.txt` §6; `RewriteDistinctAggregates.mayNeedtoRewrite`, `mustRewrite`; `AggUtils.planAggregateWithOneDistinct` |
| ROLLUP results, Expand projections, 8,216,181 rows, partial 30 | `ch83/output/sets.txt` §1-2 |
| CUBE results and bits | `sets.txt` §3 |
| GROUPING SETS vs UNION ALL: 1 vs 3 scans, 3,145,728 vs 9,437,184 rows | `sets.txt` §4 |
| NULL key vs subtotal (901,951 NULL passenger counts) | `sets.txt` §5 |
| Mixed form IDs 0, 1, 3 | `sets.txt` §6 |
| DataFrame rollup/cube/groupingSets same as SQL; groupingSets new in 4.0.0 | `sets.txt` §7; `python/pyspark/sql/dataframe.py` v4.3.0 `versionadded:: 4.0.0` |
| GROUPING SETS (()) on empty input returns one row since 4.2.0 (SPARK-57775) | `sets.txt` §8; commit 65d9c4f7ab3, and SPARK-57775 is in `git log v4.2.0` |
| ROLLUP vs UNION ALL timings | `bench.txt` |
| 63 MB file | `du -sh data/nyc-taxi/yellow` = 63M |
| PIVOT plans: fast path with PivotFirst, IF path; PivotFirst types (numbers, boolean, decimal) | `ch83/output/pivots.txt` §2, §4; `PivotTransformer.scala`, `PivotFirst.scala` v4.3.0 |
| Implicit grouping: 3,801 and 714,323 rows | `pivots.txt` §3 |
| `pivot` without values runs 5 jobs at call time; `pivotMaxValues` 10,000 | `pivots.txt` §5; `RelationalGroupedDataset.pivot` (`collectPivotValues`), `SQLConf.scala` |
| `F.count("*")` in pivot fails | `pivots.txt` §5; also on 4.2.0 (open finding 1) |
| UNPIVOT is an Expand plus `Filter isnotnull`; SQL excludes NULLs by default, DataFrame keeps them | `pivots.txt` §6-7; `UnpivotTransformer` |
| Approximate distinct errors, shuffle records 2,448,981 vs 31 | `ch83/output/approx.txt` §1-2 |
| hll_sketch default lgConfigK 12, accepts INT/BIGINT/STRING/BINARY | error and plan text from the first `approx.py` run (`hll_sketch_agg(tpep_pickup_datetime, 12)`); `approx.py` casts to STRING |
| Medians: exact vs approx_100/10000 vs KLL | `approx.txt` §3 |
| Approx timings, 11,484 distinct fares | `bench.txt` |
| UNNEST new in 4.3.0 (SPARK-58419), syntax, NULL array empty, 1-based ordinality, col/col0 names, CodegenFallback | `docs/sql-ref-syntax-qry-select-unnest.md` v4.3.0; `SqlBaseParser.g4` v4.3.0; `generators.scala` `Unnest`; `AstBuilder.visitUnnestTable`; `ch28/output/unnest.txt` |
| LEFT JOIN LATERAL UNNEST decorrelated into Aggregate + Generate + left outer join | `unnest.txt` (optimized plan section) |
| UNNEST bench (two runs differ) | `ch28/output/unnest_bench.txt` (second run, saved); first run 0.518 / 0.832 / 1.911 s from the same script, under the lock, not saved (quoted in the chapter as an earlier run) |
| No pipe UNNEST operator; `|> JOIN LATERAL` works | `operatorPipeRightSide` in `SqlBaseParser.g4` v4.3.0; `ch13/output/sampling.txt` §1 |
| TABLESAMPLE forms, plans, scan rows | `sampling.txt` §2-4; `AstBuilder.withSample`; `SampleExec` |
| TABLESAMPLE SYSTEM since 4.2.0 (SPARK-55978), JDBC (SPARK-57040, also in 4.2.0), error conditions | `git log v4.2.0` contains both; `error-conditions.json` v4.3.0 `TABLESAMPLE_SYSTEM*`; `SparkStrategies.scala` |
| The sampling docs page does not mention SYSTEM, BERNOULLI, REPEATABLE | `docs/sql-ref-syntax-qry-select-sampling.md` v4.3.0 (open finding 3) |
| sample/sampleBy plans and counts | `sampling.txt` §5; `classic/DataFrameStatFunctions.sampleBy` |

## Environment

- Linux 7.0.0, 16 cores, OpenJDK 17, Python 3.12.3, `pyspark==4.3.0` from `code/uv.lock`. Local mode `local[*]`, default 200 shuffle partitions, AQE on. `approx.py` and `bench.py` use `spark.driver.memory=4g`.
- Data: `BOOK_DATA=$HOME/spark-masterclass/data`, June 2026 yellow trips (63 MB) and the zone lookup; Chapter 28 uses `code/data/messy/nested/orders.json`.
- Spark source: `~/repos/spark` tag `v4.3.0` (98bc0212c94). Version checks with `git log v4.2.0 | grep SPARK-...`.
- pyspark 4.2.0 for `plan42.py` and the pivot `*` check: a scratch venv (`uv venv` + `pip install pyspark==4.2.0`) in the scratchpad, not committed.
- Timings: `flock ~/spark-masterclass/data/.bench.lock`, median of 5 after one warmup, cached input where the script says so. Other writers used the machine at the same time. Two locked runs of `bench.py` gave different absolute numbers (for example `count(DISTINCT)` 1.824 s, then 0.925 s); the chapter quotes the saved second run and draws only conclusions that held in both runs (bypass faster for unique keys and slower for few keys, approx_count_distinct faster than count(DISTINCT), exact percentile faster than percentile_approx on fares, ROLLUP and UNION ALL close).

## Open findings

1. **`DataFrame.groupBy(...).pivot(...).agg(F.count("*"))` fails with `INVALID_USAGE_OF_STAR_OR_REGEX`**, while `groupBy(...).agg(F.count("*"))` works and SQL `PIVOT (count(*) ...)` works. Same on 4.2.0. Repro:
   ```python
   df = spark.createDataFrame([("a","x"),("a","y"),("b","x")], "k string, p string")
   df.groupBy("k").pivot("p").agg(F.count("*")).collect()   # INVALID_USAGE_OF_STAR_OR_REGEX
   df.groupBy("k").pivot("p").agg(F.count(F.lit(1))).collect()   # works
   ```
   Possibly intended (the `*` expansion is not done inside `Pivot`), but inconsistent with the groupBy path. Not checked against master or JIRA.
2. **`approx.py`, first version: OOM (`java.lang.OutOfMemoryError: Java heap space` in `UnsafeSorterSpillReader`)** with the default 1g driver in `local[*]`, for one query that mixed `count(DISTINCT tpep_pickup_datetime)` with `approx_count_distinct` and `hll_sketch_agg` grouped by Borough. With one DISTINCT group plus object aggregates, the partial `ObjectHashAggregate` groups by (Borough, pickup time), about 1.7M keys, falls back to sort after 128 keys in each task and spills. The spill reader then ran out of heap. Splitting the query (as the chapter does) and 4g avoids it. Not reduced further; may be expected for 1g with 16 concurrent tasks. Not in the chapter.
3. **Docs gap:** `docs/sql-ref-syntax-qry-select-sampling.md` at v4.3.0 lists only PERCENT, ROWS and BUCKET. The grammar has `TABLESAMPLE [SYSTEM | BERNOULLI] (...) [REPEATABLE (seed)]` since 4.2.0. Candidate for a docs PR.
4. **Docs gap:** `docs/sql-ref-syntax-qry-select-groupby.md` says "a column position like `GROUP BY 0`". Positions are 1-based. Checked with a one-off run on pyspark 4.3.0 (not saved as chapter output): `GROUP BY 0` gives `[GROUP_BY_POS_OUT_OF_RANGE] GROUP BY position 0 is not in select list (valid range is [1, 2])`, and `GROUP BY 1` works.
5. **`LEFT JOIN LATERAL UNNEST(arr) ON true` plans an aggregate over distinct arrays, a generate and a left outer join on the array value**, not a `Generate` with `outer = true`. On 3.8M rows it adds three shuffles. Results are correct. A possible optimizer improvement (rewrite to an outer Generate), not a bug.

## Clips and visuals

| File | What |
|---|---|
| `book/media/clips/ch83-rollup.mp4/.webp` | beat `rollup` of `ch83-chapter.mp4` (0-40 s), jump cut keeps 0-10.3 and 29.2-40.0 |
| `book/media/clips/ch83-combine.mp4/.webp` | beat `combine` (40-82.4 s), jump cut keeps 40-48 and 71.3-82.4 |
| `book/media/clips/ch28-unnest.mp4/.webp` | beat `ch28-unnest`, rendered from `code/film/ch28u.tape` (the Chapter 28 setup plus only this beat, because `film render --only` needs the other beats' clips), jump cut keeps 0-11 and 33.7-41.4 |
| `book/media/clips/ch83-anim-expand.mp4/.webp` | HyperFrames (create-animation kit, paper theme, landscape, 26 s): one row through Expand of ROLLUP, numbers from `sets.txt` via `data.json`. Storyboard `code/ch83/anim/expand/storyboard.md`. The `open` beat is 0.3 s under the read-twice time (minor). Pacing not watched at 1x by a person. |
| `ch83-two-phase.svg`, `ch83-operators.svg`, `ch83-grouping-id.svg` | `code/figures/ch83.py`, checked as PNG on light and dark |
| `ch28-unnest-parallel.svg` | `code/figures/ch28_unnest.py` |
| `ch13-tablesample.svg` | `code/figures/ch13_sampling.py` (unseeded sample counts from the last `sampling.txt`) |

Film: `film render code/film/ch83.tape --format chapter` and `film render code/film/ch28u.tape --format chapter --demo code`, no bench lock. Encoding libx264 crf 28, yuv420p, faststart, no audio; poster 1.5 s before the end. Cut list: `book/media/clips/clips-v3-sql-agg.tsv`. Mermaid diagrams in Chapter 83: 4 (two phases, DISTINCT rewrite, PIVOT plans, Expand against UNION ALL).

## Nav and roadmap rows for the integrator

```yaml
      - "83. Aggregation: GROUP BY, grouping sets, ROLLUP, CUBE and pivots": chapters/ch83-aggregation.md
```

Place it in Part IV directly after Chapter 13.

Roadmap row (Part IV):

| 83 | [Aggregation: GROUP BY, grouping sets, ROLLUP, CUBE and pivots](chapters/ch83-aggregation.md) | Partial and final aggregates, HashAggregate, ObjectHashAggregate and SortAggregate, FILTER, DISTINCT and its Expand, GROUPING SETS, ROLLUP, CUBE, grouping_id, PIVOT and UNPIVOT in SQL and DataFrames, approximate aggregates. **4.3:** CombineAdjacentAggregation, ReplaceHashWithSortAgg on by default, SortAggregate codegen with keys, bypassPartialAggregation |

Roadmap updates for existing rows: Chapter 13 covers add "UNNEST in pipes, TABLESAMPLE, sample and sampleBy"; Chapter 28 covers add "**4.3:** UNNEST, WITH ORDINALITY, LEFT JOIN LATERAL".
