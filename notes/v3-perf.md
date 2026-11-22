# v3-perf notes: performance slice (ch86 new, ch33 and ch36 extended)

Branch `v3-perf`, worktree `~/spark-masterclass-parts/perf`. Ports: Spark UI 4154, Connect 15114 (the in-process Connect plugin of `ch33/ui_tabs.py`).

## Scope

Gap-audit rows covered: "Leveraging statistics / CBO", "Partitioning hints", "Storage-partitioned join", "Bucketing", "Spark tuning guide" (serialization, memory, GC, data structures, locality), "Web UI" (tab by tab), "ORC", Parquet columnar encryption and datetime rebase (items 4, 11, 13, 22, 23 of the recommendations), and the `lz4raw` line (removed in 4.0).

### New chapter

- `book/chapters/ch86-statistics-cbo-pruning.md`: "Statistics, the cost-based optimizer and partition pruning". Provisional number 86. Goes in Part VII (Performance) directly after Chapter 32 (query plans). Code `code/ch86/`, figures `book/media/figures/ch86-*`, animation `book/media/clips/ch86-anim-dpp.*`, film tape `code/film/ch86.tape`.
  - Steps: 1 what the optimizer knows without statistics, 2 ANALYZE TABLE (all forms), 3 how statistics reach the plan (`explain("cost")`, CBO off/on, histograms, estimates against real rows), 4 the estimate selects the join strategy, 5 join reorder (and a histogram case where CBO keeps the written order), 6 dynamic partition pruning (plan, measured, the selective-predicate rule, broadcast reuse, static pruning, new in 4.3 materialized filtering side), 7 bucketed tables (layout, joins, coalesce, aggregates, pruning and `autoBucketedScan`), 8 storage-partitioned joins on Iceberg, 9 join and partitioning hints, whole chapter.

### Changes to existing chapters

- **ch33** (`ch33-partitions-caching-memory.md`): Step 9 renamed "The Spark UI, tab by tab" and extended with a subsection "The tabs, one by one" (Jobs, Stages, Storage, Environment, Executors, SQL / DataFrame, Structured Streaming, Connect; one screenshot each, a table of tabs, URLs and REST endpoints). The old two clips and their text stay. The last paragraph of the old Step 9 ("The other tabs complete the picture...") was replaced by the new subsection. New Steps 10 (serialization: Java and Kryo, registration), 11 (object size with `SizeEstimator`, GC and memory tuning, GC logs), 12 (data locality and `spark.locality.wait`, measured on `local-cluster`), 13 (hardware provisioning from the docs). The intro paragraph, "The whole chapter, on one page" and "Further reading" were extended. Steps 1 to 8 are unchanged.
- **ch36** (`ch36-parquet-inside.md`): title changed to **"Parquet and ORC from the inside"**; opening paragraph and thesis mention ORC, encryption and calendars. New Steps 9 (ORC file structure with a real dump), 10 (native and hive ORC readers), 11 (Parquet against ORC: pushdown and vectorized reads on the taxi data), 12 (Parquet modular encryption), 13 (datetime rebase modes). The old Step 9 "Rules for a Parquet layout" is now Step 14 "Rules for a Parquet or ORC layout", with three more rules. One line added in Step 7 about `lz4raw`. No other chapter refers to "Chapter 36, Step 9" (checked with grep).

## Nav and roadmap rows for the integrator

`mkdocs.yml` (Part VII):

```yaml
      - "32. Reading query plans: Catalyst and AQE": chapters/ch32-query-plans.md
      - "86. Statistics, the cost-based optimizer and partition pruning": chapters/ch86-statistics-cbo-pruning.md   # new, after ch32
      - "33. Partitions, caching, memory and spill": chapters/ch33-partitions-caching-memory.md
      ...
      - "36. Parquet and ORC from the inside": chapters/ch36-parquet-inside.md     # title change
```

`book/roadmap.md`, Part VII:

| # | Chapter | Covers |
|---|---|---|
| 86 (after 32) | Statistics, the cost-based optimizer and partition pruning | ANALYZE TABLE and histograms, `explain("cost")`, CBO join selection and join reorder, dynamic partition pruning measured, bucketed tables, storage-partitioned joins on Iceberg, join and partitioning hints. **4.3:** DPP for a materialized filtering side |
| 33 | Partitions, caching, memory and spill | add: the Spark UI tab by tab with screenshots, Java and Kryo serialization, object size and GC, `spark.locality.wait` measured, hardware provisioning |
| 36 | Parquet and ORC from the inside | add: ORC stripes, row index and postscript from a real file, native and hive readers, Parquet against ORC on the taxi data, modular encryption, datetime rebase modes |

`book/coverage.md`: no SparkTutor lesson maps to the new topics beyond what ch32/ch33/ch36 already list.

<<REST>>
