# Chapter 86: Statistics, the cost-based optimizer and partition pruning

[Chapter 32](ch32-query-plans.md) showed that Spark makes most plan decisions before it reads any data. It selects a join strategy, a join order and the files to read from estimates. In [Chapter 11](ch11-joins.md), some estimates were wrong. The result was a broadcast that did not fit, or a shuffle that was not necessary. This chapter is about the inputs to those estimates, and about the layouts that let Spark skip work completely.

**The optimizer knows only what the catalog and the files tell it. Without statistics, it knows the size of each table and nothing about its rows, so a filter does not change any estimate. `ANALYZE TABLE` stores row counts, column statistics and histograms, and the cost-based optimizer uses them to select a broadcast and to reorder joins. Partitioned, bucketed and storage-partitioned tables carry their layout into the plan, and Spark uses that layout to skip partitions and to remove shuffles. Hints are for the cases where you know more than all of these.**

The scripts are in [`code/ch86`](https://github.com/lisancao/spark-masterclass/tree/main/code/ch86), and each output block comes from `code/ch86/output/`. The tables are these:

- `trips`: the June 2026 taxi trips, 3,709,141 rows after the cleaning in `c86.py`.
- `fares`: the money columns of the same trips, with the same `trip_id`.
- `zones`: the 265 taxi zones.
- `trips_by_day`: six months of trips, with one partition for each day.

The tables are in a Hive metastore in Derby under `data/work/`, so that their statistics stay between scripts. All runs use Spark 4.3.0 in `local[*]` mode on 16 cores. Each timing is a median after a warm-up run, measured under the bench lock while no other benchmark of the book ran. Other processes ran on the machine at the same time, so compare timings only inside one output block.

| Script | Steps | What it shows |
|---|---|---|
| `stats.py` | 1, 2, 3 | what the catalog knows, `ANALYZE TABLE`, estimates against real rows |
| `cbo_join.py` | 4, 5 | the estimate selects the join strategy and the join order |
| `dpp.py` | 6 | dynamic partition pruning, measured |
| `bucketing.py` | 7 | bucketed tables, joins and aggregates without a shuffle, bucket pruning |
| `spj.py` | 8 | a storage-partitioned join of two Iceberg tables |
| `hints.py` | 9 | join hints and partitioning hints |

## Step 1: What the optimizer knows without statistics

`stats.py` writes the three tables with `saveAsTable` and asks the catalog what it knows:

```text
=== 1. what the catalog knows before ANALYZE
  trips  Statistics: 49377402 bytes, 0 rows
  fares  Statistics: 32114470 bytes, 0 rows
  zones  Statistics: 5956 bytes, 0 rows
  cost of fares WHERE total_amount > 500:
    Relation: Statistics(sizeInBytes=30.6 MiB)
    Filter:   Statistics(sizeInBytes=30.6 MiB)
```

- **The catalog has the size of the files.** The write recorded the total size of the Parquet files of each table. The "0 rows" is the value that the Hive metastore keeps before the first `ANALYZE`. The plan does not use it: the `Relation` node has no `rowCount`.
- **A filter does not change the estimate.** The filter `total_amount > 500` keeps 82 rows of 3,709,141 (Step 3 counts them). The estimate of the `Filter` node is still 30.6 MiB, the size of the whole table.

Without statistics, Spark estimates each node with the visitor `SizeInBytesOnlyStatsPlanVisitor`. A `Filter`, a `Project` and most other nodes keep the size of their child, or scale it by the width of the columns. Only a `Limit`, an `Aggregate` without keys and a few other nodes can make an estimate smaller. The estimate is a size in bytes, never a count of rows.

This has one large result. The broadcast threshold (`spark.sql.autoBroadcastJoinThreshold`, 10 MB) compares a size estimate with a number. A small result of a selective filter on a large table never looks small to the planner.

## Step 2: ANALYZE TABLE

`ANALYZE TABLE` reads a table and writes statistics into the catalog. It has three forms, and each form stores more:

```text
=== 2. ANALYZE TABLE, three forms
  NOSCAN              -> 32114470 bytes, 0 rows
  COMPUTE STATISTICS  -> 32114470 bytes, 3709141 rows
  FOR COLUMNS         -> 32114470 bytes, 3709141 rows
  DESC EXTENDED fares total_amount:
    data_type       double
    min             -1815.18
    max             7053.5
    num_nulls       0
    distinct_count  18226
    avg_col_len     8
    max_col_len     8
    histogram       NULL
  DESC EXTENDED fares payment_type:
    data_type       bigint
    min             0
    max             5
    num_nulls       0
    distinct_count  6
    avg_col_len     8
    max_col_len     8
    histogram       NULL
```

| Statement | Reads | Stores |
|---|---|---|
| `ANALYZE TABLE t COMPUTE STATISTICS NOSCAN` | the file listing | the size in bytes |
| `ANALYZE TABLE t COMPUTE STATISTICS` | all rows | the size and the row count |
| `ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS a, b` | all rows | the above, and for each column: minimum, maximum, null count, distinct count, average and maximum length |
| `... FOR ALL COLUMNS` | all rows | the same for each column |
| `ANALYZE TABLES IN db COMPUTE STATISTICS` | all rows of each table | the size and the row count of each table in `db` |
| `... FOR COLUMNS` with `spark.sql.statistics.histogram.enabled=true` | all rows | the above, and an equi-height histogram for each numeric, date and timestamp column |

The distinct count is an estimate from HyperLogLog++. `spark.sql.statistics.ndv.maxError` (0.05) sets its relative error. The last statement of `stats.py` analyzes every column of `trips`, and every table of the database:

```text
=== 5. ANALYZE for every column, and for every table in a database
  trips  Statistics: 49377402 bytes, 3709141 rows
  fares  Statistics: 32114470 bytes, 3709141 rows
  zones  Statistics: 5956 bytes, 265 rows
  trips.trip_distance: min 0.01, max 311565.39, num_nulls 0, distinct_count 5036
```

The catalog keeps these values as table properties with the prefix `spark.sql.statistics.`.

Statistics do not follow the data. If you append rows, the row count and the column statistics stay old. With `spark.sql.statistics.size.autoUpdate.enabled=true`, Spark updates the size after each write, but not the other values. Run `ANALYZE` again after large changes.

!!! caution "Statistics that are out of date are worse than none"
    Run `ANALYZE TABLE` again after a large write. The optimizer trusts the stored row count and column ranges. If they are old, its estimates can be wrong by orders of magnitude, and it can broadcast a table that is now large.

## Step 3: How statistics reach the plan

Statistics in the catalog are not enough. The plan uses only the size until you turn on the cost-based optimizer (CBO) with `spark.sql.cbo.enabled`. The same filter, before and after:

```text
=== 3. the same filter, CBO off and on
  spark.sql.cbo.enabled=false
    Relation: Statistics(sizeInBytes=30.6 MiB)
    Filter:   Statistics(sizeInBytes=30.6 MiB)
  spark.sql.cbo.enabled=true
    Relation: Statistics(sizeInBytes=169.8 MiB, rowCount=3.71E+6)
    Filter:   Statistics(sizeInBytes=125.5 MiB, rowCount=2.74E+6)
```

Two things changed:

- **The `Relation` has a row count, and a larger size.** With CBO, the size is the row count multiplied by the size of one row in memory. Spark calculates the row size from `avg_col_len` of each column and a row overhead. Thus 30.6 MiB of compressed Parquet becomes 169.8 MiB. This is closer to what a broadcast must hold in memory ([Chapter 11](ch11-joins.md), Step 5 showed a broadcast that did not fit).
- **The `Filter` has its own estimate.** `FilterEstimation` uses the column statistics. Without a histogram, it assumes that the values spread evenly between the minimum and the maximum. The range of `total_amount` is -1815.18 to 7053.5, and 500 is near the bottom of it. So the estimate is that 74% of the rows pass: 2.74 million rows. The real answer is 82 rows.

A histogram fixes this. `stats.py` estimates six filters without and with a histogram, and counts the real rows:

```text
=== 4. estimated rows against real rows, without and with a histogram
  predicate                          real rows  no histogram   histogram
  total_amount > 500                        82     2,740,000      13,800
  total_amount > 100                    70,952     2,910,000      69,000
  total_amount < 10                     55,427       763,000      55,900
  total_amount BETWEEN 15 AND 25     1,500,703         4,180   1,500,000
  payment_type = 2                     348,204       618,000     358,000
  payment_type = 4                      19,269       618,000      21,900
  histogram of total_amount: height: 14602.917322834646, num_of_bins: 254
    bin_0    lower_bound: -1815.18, upper_bound: 5.9, distinct_count: 1701
    bin_1    lower_bound: 5.9, upper_bound: 8.7, distinct_count: 254
    bin_2    lower_bound: 8.7, upper_bound: 9.61, distinct_count: 91
    bin_252  lower_bound: 108.51, upper_bound: 126.87, distinct_count: 1681
    bin_253  lower_bound: 126.87, upper_bound: 7053.5, distinct_count: 6555
```

<figure markdown>
  ![Estimated and real rows for six filters](../media/figures/ch86-estimates-light.svg#only-light)
  ![Estimated and real rows for six filters](../media/figures/ch86-estimates-dark.svg#only-dark)
  <figcaption markdown="span">The rows from `stats.txt`, on a log scale. Without a histogram, the estimates are wrong by up to four orders of magnitude, in both directions. With a histogram, five of six estimates are within 15% of the real count.</figcaption>
</figure>

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch86-estimates.webp">
    <source src="../../media/clips/ch86-estimates.mp4" type="video/mp4">
  </video>
  <figcaption markdown="span">`stats.py` writes the tables, runs `ANALYZE TABLE`, and prints the estimates against the real rows. The film runs without the bench lock. The text quotes only the saved output.</figcaption>
</figure>

- **The histogram is equi-height.** Each of its 254 bins (`spark.sql.statistics.histogram.numBins`) holds the same number of rows, 14,603. Narrow bins are where the data is dense: bin 1 covers only $5.90 to $8.70. The last bin covers $126.87 to $7,053.50, because few trips cost more than $127.
- **Without a histogram, the error goes in both directions.** The estimate for `BETWEEN 15 AND 25` is 4,180 rows, because $10 is a small part of a range of $8,869. The real count is 1.5 million, because most fares are in that interval.
- **For `payment_type`, the distinct count alone gives 1/6 of the rows** for each value (618,000). The histogram knows that type 4 is rare.
- **The histogram is coarse at the edges.** The last bin holds all trips above $126.87, so the estimate for `> 500` is 13,800 rows, not 82. That is still close enough for the decision in Step 4.

The path from the catalog to a decision:

```mermaid
flowchart LR
  classDef cat fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  classDef est fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  classDef dec fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  a["ANALYZE TABLE<br/>size, rowCount,<br/>column stats, histograms"]:::cat --> b["catalog table properties<br/>spark.sql.statistics.*"]:::cat
  b --> c["Relation.stats<br/>(rowCount only if CBO is on)"]:::est
  c --> d["FilterEstimation<br/>JoinEstimation<br/>AggregateEstimation"]:::est
  d --> e["JoinSelection:<br/>broadcast or shuffle"]:::dec
  d --> f["CostBasedJoinReorder:<br/>join order"]:::dec
  d --> g["PartitionPruning:<br/>is DPP worth it"]:::dec
```

## Step 4: The estimate selects the join strategy

`cbo_join.py` joins all trips to the fares above $500. The filtered side has 82 rows, so a broadcast is the obvious plan. The planner sees it only with CBO:

```text
=== 1. one filter on the small side: the strategy follows the estimate
  spark.sql.cbo.enabled=false
    estimate of the filtered fares: Statistics(sizeInBytes=30.6 MiB)
    planned join: ['SortMergeJoin'], hash exchanges planned: 2
    final plan after AQE: ['BroadcastHashJoin'], hash exchanges that ran: 2; median 0.45 s
  spark.sql.cbo.enabled=true
    estimate of the filtered fares: Statistics(sizeInBytes=647.7 KiB, rowCount=1.38E+4)
    planned join: ['BroadcastHashJoin'], hash exchanges planned: 0
    final plan after AQE: ['BroadcastHashJoin'], hash exchanges that ran: 0; median 0.16 s
  rows: 82
```

- **CBO off.** The filtered `fares` is 30.6 MiB to the planner, above the 10 MB threshold. It plans a sort-merge join with two hash exchanges, so it shuffles all 3.7 million trips.
- **CBO on.** The histogram estimate is 13,800 rows, 647.7 KiB. The planner selects a broadcast hash join and plans no exchange for the join. The query took 0.16 s against 0.45 s.
- **AQE changed the join without CBO, but too late.** After the two shuffles, AQE knew that the filtered side was small. Then it changed the sort-merge join into a broadcast hash join ([Chapter 32](ch32-query-plans.md), Step 6). But both shuffles had already run, and the shuffle of 3.7 million trips was the expensive part. AQE can only correct a plan after a stage ran. CBO corrects it before the first stage.

## Step 5: Join reorder

The order of the joins in a query is the order that you wrote, unless CBO reorders them. With `spark.sql.cbo.joinReorder.enabled`, the rule `CostBasedJoinReorder` lists the inner joins of a query. Then it searches the join orders, from small groups of tables to large groups.

The cost of a plan is a weighted sum of the rows and the bytes of all intermediate results (`spark.sql.cbo.joinReorder.card.weight`, 0.7 for rows).

`cbo_join.py` turns the broadcast off, so that each join is a sort-merge join and the order decides how much data moves. The query joins trips to fares, and then to the zones of Staten Island:

```text
=== 2. join reorder: three tables, broadcast off so that order decides the shuffle size
  spark.sql.cbo.joinReorder.enabled=false: optimized join tree
    Join Inner
          Join Inner
                   Relation trips
                   Relation fares
             Relation zones
    rows out of the two SortMergeJoin nodes: 3,709,141 and 210
    median 1.17 s
  spark.sql.cbo.joinReorder.enabled=true: optimized join tree
    Join Inner
          Join Inner
                   Relation trips
                   Relation zones
             Relation fares
    rows out of the two SortMergeJoin nodes: 210 and 210
    median 0.52 s
  result rows: 16
  speed-up from reorder: 2.2x
```

```mermaid
flowchart TB
  classDef big fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  classDef small fill:#2f7d5b,fill-opacity:0.72,stroke:#2f7d5b,color:#ffffff
  subgraph W["as written"]
    w1["trips JOIN fares<br/>3,709,141 rows"]:::big --> w2["JOIN zones (Staten Island)<br/>210 rows"]:::small
  end
  subgraph R["reordered by CBO"]
    r1["trips JOIN zones (Staten Island)<br/>210 rows"]:::small --> r2["JOIN fares<br/>210 rows"]:::small
  end
```

As written, the first join makes 3.7 million rows, and the second join shuffles them again. Reordered, the first join keeps only the 210 trips that start in Staten Island, and the join with `fares` gets a small left side. The query took 0.52 s against 1.17 s.

The cost model is only as good as its estimates. The third block of `cbo_join.txt` adds histograms on the join keys and runs the same query:

```text
=== 3. the same query after histograms on the join keys
    Join Inner
          Join Inner
                   Relation trips
                   Relation fares
             Relation zones
    estimate of the zones join: Statistics(sizeInBytes=378.3 MiB, rowCount=7.63E+6)
    real rows of trips JOIN zones (Staten Island): 210
```

With histograms on `PULocationID` and `LocationID`, the estimate for the join with the 38 zones of Staten Island is 7.63 million rows. That is more than the 3.7 million trips. The real join has 210 rows.

With this estimate, the written order looks cheaper, and CBO keeps it. The join estimate uses the histograms of both keys. The filter `Borough = 'Staten Island'` is on another column, and the histogram of `LocationID` still describes all 265 zones. It is a limit of the estimate, and `explain("cost")` shows it.

Read the estimates before you trust a reorder.

The CBO settings in Spark 4.3.0:

| Setting | Default | What it does |
|---|---|---|
| `spark.sql.cbo.enabled` | false | use row counts and column statistics in the estimates |
| `spark.sql.cbo.planStats.enabled` | false | get row counts and column statistics from the catalog for the logical plan, also when `spark.sql.cbo.enabled` is false |
| `spark.sql.cbo.joinReorder.enabled` | false | reorder inner joins by cost (needs `spark.sql.cbo.enabled`) |
| `spark.sql.cbo.joinReorder.dp.threshold` | 12 | the largest number of joined tables for the search |
| `spark.sql.cbo.joinReorder.card.weight` | 0.7 | the weight of the rows against the bytes in the cost |
| `spark.sql.cbo.joinReorder.dp.star.filter` | false | apply the star-join heuristic inside the search |
| `spark.sql.cbo.starSchemaDetection` | false | reorder star joins (a fact table and its dimensions) without a cost search |
| `spark.sql.cbo.starJoinFTRatio` | 0.9 | how much smaller than the largest fact table another table can be and still count as a fact table |
| `spark.sql.statistics.histogram.enabled` | false | write histograms in `ANALYZE ... FOR COLUMNS` |
| `spark.sql.statistics.histogram.numBins` | 254 | the bins of each histogram |

## Step 6: Dynamic partition pruning

Partition pruning skips the folders of a partitioned table that a filter rules out. With a literal filter, such as `pickup_date IN (...)`, Spark prunes when it plans the query. That is static pruning.

In a star schema, the filter is usually on a small dimension table, and the partition column is on the large fact table. For example, "the trips on Sundays" is `days.weekday = 'Sunday'`, joined to `trips.pickup_date`. Spark cannot know the Sunday dates when it plans. **Dynamic partition pruning** (DPP) finds them when the query runs, and gives them to the scan of the fact table as a partition filter.

`dpp.py` writes six months of trips partitioned by pickup date (181 partitions) and a table `days` with one row for each date:

```text
trips_by_day: 22,131,665 rows in 181 partitions; days: 181 rows

=== 1. the plan for Sundays only: a subquery on the dimension filters the fact scan
  +- BroadcastExchange (4)
  PartitionFilters: [isnotnull(pickup_date), dynamicpruningexpression(pickup_date IN dynamicpruning)]
  Subquery:1 Hosting operator id = 1 Hosting Expression = pickup_date IN dynamicpruning
  result: 261 pickup zones, 2,885,119 Sunday trips
```

```mermaid
sequenceDiagram
  participant D as scan of days
  participant B as BroadcastExchange
  participant S as SubqueryBroadcast
  participant F as scan of trips_by_day
  participant J as BroadcastHashJoin
  D->>B: rows with weekday = 'Sunday'
  B->>J: hash table of the 26 Sundays
  B->>S: the same broadcast, reused
  S->>F: pickup_date IN (26 dates)
  F->>J: rows of 26 partitions, not 181
```

The plan has two parts. The scan of `trips_by_day` has the partition filter `dynamicpruningexpression(pickup_date IN dynamicpruning)`. The subquery that fills it reuses the `BroadcastExchange` of the join, so Spark reads the dimension only one time.

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch86-anim-dpp.webp">
    <source src="../../media/clips/ch86-anim-dpp.mp4" type="video/mp4">
  </video>
  <figcaption markdown="span">Animation: the 26 Sunday dates go from the dimension to the scan of the fact table. The partition and row counts come from `dpp.txt`. The grid of days is schematic.</figcaption>
</figure>

### The effect, measured

```text
=== 2. what the fact scan read, and the time (median of 7 after a warmup)
  DPP on (default)         partitions read   26  rows out of scan   2,885,119  task time   1.7 s  wall 0.33 s
  DPP off                  partitions read  181  rows out of scan  22,131,665  task time   5.2 s  wall 0.55 s
                           no broadcast: PartitionFilters [isnotnull(pickup_date), dynamicpruningexpression(pickup_date IN dynamicpruning)]
  DPP on, broadcast off    partitions read  181  rows out of scan  22,131,665  task time  13.9 s  wall 1.40 s
  DPP off / DPP on: 1.7x
```

<figure markdown>
  ![Partitions that the fact scan read](../media/figures/ch86-dpp-light.svg#only-light)
  ![Partitions that the fact scan read](../media/figures/ch86-dpp-dark.svg#only-dark)
  <figcaption markdown="span">The partitions, rows and times from `dpp.txt`. The bar length is the number of partitions that the scan read.</figcaption>
</figure>

- **With DPP, the scan read 26 of 181 partitions**, and 2,885,119 rows instead of 22,131,665.
- **The work fell by a factor of three.** The task time, the sum of the run times of all tasks, was 1.7 s against 5.2 s. The wall time was 0.33 s against 0.55 s. The wall time includes the planning, the broadcast and the subquery, which DPP does not make shorter.
- **Without a broadcast, there was no pruning.** With `spark.sql.autoBroadcastJoinThreshold=-1`, the plan still shows the dynamic filter. But by default, DPP only reuses a broadcast that the join makes (`spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly`, true). When AQE finds no broadcast to reuse, it replaces the filter with `true` (`PlanAdaptiveDynamicPruningFilters`). The query read all partitions, and the sort-merge join made it the slowest of the three.

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch86-dpp.webp">
    <source src="../../media/clips/ch86-dpp.mp4" type="video/mp4">
  </video>
  <figcaption markdown="span">`dpp.py` with one timed run for each setting, without the bench lock. The partitions and rows are the same as in `dpp.txt`. The times are not.</figcaption>
</figure>

### When Spark adds DPP, and when it does not

The rule `PartitionPruning` adds a dynamic filter only if the dimension side has a filter that is "likely selective". In the 4.3.0 source, these are comparisons, `IN`, `LIKE` and other string predicates, and `NOT` or `AND` of them. `IS NOT NULL` is not one of them:

```text
=== 3. holidays: 'holiday IS NOT NULL' is not a selective predicate
  PartitionFilters: [isnotnull(pickup_date)]
  no column statistics     partitions read  181  rows out of scan  22,131,665  task time   3.3 s  wall 0.38 s
  after ANALYZE ... FOR COLUMNS on both tables, and spark.sql.cbo.enabled=true:
  PartitionFilters: [isnotnull(pickup_date)]
  statistics and CBO       partitions read  181  rows out of scan  22,131,665  task time   3.9 s  wall 0.50 s
  the same filter written as "d.holiday <> ''", a comparison:
  PartitionFilters: [isnotnull(pickup_date), dynamicpruningexpression(pickup_date IN dynamicpruning)]
  holiday <> ''            partitions read    5  rows out of scan     479,694  task time   0.1 s  wall 0.15 s
    Juneteenth                    116,083  2.87
    Martin Luther King Jr. Day     95,810  2.95
    Memorial Day                   75,497  3.25
    New Year's Day                109,534  2.29
    Presidents' Day                82,770  3.16

=== 4. static partition pruning: the five holidays as literals
  PartitionFilters: [pickup_date IN (2026-01-01,2026-01-19,2026-02-16,2026-05-25,2026-06-19)]
  partitions read 5, files read 5
```

- **`holiday IS NOT NULL` gave no DPP**, with or without column statistics and CBO. The statistics only help the rule estimate the benefit. They do not make a predicate selective.
- **The same filter as a comparison, `holiday <> ''`, gave DPP.** The scan read 5 of 181 partitions and 479,694 rows, with 0.1 s of task time instead of 3.3 s. The result is the same, because no holiday name is empty.
- **Static pruning with the five dates as literals read the same 5 partitions.** DPP gets the same result when the values come from a table.

The benefit check compares the bytes that the scan can skip with the size of the dimension side. Without column statistics, it assumes that the filter keeps half of the partitions (`spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio`, 0.5). With statistics and CBO, it uses the distinct counts of the join keys (`dynamicPartitionPruning.useStats`, true).

!!! note "New in 4.3"
    DPP can also apply when the dimension side has no filter, if Spark already holds that side (SPARK-54593). Examples are a `LocalRelation`, such as a `VALUES` list, and a checkpointed DataFrame. Such a side costs almost nothing to evaluate again.

```text
=== 5. new in 4.3: a small local relation as the filtering side, with no filter at all (SPARK-54593)
  the filtering side: LocalRelation [pickup_date]
  spark.sql.cbo.enabled=false: PartitionFilters [isnotnull(pickup_date), dynamicpruningexpression(pickup_date IN dynamicpruning)]
    partitions read 5, rows out of scan 479,694
  spark.sql.cbo.enabled=true: PartitionFilters [isnotnull(pickup_date), dynamicpruningexpression(pickup_date IN dynamicpruning)]
    partitions read 5, rows out of scan 479,694
```

The five holiday dates as a `VALUES` list pruned the fact table to 5 partitions. The commits of SPARK-54593 are in the history between the tags `v4.2.0` and `v4.3.0`. This book did not run the same query on Spark 4.2.

## Step 7: Bucketed tables

A bucketed table stores its rows hashed by a column into a fixed number of buckets. Spark remembers the bucket column and the number of buckets in the catalog. When a later query joins or groups by that column, each task can read one bucket, and the rows of one key are already together. The shuffle is gone. [Chapter 11](ch11-joins.md), Step 11 showed one bucketed join. This step shows the layout on disk and the cases where buckets help and do not help.

### The layout on disk

```python
(trips.repartition(16, "PULocationID")                       # (1)!
      .write.bucketBy(16, "PULocationID").sortBy("PULocationID")
      .saveAsTable("trips_b16"))
```

1. Without this `repartition`, each of the 17 write tasks writes a file for each bucket that it sees: 272 files instead of 16.

```text
=== 1. write: bucketBy without and with a repartition on the bucket column
  trips_b16_many   272 files, bucket ids in the names: 16 distinct (00000 to 00015)
  trips_b16         16 files, bucket ids in the names: 16 distinct (00000 to 00015)
  trips_plain       17 files, bucket ids in the names: 0 distinct
  one file name: part-00003-4211af59-6479-49ed-873e-c421de208d6e_00003.c000.snappy.parquet
  DESC EXTENDED: Num Buckets 16, Bucket Columns [`PULocationID`], Sort Columns [`PULocationID`]
```

- **The bucket number is in the file name**: `_00003` is bucket 3. Spark finds the buckets of a table from these names, so do not rename the files.
- **`bucketBy` alone makes many files.** Each write task hashes its own rows into 16 buckets and writes one file for each. With 17 write tasks, the table had 272 files. A `repartition` on the bucket column with the same number first puts each bucket into one task, and the table has 16 files.
- **`bucketBy` works only with `saveAsTable`.** The bucket specification lives in the catalog. A plain `parquet(path)` write has nowhere to keep it.

```mermaid
flowchart LR
  classDef t fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  classDef z fill:#2f7d5b,fill-opacity:0.72,stroke:#2f7d5b,color:#ffffff
  classDef j fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  t0["trips_b16<br/>bucket 0"]:::t --> j0["task 0:<br/>sort-merge join<br/>of bucket 0"]:::j
  z0["zstats_b16<br/>bucket 0"]:::z --> j0
  t1["trips_b16<br/>bucket 1"]:::t --> j1["task 1:<br/>bucket 1"]:::j
  z1["zstats_b16<br/>bucket 1"]:::z --> j1
  tn["... bucket 15"]:::t --> jn["task 15:<br/>bucket 15"]:::j
  zn["... bucket 15"]:::z --> jn
```

### Joins and aggregates without a shuffle

`bucketing.py` joins the trips to a table of zone statistics, with the broadcast turned off. It uses plain tables, tables with 16 buckets on both sides, and 16 buckets against 8:

```text
=== 2. the same join on four pairs of tables (broadcast off)
  plain   x plain                  SortMergeJoin  exchanges 2  0.37 s
                                     Exchange hashpartitioning(PULocationID, 200)
                                     Exchange hashpartitioning(PULocationID, 200)
  b16     x b16                    SortMergeJoin  exchanges 0  0.19 s
  b16     x b8                     SortMergeJoin  exchanges 1  0.16 s
                                     Exchange hashpartitioning(PULocationID, 16)
  b16     x b8, coalesce buckets   SortMergeJoin  exchanges 0  0.16 s
  b16     x b16, bucketing off     SortMergeJoin  exchanges 2  0.35 s
                                     Exchange hashpartitioning(PULocationID, 200)
                                     Exchange hashpartitioning(PULocationID, 200)
  plain / b16 x b16: 1.9x

=== 3. an aggregate on the bucket key
  trips_plain  exchanges 1 ['hashpartitioning(PULocationID, 200)']
  trips_b16    exchanges 0 []
```

<figure markdown>
  ![Exchanges for each join](../media/figures/ch86-joins-light.svg#only-light)
  ![Exchanges for each join](../media/figures/ch86-joins-dark.svg#only-dark)
  <figcaption markdown="span">The `Exchange` nodes of one join for each pair of tables (`bucketing.txt`, `spj.txt`).</figcaption>
</figure>

- **Same bucket count, same key: no exchange.** The join of `trips_b16` and `zstats_b16` read bucket i of each table in task i. It took 0.19 s against 0.37 s for the plain tables, which shuffled both sides into 200 partitions.
- **16 against 8: one exchange.** Spark shuffled the 8-bucket side into 16 partitions to match the larger side.
- **With `spark.sql.bucketing.coalesceBucketsInJoin.enabled`, no exchange.** Spark reads the 16 buckets as 8 groups of 2. This works when the larger count is a multiple of the smaller one, and the ratio is at most `spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio` (4).
- **With `spark.sql.sources.bucketing.enabled=false`**, Spark treats the tables as plain tables.
- **An aggregate on the bucket key needs no exchange either.**
- **The joins with two exchanges took 0.35 and 0.37 s. The joins with one exchange or none took 0.16 to 0.19 s.** At this size, the times of one exchange and of none are the same within the noise.

### Bucket pruning, and when Spark does not use the buckets

A filter on the bucket column with an equality can select one bucket. Spark shows it as `SelectedBucketsCount`. But Spark does not always read a table as bucketed:

```text
=== 4. bucket pruning: a filter on the bucket column
  autoBucketedScan.enabled=true: Bucketed: false (disabled by query planner)
    metric 'number of files read' 16, rows out of scan 180,000
    tasks of the query 2, tasks that read input 1, input bytes 90,940
  autoBucketedScan.enabled=false: Bucketed: true | SelectedBucketsCount: 1 out of 16
    metric 'number of files read' 16, rows out of scan 180,000
    tasks of the query 17, tasks that read input 1, input bytes 90,940

=== 5. a query that gains nothing from buckets: the planner turns the bucketed scan off
  Bucketed: false (bucket column(s) not read)
```

- **By default, the planner reads the table as a plain table for this query.** The setting `spark.sql.sources.bucketing.autoBucketedScan.enabled` (true) turns the bucketed scan off when no join or aggregate above it needs the buckets. The plan says `Bucketed: false (disabled by query planner)`.
- **With the automatic choice turned off, Spark pruned to 1 of 16 buckets.** The query then ran 17 tasks, and 16 of them read nothing. The plain scan used 1 task, because the Parquet statistics of the sorted files had already skipped the other files. Both scans read the same 90,940 bytes. In this case, the planner's choice was the better one.
- **The metric `number of files read` counts all 16 files in both cases.** Spark sets it from the list of files, before it removes the other buckets (`DataSourceScanExec.scala`). Count the tasks that read input to see the pruning.
- **A query that does not read the bucket column cannot use the buckets.**

Buckets have costs. Each write must hash and sort the data into the buckets. The table keeps its number of buckets, so a table that grows gets larger buckets. And the advantage holds only for joins and aggregates on exactly the bucket columns. Table formats offer the same effect with more freedom, which is the subject of the next step.

## Step 8: Storage-partitioned joins

Bucketing is a feature of Spark's own file tables. A table format such as Apache Iceberg has its own partition transforms, for example `bucket(16, PULocationID)`. A DataSource V2 table can report its partitioning to Spark through the interface `SupportsReportPartitioning`. If both sides of a join report a compatible partitioning on the join keys, Spark can join them partition by partition, without a shuffle. This is a **storage-partitioned join** (SPJ). The 4.3.0 tuning guide calls it a generalization of the bucket join.

!!! note "The Iceberg runtime in this step is not a release"
    No released Iceberg runtime supports Spark 4.3 yet ([Chapter 45](ch45-iceberg.md), Step 1). `spj.py` uses the same jar as Chapter 45: `iceberg-spark-runtime-4.3_2.13`, built from apache/iceberg pull request 18749 (commit `b1eab64`). The Spark side of SPJ is in Spark 4.3.0. The Iceberg side can change before a release.

`spj.py` makes three Iceberg tables in a Hadoop catalog. The partition transform of `trips` and `zstats` is `bucket(16, PULocationID)`, and of `zstats8` it is `bucket(8, PULocationID)`. Then it runs one join with five sets of settings:

```sql
CREATE TABLE lake.db.trips (PULocationID INT, DOLocationID INT, total_amount DOUBLE, tip_amount DOUBLE)
USING iceberg PARTITIONED BY (bucket(16, PULocationID))
```

```text
Spark 4.3.0, Iceberg jar iceberg-spark-runtime-4.3_2.13-b1eab64.jar
  lake.db.trips    data files 16
  lake.db.zstats   data files 16
  lake.db.zstats8  data files 8

=== the same join, with each set of settings (broadcast off)
  defaults                                 SortMergeJoin  Exchange nodes 2  0.53 s
      +- Project (8)
         +- SortMergeJoin Inner (7)
            :- Sort (3)
            :  +- Exchange (2)
            :     +- BatchScan lake.db.trips (1)
            +- Sort (6)
               +- Exchange (5)
                  +- BatchScan lake.db.zstats (4)
  preserve-data-grouping                   SortMergeJoin  Exchange nodes 0  0.28 s
      +- Project (6)
         +- SortMergeJoin Inner (5)
            :- Sort (2)
            :  +- BatchScan lake.db.trips (1)
            +- Sort (4)
               +- BatchScan lake.db.zstats (3)
  16 x 8 buckets, preserve-data-grouping   SortMergeJoin  Exchange nodes 2  0.46 s
  16 x 8, + allowCompatibleTransforms      SortMergeJoin  Exchange nodes 0  0.32 s
  v2 bucketing off                         SortMergeJoin  Exchange nodes 2  0.42 s
```

- **With the defaults, the plan has two `Exchange` nodes.** Spark's side of SPJ (`spark.sql.sources.v2.bucketing.enabled`) is on by default since Spark 3.3. But Iceberg reports its partitioning only if `spark.sql.iceberg.planning.preserve-data-grouping` is true, and that setting is false by default.
- **With `preserve-data-grouping`, the `Exchange` nodes are gone.** Each `BatchScan` delivers the rows of one bucket to one task, and the sort-merge join reads them in place. The join took 0.28 s against 0.53 s.
- **16 buckets against 8 buckets needs one more setting.** `bucket(16, x)` and `bucket(8, x)` are different transforms, so Spark shuffled both sides. With `spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled`, Spark asks the bucket function for a reducer. Iceberg's `BucketFunction` is a `ReducibleFunction`. It returns a reducer to the greatest common divisor of the two counts, which is 8. Spark groups the 16 buckets into 8, and the join has no `Exchange` again.
- **With `spark.sql.sources.v2.bucketing.enabled=false`**, Spark ignores the reported partitioning.

The settings for SPJ in Spark 4.3.0, from the tuning guide and `SQLConf.scala`:

| Setting | Default | What it does |
|---|---|---|
| `spark.sql.sources.v2.bucketing.enabled` | true | use the partitioning that a V2 source reports |
| `spark.sql.sources.v2.bucketing.pushPartValues.enabled` | true | if one side has partition values that the other side does not have, add empty partitions instead of a shuffle |
| `spark.sql.requireAllClusterKeysForCoPartition` | true | require that the join keys are the same as the partition keys, in the same order |
| `spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled` | false | split the partitions of the larger side and replicate the matching partitions of the other side, for skew |
| `spark.sql.sources.v2.bucketing.allowKeysSubsetOfPartitionKeys.enabled` | false | allow a join on a subset of the partition keys (needs `requireAllClusterKeysForCoPartition=false`) |
| `spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled` | false | allow transforms that are compatible but not the same, such as two bucket counts |
| `spark.sql.sources.v2.bucketing.shuffle.enabled` | false | shuffle only the side that does not report a partitioning |
| `spark.sql.iceberg.planning.preserve-data-grouping` | false | an Iceberg setting: report the partitioning of the table to Spark |

In 4.3.0, the main name of the subset setting is `spark.sql.sources.v2.bucketing.allowKeysSubsetOfPartitionKeys.enabled`, and `allowJoinKeysSubsetOfPartitionKeys` is its alternative name. The tuning guide of 4.3.0 still uses the old name. Both names work.

## Step 9: Hints

A hint tells the planner what you know and the statistics do not. Spark has two kinds of hints. Join hints select the strategy of one join, and partitioning hints add or change an exchange. Step 3 of [Chapter 11](ch11-joins.md) measured each join strategy on the same query. This step shows what each hint puts in the plan, which hint wins, and what happens to a hint that cannot apply. `hints.py` reads the Parquet folders of the tables as temporary views.

### Join hints

```text
=== 1. join hints in SQL (trips JOIN zones; zones is 6 KB, so the default is a broadcast)
  (no hint)                  BroadcastHashJoin BuildRight
  BROADCAST(t)               BroadcastHashJoin BuildLeft
  MERGE(z)                   SortMergeJoin
  SHUFFLE_MERGE(z)           SortMergeJoin
  SHUFFLE_HASH(z)            ShuffledHashJoin BuildRight
  SHUFFLE_REPLICATE_NL(z)    CartesianProduct
  MAPJOIN(z)                 BroadcastHashJoin BuildRight
```

=== "SQL"

    ```sql
    SELECT /*+ SHUFFLE_HASH(z) */ z.Borough, count(*)
    FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Borough
    ```

=== "Python"

    ```python
    t.join(z.hint("shuffle_hash"), t.PULocationID == z.LocationID)
    t.join(F.broadcast(z), t.PULocationID == z.LocationID)   # the same as z.hint("broadcast")
    ```

```text
=== 2. the same hints on a DataFrame
  z.hint('broadcast')        BroadcastHashJoin BuildRight
  z.hint('merge')            SortMergeJoin
  z.hint('shuffle_hash')     ShuffledHashJoin BuildRight
  z.hint('shuffle_replicate_nl') CartesianProduct
  F.broadcast(t)             BroadcastHashJoin BuildRight
```

| Hint | Other names | Strategy | Needs |
|---|---|---|---|
| `BROADCAST(t)` | `BROADCASTJOIN`, `MAPJOIN` | broadcast hash join, or broadcast nested loop join without an equality | `t` must be a side that the join type lets Spark build |
| `MERGE(t)` | `SHUFFLE_MERGE`, `MERGEJOIN` | sort-merge join | an equality, sortable keys |
| `SHUFFLE_HASH(t)` | | shuffled hash join, with `t` as the build side | an equality |
| `SHUFFLE_REPLICATE_NL(t)` | | Cartesian product (a nested loop on each pair of partitions) | an inner join |

A hint names a table or an alias, not a side. `BROADCAST(t)` on the trips made Spark build a hash table of 3.7 million trips (`BuildLeft`), because the hint is stronger than the size estimate. A hint is an order, not a suggestion. Make sure that it stays correct when the data grows.

### Two hints, and hints that cannot apply

```text
=== 3. two hints in one query: which one wins
  BROADCAST(t), MERGE(z)                     BroadcastHashJoin BuildLeft
  MERGE(t), SHUFFLE_HASH(z)                  SortMergeJoin
  SHUFFLE_HASH(t), SHUFFLE_REPLICATE_NL(z)   ShuffledHashJoin BuildLeft
  BROADCAST(t), BROADCAST(z)                 BroadcastHashJoin BuildRight
  SHUFFLE_HASH(t), SHUFFLE_HASH(z)           ShuffledHashJoin BuildRight
```

When the two sides have different hints, the order of precedence is `BROADCAST`, then `MERGE`, then `SHUFFLE_HASH`, then `SHUFFLE_REPLICATE_NL`. Sometimes both sides have the same `BROADCAST` or `SHUFFLE_HASH` hint. Then the planner selects the build side from the join type and the sizes, here the zones (`BuildRight`).

```text
=== 4. hints that the planner cannot use (it ignores them and logs a warning)
  BROADCAST(t) on a full outer join:   SortMergeJoin
  SHUFFLE_HASH(z) on a non-equi join:  BroadcastNestedLoopJoin BuildRight
  BROADCAST(x), no table x:            BroadcastHashJoin BuildRight
  BROADKAST(z), a misspelled name:     BroadcastHashJoin BuildRight
--- warnings in the log (stderr):
WARN HintErrorLogger: Hint (strategy=broadcast) is not supported in the query: build left for full outer join.
WARN HintErrorLogger: Hint (strategy=shuffle_hash) is not supported in the query: no equi-join keys.
WARN HintErrorLogger: Count not find relation 'x' specified in hint 'BROADCAST(x)'.
WARN HintErrorLogger: Unrecognized hint: BROADKAST(z)
```

The planner never fails a query because of a hint. It ignores a hint that cannot apply, and writes a warning from the logger `HintErrorLogger`:

- **A full outer join cannot broadcast a side**, because each row of both sides must stay in the result. The planner used a sort-merge join.
- **A hash join needs an equality.** For `t.PULocationID < z.LocationID`, the planner used its default for a non-equi join, a broadcast nested loop join.
- **A hint on a name that is not in the query, and a misspelled hint, have no effect.** The plan is the default plan.

These warnings go to the driver log at level WARN. At the log level of the book's scripts (ERROR), a hint with a wrong name gives no message. Read `explain()` after you add a hint, and make sure that the plan changed.

### Partitioning hints

Partitioning hints are the SQL form of `repartition`, `coalesce` and `repartitionByRange` ([Chapter 33](ch33-partitions-caching-memory.md), Step 3). `REBALANCE` has no DataFrame method. `hints.py` writes the trips once with each hint and counts the files:

```text
=== 5. partitioning hints: the Exchange that each one adds, and the files of a write
  (no hint)                                  files   5  MB min   0.0 max  13.1
                                             (no Exchange)
  COALESCE(2)                                files   2  MB min  12.9 max  34.4
                                             Coalesce 2
  REPARTITION(4)                             files   4  MB min  14.2 max  14.2
                                             Exchange RoundRobinPartitioning(4), REPARTITION_BY_NUM
  REPARTITION(4, PULocationID)               files   4  MB min   8.2 max  16.7
                                             Exchange hashpartitioning(PULocationID, 4), REPARTITION_BY_NUM
  REPARTITION(PULocationID)                  files  21  MB min   0.8 max   3.0
                                             Exchange hashpartitioning(PULocationID, 200), REPARTITION_BY_COL
  REPARTITION_BY_RANGE(4, trip_distance)     files   4  MB min  10.9 max  13.9
                                             Exchange rangepartitioning(trip_distance ASC NULLS FIRST, 4), REPARTITION_BY_NUM
  REBALANCE                                  files  17  MB min   2.3 max   3.4
                                             Exchange RoundRobinPartitioning(200), REBALANCE_PARTITIONS_BY_NONE
  REBALANCE(PULocationID)                    files  21  MB min   0.8 max   3.0
                                             Exchange hashpartitioning(PULocationID, 200), REBALANCE_PARTITIONS_BY_COL
  REBALANCE, parallelismFirst off            files   2  MB min  21.0 max  36.0
                                             Exchange RoundRobinPartitioning(200), REBALANCE_PARTITIONS_BY_NONE
  REBALANCE, parallelismFirst off, 16 MB     files   7  MB min   4.0 max   8.8
                                             Exchange RoundRobinPartitioning(200), REBALANCE_PARTITIONS_BY_NONE
```

```text
=== 6. the DataFrame forms
  t.coalesce(2)                              Coalesce 2
  t.repartition(4, 'PULocationID')           Exchange hashpartitioning(PULocationID, 4), REPARTITION_BY_NUM
  t.repartitionByRange(4, 'trip_distance')   Exchange rangepartitioning(trip_distance ASC NULLS FIRST, 4), REPARTITION_BY_NUM
  t.hint('rebalance', 'PULocationID')        Exchange hashpartitioning(PULocationID, 200), REBALANCE_PARTITIONS_BY_COL
```

| Hint | Exchange | Number of output partitions | AQE can change it |
|---|---|---|---|
| `COALESCE(n)` | none (`Coalesce n`) | n, by merging the input partitions | no |
| `REPARTITION(n)` | round robin | n | no |
| `REPARTITION(n, col)` | hash on `col` | n | no |
| `REPARTITION(col)` | hash on `col` | `spark.sql.shuffle.partitions`, then coalesced | yes, coalesce |
| `REPARTITION_BY_RANGE(n, col)` | range on `col` | n, sorted ranges | no |
| `REBALANCE` | round robin | coalesced to the advisory size | yes, coalesce and split skew |
| `REBALANCE(col)` | hash on `col` | coalesced to the advisory size | yes, coalesce and split skew |

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch86-hints.webp">
    <source src="../../media/clips/ch86-hints.mp4" type="video/mp4">
  </video>
  <figcaption markdown="span">`hints.py`: the exchange and the files that each partitioning hint gives.</figcaption>
</figure>

- **A number fixes the result.** `REPARTITION(4)` made four files of 14.2 MB each. AQE does not change a partition count that you gave.
- **Without a number, AQE decides.** `REPARTITION(PULocationID)` and `REBALANCE(PULocationID)` both started with 200 shuffle partitions, and AQE coalesced them to 21.
- **`REBALANCE` follows the advisory size only if parallelism comes second.** The default is `spark.sql.adaptive.coalescePartitions.parallelismFirst=true`. Then AQE keeps at least as many partitions as the default parallelism (16 here), so `REBALANCE` made 17 files of about 3 MB. With `parallelismFirst=false`, AQE coalesced to the 64 MB advisory size (`spark.sql.adaptive.advisoryPartitionSizeInBytes`), and made 2 files. With an advisory size of 16 MB, it made 7 files. The advisory size counts shuffle bytes, not Parquet bytes, so the files are smaller than the advisory size.
- **`REBALANCE` is the hint for the last step before a write.** It gives files of a useful size, and AQE splits a skewed partition into several files. It does not guarantee that all rows of one key are in one file.

## The whole chapter, on one page

```python
# Statistics: size, rows, columns, histograms. Run again after large writes.
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS FOR COLUMNS total_amount, payment_type")
spark.sql("DESC EXTENDED fares total_amount").show(truncate=False)

# Use them: estimates with rows, join selection and join reorder.
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
q.explain("cost")                                   # read the rowCount of each node before you trust it

# Dynamic partition pruning: a selective filter on a small, broadcast dimension.
spark.sql("""SELECT ... FROM trips_by_day t JOIN days d ON t.pickup_date = d.pickup_date
             WHERE d.weekday = 'Sunday'""")         # 'IS NOT NULL' alone does not trigger it

# Buckets: same column, same count on both sides, written with saveAsTable.
(df.repartition(16, "PULocationID").write.bucketBy(16, "PULocationID")
   .sortBy("PULocationID").saveAsTable("trips_b16"))

# Storage-partitioned join on Iceberg tables with the same bucket transform.
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")

# Hints: strategies and partitioning, when you know more than the estimates.
spark.sql("SELECT /*+ BROADCAST(f) */ ... FROM trips t JOIN fares f ON t.trip_id = f.trip_id")
spark.sql("SELECT /*+ REBALANCE(PULocationID) */ * FROM trips").write.parquet(out)
```

- **Without statistics, the optimizer knows only sizes.** A filter does not change the size estimate, so a small filtered result never looks small.
- **`ANALYZE TABLE` stores sizes, rows and column statistics, and CBO uses them.** Without a histogram, a filter estimate assumes values spread evenly between the minimum and the maximum, and it can be wrong by orders of magnitude. A histogram fixed five of six estimates in Step 3.
- **Better estimates change the plan before it runs.** CBO selected a broadcast for 82 rows before the shuffles. AQE made the same change only after both shuffles had run. It also reordered three joins, so that the first join kept 210 rows instead of 3.7 million.
- **The cost model is only as good as its estimates.** With histograms on the join keys, the estimate for a join was larger than both inputs, and CBO kept the written order.
- **DPP turns a filter on a dimension into a filter on the partitions of a fact table.** It needs a selective predicate on the dimension, or (new in 4.3) a small local relation, and a broadcast that it can reuse.
- **Buckets and storage-partitioned joins remove the shuffle** when both sides have the same layout on the join key. Spark can group buckets when one count is a multiple of the other.
- **Hints win over estimates, and the planner ignores a hint that cannot apply.** Read the plan after each hint. `REBALANCE` before a write gives files of a useful size.

## Further reading

- [Performance tuning for Spark SQL](https://spark.apache.org/docs/4.3.0/sql-performance-tuning.html): statistics, join hints, coalesce hints, AQE, storage partition join.
- [ANALYZE TABLE](https://spark.apache.org/docs/4.3.0/sql-ref-syntax-aux-analyze-table.html) and [hints](https://spark.apache.org/docs/4.3.0/sql-ref-syntax-qry-select-hints.html) in the SQL reference.
- [Bucketing and partitioning](https://spark.apache.org/docs/4.3.0/sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning) in the data source guide.
- The 4.3.0 source: `statsEstimation/` (`FilterEstimation.scala`, `JoinEstimation.scala`, `SizeInBytesOnlyStatsPlanVisitor.scala`), `CostBasedJoinReorder.scala`, `dynamicpruning/PartitionPruning.scala`, `adaptive/PlanAdaptiveDynamicPruningFilters.scala`, `bucketing/CoalesceBucketsInJoin.scala`, `DataSourceScanExec.scala`, and `ResolveHints.scala`.
- [Chapter 11](ch11-joins.md) for the join strategies and their measured times, [Chapter 32](ch32-query-plans.md) for plans and AQE, [Chapter 45](ch45-iceberg.md) for the Iceberg runtime.
