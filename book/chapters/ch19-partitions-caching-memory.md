# Chapter 19: Partitions, caching, memory and spill

[Chapter 18](ch18-query-plans.md) showed what Spark decides. This chapter shows what those decisions cost when they run. A plan can be correct and still slow.

For example, 16 tasks read a file with 4 row groups, and twelve tasks do nothing. One task does all the work because of a `coalesce(1)` at the end. A sort writes almost 1 GiB to the disk because each task got a small part of a small heap. A cache makes a query slower than the Parquet files that it replaced.

**Three numbers set the cost of most Spark jobs. They are the number of partitions in each stage, the memory that each task gets, and the bytes that move between stages. You can predict the first number from the input files before you run anything. You can read the second number from the memory settings. You can measure the third number in the Spark UI. Caching and repartitioning are tools that change these numbers, and each of them has a cost.**

The scripts are in [`code/ch19`](https://github.com/lisancao/spark-masterclass/tree/main/code/ch19), and each output block comes from `code/ch19/output/`. The data is six months of yellow-taxi trips, January to June 2026, plus the June file alone from [Chapter 1](ch01-how-spark-runs.md). The six months are six Parquet files of 372.6 MB, with 21,828,310 rows after the cleaning in Step 5. All runs use Spark 4.3.0 in `local[*]` mode on a machine with 16 cores. Each timing is a median after a warm-up run, measured while no other benchmark ran on the machine.

## Step 1: Which settings you can change, and when

Spark has two kinds of settings. **Runtime settings** are SQL settings. You can change them in a session at any time with `spark.conf.set`, and the next query uses the new value. **Static settings** configure the JVM, the memory manager and the scheduler. Spark reads them one time, when the application starts. `config.py` asks the session which settings it can change:

```python
for k in KEYS:
    print(f"{k:<46}{spark.conf.get(k, '(not set)'):>12}  {spark.conf.isModifiable(k)}")
```

```text
key                                                  value  isModifiable
spark.sql.shuffle.partitions                           200  True
spark.sql.files.maxPartitionBytes               134217728b  True
spark.sql.adaptive.enabled                            true  True
spark.sql.autoBroadcastJoinThreshold             10485760b  True
spark.sql.execution.arrow.maxRecordsPerBatch         10000  True
spark.executor.memory                            (not set)  False
spark.driver.memory                                     4g  False
spark.memory.fraction                            (not set)  False
spark.default.parallelism                        (not set)  False
spark.serializer                                 (not set)  False
spark.python.worker.reuse                        (not set)  False

set spark.sql.shuffle.partitions=64 -> 64
set spark.memory.fraction=0.8 -> [CANNOT_MODIFY_CONFIG] Cannot modify the value of the Spark config: "spark.memory.fraction".
set spark.executor.memory=8g -> [CANNOT_MODIFY_CONFIG] Cannot modify the value of the Spark config: "spark.executor.memory".
```

All `spark.sql.*` settings in this list are runtime settings. The memory settings, the default parallelism, the serializer and the Python worker settings are static. If you try to change a static setting in a session that runs, Spark gives the error `CANNOT_MODIFY_CONFIG`. To change a static setting, give it to `SparkSession.builder.config(...)` before the first session starts, or to `spark-submit --conf`. The **Environment** tab of the Spark UI shows the values that the application started with.

`(not set)` means that the application did not set a value, and Spark uses the default in its code. In local mode, `spark.driver.memory` is the only memory setting that matters, because the driver and the executor are one JVM. The scripts that read six months of data set it to `4g`. The reason is in Step 6.

## Step 2: How many partitions a file read makes

A scan stage has one task for each **input partition**. Spark calculates the input partitions on the driver, before it reads any data, from the list of files and their sizes. The calculation is in `FilePartition.scala`:

1. Add the size of each file, plus `spark.sql.files.openCostInBytes` (4 MB) for each file. The open cost is the estimated cost to open a file, written as bytes.
2. Divide this total by the default parallelism (16 here), or by `spark.sql.files.minPartitionNum` if you set it. The result is the bytes for each core.
3. The maximum split size is `min(maxPartitionBytes, max(openCostInBytes, bytesPerCore))`. `maxPartitionBytes` is 128 MB by default.
4. Cut each splittable file into pieces of the maximum split size. A file that Spark cannot split stays as one piece.
5. Sort the pieces from the largest to the smallest. Put them into partitions in that sequence, and start a new partition when the next piece does not fit. Each piece also adds the open cost to its partition.
6. If there are more partitions than `spark.sql.files.maxPartitionNum`, do the packing again with a larger split size. The new split size is the total divided by `maxPartitionNum`.

```mermaid
flowchart LR
  classDef in fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  classDef calc fill:#6b4a9b,fill-opacity:0.72,stroke:#6b4a9b,color:#ffffff
  classDef out fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  files["file list<br/>and sizes"]:::in --> total["total bytes<br/>+ open cost per file"]:::calc
  total --> split["max split =<br/>min(maxPartitionBytes,<br/>max(openCost, total / cores))"]:::calc
  split --> cut["cut splittable files<br/>into pieces"]:::calc
  cut --> pack["sort pieces, largest first,<br/>pack into partitions"]:::calc
  pack --> check{"more than<br/>maxPartitionNum?"}:::calc
  check -->|"no"| parts["input partitions<br/>= scan tasks"]:::out
  check -->|"yes: split = total / maxPartitionNum"| pack
```

`file_partitions.py` does this calculation in Python for each input, and then it compares the prediction with `df.rdd.getNumPartitions()`:

```text
local[*] with 16 cores; defaults: maxPartitionBytes 128MB, openCostInBytes 4MB

input                                  files      MB  max split  predicted  actual
June: 1 Parquet file                       1    62.4       4.2M         16      16
June, maxPartitionBytes 2MB                1    62.4       2.0M         32      32
January to June: 6 files                   6   372.6      24.8M         17      17
January to June, minPartitionNum 64        6   372.6       6.2M         62      62
June as 200 small files                  200    98.4      56.2M         16      16
200 small files, openCostInBytes 1       200    98.4       6.2M         17      17
200 small files, maxPartitionNum 4       200    98.4     224.6M          4       4
CSV, 500,000 rows                          1    55.1       4.0M         14      14
the same CSV, gzip                         1     9.7       4.0M          1       1
the same CSV, bzip2                        1     5.1       4.0M          2       2
```

The prediction is correct in each case. Some rows need an explanation:

- **The split size follows the data, not the 128 MB default.** For less than 2 GB of input on 16 cores, the bytes for each core are smaller than 128 MB. Thus, Spark makes about one partition for each core. The 128 MB limit applies only to large inputs.
- **Six files make 17 partitions, not 16.** The pieces do not divide exactly into 16 groups, so the packing needs one more partition.
- **Small files cost an open cost each.** June as 200 small files is 98.4 MB on the disk, but the packing counts 200 × 4 MB of open cost more. The partitions are full sooner. With `openCostInBytes` at 1, the same files pack into 17 partitions of real data.
- **`maxPartitionNum` is a ceiling.** Spark packs the files again with a split of 224.6 MB, and it ignores `maxPartitionBytes`. Spark logs a warning when it does this.
- **gzip cannot split.** A gzip stream has no points where a reader can start in the middle. Thus, the 9.7 MB file is one piece and one task, regardless of its size. bzip2 has block boundaries, and Hadoop's `BZip2Codec` implements `SplittableCompressionCodec`. Thus, the 5.1 MB bzip2 file splits into two pieces.

Other inputs that do not split: a JSON or CSV file with `multiLine=true`, a `wholetext` text read, and each file of a `binaryFile` read. One large gzip file makes one task for the whole scan stage. If a job reads such a file, `repartition` after the read is the only method to spread the next steps over the cores.

### A partition is not the same as a task with work

The split rule counts bytes. Parquet decodes **row groups**, as [Chapter 1](ch01-how-spark-runs.md) showed. A Parquet split reads a row group only if the split contains the midpoint of that row group:

```text
row groups: June 4 | January to June 24
June: 16 partitions, 4 of them return rows
January to June: 17 partitions, 17 of them return rows
```

The June file gives 16 partitions, but only 4 of them return rows. The six months have 24 row groups in 17 partitions, so each partition gets at least one row group. For a scan, the worst case is a small file with a few large row groups. A writer sets the size of the row groups, as [Chapter 22](ch22-parquet-inside.md) shows.

!!! note "Project Feather changes the June row"
    Spark 4.3 can read a single small file in one task. If `spark.sql.optimizer.singleTaskExecution.enabled` is `true`, the June file is one input partition, not 16. The conditions are in [Chapter 23](ch23-project-feather.md). All the numbers in this chapter use the default, `false`.

## Step 3: repartition, coalesce and the cost of `coalesce(1)`

After the scan, two methods change the number of partitions. `repartition` adds a shuffle. `coalesce` merges partitions that are next to each other, without a shuffle. `repartition.py` applies each variant to the six months and prints the top operator of the plan:

```text
input partitions: 17

call                                   partitions  top of the plan
repartition(8)                                  8  Exchange RoundRobinPartitioning(8), REPARTITION_BY_NUM, [plan_
repartition(8, 'PULocationID')                  8  Exchange hashpartitioning(PULocationID#7, 8), REPARTITION_BY_N
repartition('PULocationID')                    22  Exchange hashpartitioning(PULocationID#7, 200), REPARTITION_BY
repartitionByRange(4, 'fare_amount')            4  Exchange rangepartitioning(fare_amount#10 ASC NULLS FIRST, 4),
coalesce(4)                                     4  Coalesce 4
coalesce(100)                                  17  Coalesce 100

non-empty partitions after repartition('PULocationID'): 8
```

Each call has a different effect on the plan:

- **`repartition(n)`** adds an `Exchange` with round-robin partitioning. The rows go to the `n` partitions in turn, so the partitions have almost equal sizes. The rows with one key do not stay together.
- **`repartition(n, col)`** adds an `Exchange` with hash partitioning on the column. All the rows with one value go to the same partition. A later `groupBy` or join on that column does not need a new shuffle.
- **`repartition(col)` without a number** uses `spark.sql.shuffle.partitions` (200). AQE can coalesce these partitions. Here, `getNumPartitions()` gave 22 after the coalesce. A second query on the same DataFrame found rows in only 8 partitions. That query selected only `spark_partition_id()`, so its shuffle had less data, and AQE made fewer partitions from it. The number of partitions after a shuffle with AQE is a result of the run, not a fixed property of the DataFrame.
- **`repartitionByRange(n, col)`** samples the column, and then it sends each range of values to one partition. Partition 0 gets the lowest fares. Spark uses this method for a global sort.
- **`coalesce(n)`** has no `Exchange`. It merges partitions that are next to each other inside one stage. It can only decrease the number of partitions. `coalesce(100)` on 17 partitions keeps 17.

The `spark_partition_id()` count also works over Spark Connect, where `df.rdd` does not exist. It counts only partitions that hold rows.

### The cost of `coalesce(1)`

`coalesce` has no shuffle. Thus, it has no stage boundary. The operators before `coalesce(1)` are in the same stage as the `coalesce`, and that stage now has one task. All the work before it runs in that one task. `repartition(1)` adds a shuffle. The work before the shuffle keeps all its tasks, and only the write after the shuffle runs in one task.

The script calculates a SHA-256 hash of each row of the six months, which is an expensive step. Then it writes the result to the `noop` sink, which runs the query and discards the rows:

```python
expensive = trips.select(F.sha2(F.concat_ws("|", *trips.columns), 256).alias("h"))
timed("no change", expensive)
timed("coalesce(1)", expensive.coalesce(1))
timed("repartition(1)", expensive.repartition(1))
```

```text
SHA-256 of every row of 22,836,530 trips, written to the noop sink, 5 runs after a warm-up:
  no change                  median 17.70 s  (task counts per stage: [17])
  coalesce(1)                median 67.99 s  (task counts per stage: [1])
  repartition(1)             median 19.81 s  (task counts per stage: [1, 17])
```

```mermaid
flowchart LR
  classDef many fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  classDef one fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  subgraph C["coalesce(1): one stage"]
    direction LR
    c1["scan + hash<br/>+ write<br/>1 task"]:::one
  end
  subgraph R["repartition(1): two stages"]
    direction LR
    r1["scan + hash<br/>17 tasks"]:::many -->|"shuffle"| r2["write<br/>1 task"]:::one
  end
```

With `coalesce(1)`, the hash of 22.8 million rows ran in one task, and the job took 3.8 times as long. With `repartition(1)`, the hash ran in 17 tasks, and the job took only 12% more time than the job with no change. The extra time is the cost of the shuffle.

Use `coalesce(n)` only to decrease a large number of small partitions to a number that is still large enough for the cores. If you must write one file, use `repartition(1)`. Then the work before the write keeps its parallelism.

## Step 4: Write a layout that later queries can skip

A layout on disk sets the partitions of each later read. `partitionBy` on a write puts the rows into one folder for each value of a column, such as `day=2026-06-15/`. A filter on that column then removes whole folders before Spark lists a file in them. This is **partition pruning**. `layout.py` writes the June trips by pickup day in four ways:

```python
WAYS = [
    ("plain write", lambda d: d.write),
    ("repartition(16) first", lambda d: d.repartition(16).write),
    ("repartition('day') first", lambda d: d.repartition("day").write),
    ("repartition('day'), maxRecordsPerFile 50000",
     lambda d: d.repartition("day").write.option("maxRecordsPerFile", 50000)),
]
```

```text
input partitions: 16

how                                            folders  files  min/folder  max/folder
plain write                                         30     63           2           3
repartition(16) first                               30    480          16          16
repartition('day') first                            30     30           1           1
repartition('day'), maxRecordsPerFile 50000         30     92           3           4
```

Each write task writes one file into each folder for which it has rows. Thus, the number of files is the number of tasks multiplied by the number of days that each task holds:

- **The plain write** made 63 files. The input has four row groups in time sequence, so each task holds only a few days.
- **`repartition(16)`** spreads each day over all 16 tasks. The result is 16 files in each of 30 folders, 480 small files in total. This is the most frequent cause of the small-file problem.
- **`repartition("day")`** puts each day into one task, so each folder gets one file.
- **`maxRecordsPerFile`** starts a new file after 50,000 rows. Thus, a large day does not become one very large file.

A filter on the folder column shows in the plan as `PartitionFilters`. The SQL tab of the UI records how many files the scan read:

```text
schema read back: cbd_congestion_fee:double, request_source:string, day:date
  day = 2026-06-15                             files read    3   PartitionFilters: [isnotnull(day#44), (day#44 = 2026-06-15)]
  day >= 2026-06-25                            files read   18   PartitionFilters: [isnotnull(day#44), (day#44 >= 2026-06-25)]
  to_date(pickup) = 2026-06-15                 files read   92   PartitionFilters: []
  dayofweek(day) = 1 (Sunday)                  files read   12   PartitionFilters: [isnotnull(day#44), (dayofweek(day#44) = 1)]
```

A filter prunes folders only if it uses the folder column. The filter `to_date(tpep_pickup_datetime) = '2026-06-15'` gives the same rows as `day = '2026-06-15'`. But Spark cannot know that, so it reads all 92 files. A function of the folder column is a different case. `dayofweek(day) = 1` is still a partition filter, because Spark can calculate it from the folder names alone.

Folder names are text, and Spark guesses a type for them when it reads them back:

```text
folders: ['code=001', 'code=010', 'code=100']
read back: [(1, 1), (2, 10), (3, 100)] struct<n:int,code:int>
with a schema: [(1, '001'), (2, '010'), (3, '100')]
one folder alone: ['n'] | with basePath: ['n', 'code']
```

The codes `001`, `010` and `100` came back as the integers 1, 10 and 100. Spark removes the zeros at the start. To keep the values as text, give a schema, or set `spark.sql.sources.partitionColumnTypeInference.enabled` to `false`. If you read one folder alone, the folder column is not in the result. Give the option `basePath` to keep it.

## Step 5: What `cache()` does

`cache()` keeps the result of a DataFrame in memory, so that later queries do not calculate it again. `caching.py` cleans the six months of trips and caches the result:

```python
clean = cleaned()
print("before cache():", clean.storageLevel, "| is_cached", clean.is_cached)
clean.cache()
print("after cache(): ", clean.storageLevel, "| is_cached", clean.is_cached)
print("cache() is lazy: storage entries now:", len(ui_api("storage/rdd")))
```

```text
before cache(): Serialized 1x Replicated | is_cached False
after cache():  Disk Memory Deserialized 1x Replicated | is_cached True
cache() is lazy: storage entries now: 0

the plan of a query on the cached DataFrame:
  +- InMemoryTableScan [PULocationID#7, fare_amount#10], [isnotnull(PULocationID#7), (PULocationID#7 = 132)]
  +- InMemoryRelation [tpep_pickup_datetime#1, PULocationID#7, DOLocationID#8, fare_amount#10, tip_amount#13, trip_distance#4, payment_type#9L], Storage
  +- FileScan parquet [tpep_pickup_datetime#1,trip_distance#4,PULocationID#7,DOLocationID#8,payment_type#9L,fare_amount#10,tip_amount#13] Batched: true,

first action fills the cache: 21,828,310 rows in 8.2 s
Storage tab: '*(1) Project [tpep_pickup_datetime#1, PULocationID#7, DOLoca...'
  17/17 partitions cached, memory 747 MiB, disk 0 MiB, level 'Disk Memory Deserialized 1x Replicated'
```

The output shows four facts:

- **`cache()` is lazy.** It only marks the DataFrame. The Storage tab has no entry until an action runs. The first action read the files, cleaned 21,828,310 rows, and filled the cache in 8.2 s.
- **The default level for a DataFrame is `MEMORY_AND_DISK_DESER`.** The level string is `Disk Memory Deserialized`. If a partition does not fit in memory, Spark writes it to the disk.
- **The cache is columnar and compressed.** The seven columns of 21.8 million rows used 747 MiB of memory.
- **Spark finds the cache by plan.** `InMemoryTableScan` replaces the scan. Spark applies the filter on `PULocationID` to the cached batches.

### When a cache helps, and when it does not

A cache costs time to fill and memory to hold. It helps only if the work that it saves is more than that cost:

```text
a cheap aggregate, median of 7 runs after a warm-up:
  from the cache                450 ms
  a new DataFrame, same plan    480 ms   (cache matched by plan)
  a different plan              531 ms   (reads the Parquet files)
  same plan, InMemoryTableScan: True | different plan: False

an expensive step (a shuffle into 8,951,369 route-hours), then a small query on it:
  recomputed each time         3283 ms
  the same DataFrame object, collect() again     27 ms   (skipped stages: 2)
  from a cache of the step      518 ms   (InMemoryTableScan: True)
```

For a cheap aggregate, the cache saved only 81 ms compared to a read of the Parquet files. Parquet is already columnar and compressed, and a scan of 372.6 MB on 16 cores is fast. To fill this cache took 8.2 s. Thus, the cache pays for itself only after about 100 such queries.

The second test has an expensive step: a shuffle of all trips into 8,951,369 rows, one for each route and hour. A query that calculates the step again took 3,283 ms. A cache of the step decreased this time to 518 ms.

The line in the middle is a trap. A second `collect()` on the same DataFrame object took 27 ms, because the scheduler skipped the two stages that wrote the shuffle files. The shuffle files of the first run are still on the disk, and the same DataFrame reuses them. A new DataFrame with the same plan does not reuse them. Thus, a benchmark that collects one DataFrame in a loop measures only its last stage. [Chapter 23](ch23-project-feather.md) shows the same trap in a Feather benchmark.

A cache is also a match for a new DataFrame that has the same plan. The second line ("a new DataFrame, same plan") read from the cache. Spark compares the analyzed plans, not the Python objects.

### Levels, SQL and the end of a cache

```text
after persist(DISK_ONLY) on the cached DataFrame: Disk Memory Deserialized 1x Replicated
after unpersist(): Serialized 1x Replicated | storage entries: 0

CACHE LAZY TABLE: storage entries 0 | cached partitions 0
CACHE TABLE took 4.3 s: storage entries 1 | name In-memory table clean_trips
after clearCache(): 0 entries
```

- A second `persist()` with a different level does nothing. Call `unpersist()` first.
- In SQL, `CACHE TABLE` is eager. It fills the cache before the statement returns. `CACHE LAZY TABLE` waits for the first query, like `cache()`.
- `unpersist()` removes one entry. `spark.catalog.clearCache()` removes all entries.

PySpark has fewer storage levels than Scala. There is no `MEMORY_ONLY_SER`, because Python stores its objects as serialized bytes in all cases:

```text
PySpark storage levels:
  DISK_ONLY                disk=True  memory=False offHeap=False deserialized=False replicas=1
  MEMORY_AND_DISK          disk=True  memory=True  offHeap=False deserialized=False replicas=1
  MEMORY_AND_DISK_DESER    disk=True  memory=True  offHeap=False deserialized=True  replicas=1
  MEMORY_ONLY              disk=False memory=True  offHeap=False deserialized=False replicas=1
  OFF_HEAP                 disk=True  memory=True  offHeap=True  deserialized=False replicas=1
  ...
```

### A DataFrame keeps its file list

A file DataFrame lists its files when you create it. It does not see files that arrive later:

```text
a file arrives after read.parquet():
  old DataFrame counts 3,724,889 rows; a new read counts 7,124,755
  after refreshByPath: old DataFrame counts 3,724,889
  a listed file is deleted: FAILED_READ_FILE.FILE_NOT_EXIST
```

`spark.catalog.refreshByPath` did not change the old DataFrame. It only clears cached data and metadata for the path, so that a new read sees the new files. If you delete a file that the DataFrame listed, the next action fails with `FAILED_READ_FILE.FILE_NOT_EXIST`. Thus, read the files again each time that a job runs. Do not keep a file DataFrame for a long time in a service.

!!! note "New in 4.3: an Arrow format for the cache"
    Spark 4.3 can keep the cache as Arrow batches. The static setting `spark.sql.cache.serializer=org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer` turns it on. It is a part of Project Feather, and [Chapter 23](ch23-project-feather.md) measures it.

## Step 6: The memory of an executor

An executor is one JVM. Spark divides its heap into regions, and only one region holds cached blocks and the memory that tasks use for their work. The division is in `UnifiedMemoryManager.scala`:

```mermaid
flowchart TB
  classDef res fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  classDef user fill:#6b4a9b,fill-opacity:0.72,stroke:#6b4a9b,color:#ffffff
  classDef exec fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  classDef stor fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  heap["JVM heap<br/>(spark.executor.memory)"]:::res --> reserved["reserved<br/>300 MiB"]:::res
  heap --> usable["usable = heap - 300 MiB"]:::res
  usable --> user["user memory<br/>(1 - spark.memory.fraction)<br/>your JVM objects, Spark metadata"]:::user
  usable --> unified["unified pool<br/>spark.memory.fraction = 0.6"]:::res
  unified --> exec["execution<br/>sorts, hash tables,<br/>shuffle buffers"]:::exec
  unified --> stor["storage<br/>cached blocks, broadcasts<br/>(storageFraction = 0.5, soft)"]:::stor
  exec <-.->|"execution can evict storage<br/>down to storageFraction"| stor
```

- **Reserved memory** is 300 MiB, for Spark itself.
- **The unified pool** is `spark.memory.fraction` (0.6) of the rest. Execution and storage share it. `spark.memory.storageFraction` (0.5) is a soft limit. Execution can take memory from storage and evict cached blocks, but only until storage has 0.5 of the pool. Storage can never evict execution memory.
- **User memory** is the other 0.4. Spark does not manage it. Objects that your code makes on the JVM heap use it.
- **Each task** can get between 1/(2N) and 1/N of the execution memory, if N tasks run at the same time (`ExecutionMemoryPool.scala`). If a task cannot get its minimum, it spills, or it waits.

`memory.py` starts a new JVM for each configuration, because a JVM cannot change its heap size after it starts. Then it reads the pool sizes from the executors endpoint of the REST API:

```text
configuration                   heap  (heap-300)*f  on-heap pool  off-heap  maxMemory   (MiB)
defaults                        1024         434.4         434.4         0      434.4
driver 2g                       2048        1048.8        1048.8         0     1048.8
driver 2g, off-heap 256m        2048        1048.8        1048.8       256     1304.8
driver 4g, fraction 0.8         4096        3036.8        3036.8         0     3036.8
driver 400m                   fails: [INVALID_DRIVER_MEMORY.SYSTEM_MEMORY] Insufficient driver memory: System memory 419430400 must be at least 471859200.
```

The formula agrees with the measured pool in each row. The JVM reports a heap that is a little smaller than the setting, so the formula uses the measured heap. These are the important numbers:

- **The default 1 GiB heap gives 434 MiB** to execution and storage together. In local mode, 16 tasks share it. A task can get between 13.6 MiB and 27 MiB. This is why a sort of a few hundred megabytes spills in Step 7.
- **Off-heap memory adds a second pool.** With `spark.memory.offHeap.enabled` and a size, `maxMemory` includes the off-heap pool. Spark manages this memory outside the garbage collector.
- **A heap below 450 MiB fails at start-up.** Spark requires 1.5 times the reserved memory, which is 471,859,200 bytes.

In local mode, the driver is also the executor. Thus, `spark.driver.memory` sets the size of all these pools, and `spark.executor.memory` has no effect. The scripts that cache six months of trips set `spark_driver_memory="4g"`. With the default 1 GiB heap, the cache of six months failed with `OutOfMemoryError` in one run.

## Step 7: Spill

A task **spills** when it needs more execution memory than it can get. A sort writes its sorted run to a file on the local disk, frees the memory, and continues. At the end, it merges the runs from the disk. An aggregate or a join can spill in the same way. The query gives the correct result, but it does more I/O.

`spill.py` keeps the default 1 GiB heap. It sorts all June trips by distance and writes the result to the `noop` sink. It changes the number of shuffle partitions and AQE, and it reads the spill metrics of the sort stage from the REST API:

```python
for parts, aqe in CASES:
    spark.conf.set("spark.sql.shuffle.partitions", parts)
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    ...
        trips.orderBy("trip_distance").write.format("noop").mode("overwrite").save()
    ...
    sort = max(stages, key=lambda s: s["shuffleReadBytes"])           # the stage after the shuffle
```

```text
heap: 1024 MiB; storage+execution pool: 434 MiB

sort all June trips by trip_distance, write to noop; median of 3 runs after a warm-up

shuffle.partitions   AQE  sort tasks  memory spilled  disk spilled  shuffle write    time
                 2 false           2         370 MiB        98 MiB        197 MiB    5.4s
                16 false          16         391 MiB       104 MiB        188 MiB    3.8s
               200 false         200           0 MiB         0 MiB        184 MiB    3.7s
                 2  true           2         555 MiB       148 MiB        197 MiB    4.7s
               200  true          17         368 MiB        98 MiB        184 MiB    3.8s
```

There are two spill metrics, and they measure different things:

- **Memory spilled** (`memoryBytesSpilled`) is the size of the spilled data in memory, before Spark writes it.
- **Disk spilled** (`diskBytesSpilled`) is the size of the same data on the disk, after serialization and compression.

The numbers show how the partition count controls spill:

- **2 partitions:** each sort task holds about half of the shuffle (197 MiB written). The pool is 434 MiB, and a task can get at most half of it while the other task runs. The tasks spilled 370 MiB.
- **16 partitions:** each task holds less data, but each task also gets less memory, between 1/32 and 1/16 of the pool. The tasks still spilled.
- **200 partitions:** each task holds about 1 MiB, which fits. Nothing spilled.
- **200 partitions with AQE:** AQE coalesced the 200 partitions into 17, because each partition was much smaller than the advisory size of 64 MB. The larger partitions spilled again, 368 MiB.

The last row is important. AQE coalescing selects partition sizes from `advisoryPartitionSizeInBytes`, not from the memory of each task. On executors with little memory for each core, a coalesced partition of 64 MB can spill. If a stage spills after a coalesce, decrease `spark.sql.adaptive.advisoryPartitionSizeInBytes`, or give each task more memory (more heap, or fewer cores for each executor).

The time cost of the spill was small here, because the local disk is fast and the page cache holds the spill files. The sort with 2 tasks took 5.4 s, and the sort with 200 tasks took 3.7 s. Most of the difference is parallelism, not spill. On a cluster with slow disks, spill costs more.

```mermaid
flowchart LR
  classDef mem fill:#3a5f96,fill-opacity:0.72,stroke:#3a5f96,color:#ffffff
  classDef disk fill:#c0661a,fill-opacity:0.72,stroke:#c0661a,color:#ffffff
  rows["rows of one<br/>shuffle partition"]:::mem --> buf["sort buffer<br/>(execution memory)"]:::mem
  buf -->|"buffer full and no<br/>more memory"| run1["spill file 1<br/>(sorted run)"]:::disk
  buf -->|"again"| run2["spill file 2"]:::disk
  buf -->|"end of input"| last["last run<br/>in memory"]:::mem
  run1 --> merge["merge of<br/>all runs"]:::mem
  run2 --> merge
  last --> merge
  merge --> out["sorted output"]:::mem
```

## Step 8: Very large files

The memory model has a hard consequence for large binary files. The `binaryFile` source reads each file as one row, with the bytes in a `content` column. Spark cannot split a binary file, and it reads the whole file into one value. `blobs.py` makes a sparse file of 2.5 GB, which uses no disk blocks, and reads it:

```text
huge.bin: apparent size 2,500,000,000 bytes, blocks on disk 0
schema: struct<path:string,modificationTime:timestamp,length:bigint,content:binary>
select(path, length): [('huge.bin', 2500000000)]
count(): 1
select(content): [FAILED_READ_FILE.NO_HINT] Caused by: org.apache.spark.SparkException: The length of file:huge.bin is 2500000000, which exceeds the max length allowed: 2147483647.
with maxLength 200 MiB, f300.bin: The length of f300.bin is 314572800, which exceeds the max length allowed: 209715200.
```

The metadata columns are cheap. `select(path, length)` and `count()` succeeded, because `BinaryFileFormat` reads the file content only when the query needs the `content` column. `select(content)` failed. A Spark binary value holds at most 2,147,483,647 bytes (2 GiB minus one byte), and `spark.sql.sources.binaryFile.maxLength` has that value by default. A lower `maxLength` makes the read fail early, with a clear message, for a file above your own limit.

The real limit is much lower, and it comes from the memory model of Step 6:

```text
a fresh driver with the default 1g heap, one file each:
  f100.bin (100 MiB): exit code 0; read 104,857,600 bytes
  f300.bin (300 MiB): exit code 1; java.lang.OutOfMemoryError: Java heap space
      at org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder.grow(BufferHolder.java:85)
      at org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat$.$anonfun$parse$2(BinaryFileFormat.s
      at org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat$.$anonfun$parse$2$adapted(BinaryFile
```

A driver with the default 1 GiB heap read a 100 MiB file. It failed with `OutOfMemoryError` on a 300 MiB file.

The stack trace shows where: `BufferHolder.grow` makes the row buffer larger when the value does not fit. A larger buffer is a new array, and the old array stays until the copy is complete. Thus, one 300 MiB value needs several times its size for a short time. All of this memory is user memory, outside the unified pool. The 16 task slots also share the same heap.

Rules for large files:

- Select only the metadata columns (`path`, `length`, `modificationTime`) to make an inventory. That costs almost nothing.
- Do not put a file that is larger than a small fraction of the executor heap into one row. Split it at the source, or read it in parts with your own reader. [Part IX](ch50-python-data-sources-readers.md) builds such a reader as a Python data source.
- Spark 4.3 can read entries inside zip and tar archives as separate files. [Chapter 11](ch11-files-as-files.md) shows it.

## Step 9: A walk through the Spark UI

The REST API gave all the numbers in this chapter. The web UI shows the same data, and it is the faster tool when you do not know yet where to look. `ui_tour.py` runs three labelled actions on the default 1 GiB heap, and then it keeps the application alive. `film/ch19_ui.py` records a browser while it visits the UI pages. The workload:

```python
sc.setJobDescription("1. sort six months by distance, 2 partitions, AQE off")
spark.conf.set("spark.sql.shuffle.partitions", "2")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.read.parquet(str(TAXI / "yellow-h1")).orderBy("trip_distance").write.format("noop").mode("overwrite").save()
...
sc.setJobDescription("2. cache the cleaned June trips")
...
sc.setJobDescription("3. tips by pickup zone, from the cache")
```

What the REST API reported for the same run:

```text
UI on port 4138
  job  0  SUCCEEDED  stages 1  tasks   1  1. sort six months by distance, 2 partitions, AQE 
  job  1  SUCCEEDED  stages 1  tasks  17  1. sort six months by distance, 2 partitions, AQE 
  job  2  SUCCEEDED  stages 2  tasks  19  1. sort six months by distance, 2 partitions, AQE 
  job  3  SUCCEEDED  stages 1  tasks   1  2. cache the cleaned June trips
  job  4  SUCCEEDED  stages 1  tasks  16  2. cache the cleaned June trips
  job  5  SUCCEEDED  stages 1  tasks  16  2. cache the cleaned June trips
  job  6  SUCCEEDED  stages 2  tasks  17  2. cache the cleaned June trips
  job  7  SUCCEEDED  stages 1  tasks  16  3. tips by pickup zone, from the cache
  job  8  SUCCEEDED  stages 2  tasks  17  3. tips by pickup zone, from the cache
  stage  0  tasks   1  spill memory      0 MiB  disk     0 MiB  1. sort six months by distance, 2 partit
  stage  1  tasks  17  spill memory      0 MiB  disk     0 MiB  1. sort six months by distance, 2 partit
  stage  2  tasks  17  spill memory      0 MiB  disk     0 MiB  1. sort six months by distance, 2 partit
  stage  3  tasks   2  spill memory  3,520 MiB  disk   971 MiB  1. sort six months by distance, 2 partit
  stage  4  tasks   1  spill memory      0 MiB  disk     0 MiB  2. cache the cleaned June trips
  stage  5  tasks  16  spill memory      0 MiB  disk     0 MiB  2. cache the cleaned June trips
  stage  6  tasks  16  spill memory      0 MiB  disk     0 MiB  2. cache the cleaned June trips
  stage  8  tasks   1  spill memory      0 MiB  disk     0 MiB  2. cache the cleaned June trips
  stage  9  tasks  16  spill memory      0 MiB  disk     0 MiB  3. tips by pickup zone, from the cache
  stage 11  tasks   1  spill memory      0 MiB  disk     0 MiB  3. tips by pickup zone, from the cache
the stage that spilled: 3, 2 tasks, spill memory 3,520 MiB, disk 971 MiB
storage: 16/16 partitions, 95 MiB in memory, level Disk Memory Deserialized 1x Replicated
SQL execution 2 ('3. tips by pickup zone, from the cache'), jobs [7, 8]
```

### Jobs, and a stage that spilled

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch19-ui-stages.webp">
    <source src="../../media/clips/ch19-ui-stages.mp4" type="video/mp4">
  </video>
  <figcaption>The Jobs tab, then the detail page of the sort stage. The summary metrics show the spill of each of the two tasks.</figcaption>
</figure>

- **The Jobs tab** lists each job with its description, so `setJobDescription` is the easiest way to find your own work in a busy application. Each action made more than one job, as [Chapter 1](ch01-how-spark-runs.md) explained.
- **The stage detail page** has the totals at the top: `Spill (Memory)` and `Spill (Disk)`. The two sort tasks spilled 3.4 GiB in memory, which was 971 MiB on the disk. The **Summary Metrics** table gives the minimum, the quartiles and the maximum of each metric over the tasks. Use it to find skew. If the maximum duration is much larger than the median, one task did much more work than the others.

### Storage and the plan of a query

<figure markdown>
  <video autoplay loop muted playsinline controls width="1280" height="720" poster="../../media/clips/ch19-ui-storage-sql.webp">
    <source src="../../media/clips/ch19-ui-storage-sql.mp4" type="video/mp4">
  </video>
  <figcaption>The Storage tab with the cached June trips, then the plan graph of the query that read the cache.</figcaption>
</figure>

- **The Storage tab** shows each cache: its plan, its storage level, how many partitions it holds, and their size in memory and on the disk. A fraction cached below 100% means that some partitions did not fit and Spark must calculate them again.
- **The SQL / DataFrame tab** shows each query as a graph of physical operators, with metrics on each node. This query starts at `InMemoryTableScan`, so it read the cache and not the files. The graph shows the final plan after AQE, and it is the easiest way to see what AQE did ([Chapter 18](ch18-query-plans.md), Step 7).

The other tabs complete the picture. **Executors** shows the memory pools of Step 6, and the GC time and the shuffle bytes of each executor. **Environment** shows each setting that the application started with. The 4.3 UI marks the settings that are not at their default values.

## The whole chapter, on one page

```python
# Partitions: predict them, then check.
df = spark.read.parquet(path)
df.rdd.getNumPartitions()                                   # Classic
df.select(F.spark_partition_id()).distinct().count()        # Classic and Connect: partitions with rows

# Change them.
df.repartition(200, "key")          # shuffle, hash on key
df.repartitionByRange(8, "ts")      # shuffle, sorted ranges
df.coalesce(8)                      # no shuffle: merges partitions, and the work before it
df.repartition(1).write.parquet(out)   # one file, without one task for all the work

# Layout on disk.
df.repartition("day").write.partitionBy("day").option("maxRecordsPerFile", 1_000_000).parquet(out)

# Cache only what is expensive and used again; remove it after.
heavy = expensive_step(df).cache(); heavy.count()
...
heavy.unpersist()

# Static settings go to the builder, before the first session starts.
SparkSession.builder.config("spark.driver.memory", "4g").config("spark.memory.fraction", "0.6")
```

- **Scan partitions come from the file sizes**, through `maxPartitionBytes`, `openCostInBytes` and the default parallelism. Parquet tasks only do work if they hold the midpoint of a row group. gzip files do not split.
- **`coalesce(1)` puts the work before it into one task.** `repartition(1)` keeps that work parallel and costs one shuffle.
- **A layout on disk sets later reads.** `partitionBy` lets a filter on the folder column skip folders. `repartition` on the same column first keeps the file count low.
- **A cache helps only for expensive work used again.** It is lazy, Spark finds it by plan, and a second action on the same DataFrame can reuse shuffle files without any cache.
- **Execution and storage share `(heap - 300 MiB) x 0.6`.** Each of N tasks gets between 1/(2N) and 1/N of the execution memory.
- **Spill is the sign of too little memory for each task.** More partitions, a smaller advisory size for AQE, or more memory for each core removes it.
- **A binary value must fit in the heap several times.** Read metadata columns for an inventory, and keep large files out of single rows.
- **The UI shows all of it**: spill and skew on the stage page, caches on the Storage tab, the final plan on the SQL tab.

## Further reading

- [Tuning Spark](https://spark.apache.org/docs/4.3.0/tuning.html), with the memory management overview.
- [Performance tuning for Spark SQL](https://spark.apache.org/docs/4.3.0/sql-performance-tuning.html): caching, partition hints, AQE.
- [The Web UI guide](https://spark.apache.org/docs/4.3.0/web-ui.html), page by page.
- The 4.3.0 source: `FilePartition.scala` (split sizes), `UnifiedMemoryManager.scala` and `ExecutionMemoryPool.scala` (the pools), `BinaryFileFormat.scala` (the binary reader).
