"""Chapter 82: batch writers: who runs what, save modes, a manifest commit, abort, retries, Arrow batches.

Run from code/:  uv run python ch82/writers.py
                 SPARK_MASTER='local[4,2]' uv run python ch82/writers.py --retry   (task retries)
"""
import json, os, re, shutil, sys, tempfile, uuid
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.datasource import (DataSource, DataSourceArrowWriter, DataSourceWriter,
                                    WriterCommitMessage)
from common.book import session, MESSY

MANIFEST = "_MANIFEST.json"
LOG_PATTERN = r'^(\S+) \S+ \S+ \[[^\]]+\] "GET (\S+?)(?:\?\S*)? HTTP/1\.1" (\d{3}) (\d+)'
BASE = tempfile.mkdtemp(prefix="ch82-writers-")
TRACE = os.path.join(BASE, "trace.log")


def trace(what):
    """One line per call: what ran, in which process, for which task attempt."""
    from pyspark import TaskContext
    ctx = TaskContext.get()
    where = f"task {ctx.partitionId()} attempt {ctx.attemptNumber()}" if ctx else "no task"
    with open(TRACE, "a") as f:
        f.write(f"{what:<38} pid {os.getpid():<8} {where}\n")


def take_trace():
    if not os.path.exists(TRACE):
        return []
    lines = open(TRACE).read().splitlines()
    os.remove(TRACE)
    return lines


def read_manifest(root):
    path = os.path.join(root, MANIFEST)
    if not os.path.exists(path):
        return {"files": [], "rows": 0, "commits": 0, "last_mode": None}
    with open(path) as f:
        return json.load(f)


def write_manifest(root, manifest):
    """Replace the manifest in one step: write a hidden temporary file, then rename it."""
    tmp = os.path.join(root, f".{MANIFEST}.{uuid.uuid4().hex}")
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=1)
    os.replace(tmp, os.path.join(root, MANIFEST))


class FileWritten(WriterCommitMessage):
    """What one task tells the driver: the file it staged and its row count."""
    def __init__(self, file, rows):
        self.file, self.rows = file, rows


class JsonLogDataSource(DataSource):
    """Write-only: no schema(), no reader()."""
    @classmethod
    def name(cls):
        return "jsonlog"

    def writer(self, schema, overwrite):
        return JsonLogWriter(self.options, overwrite)


class JsonLogWriter(DataSourceWriter):
    def __init__(self, options, overwrite):
        trace("JsonLogWriter.__init__")
        if "path" not in options:
            raise ValueError("jsonlog needs a path: .save(path) or .option('path', ...)")
        self.root = options["path"]
        self.overwrite = overwrite
        self.job = uuid.uuid4().hex[:8]                    # one id for the whole job, pickled to every task
        self.staging = os.path.join(self.root, "_staging", self.job)
        self.fail_on = options.get("fail_on")              # a path that makes a task fail, for the demo

    def write(self, rows):
        from pyspark import TaskContext
        ctx = TaskContext.get()
        trace("JsonLogWriter.write")
        os.makedirs(self.staging, exist_ok=True)
        # The attempt number is in the name, so two attempts of one task never share a file.
        name = f"part-{ctx.partitionId():05d}-{self.job}-a{ctx.attemptNumber()}.jsonl"
        count = 0
        with open(os.path.join(self.staging, name), "w") as f:
            for row in rows:
                if self.fail_on and row.path == self.fail_on and ctx.attemptNumber() == 0:
                    raise RuntimeError(f"cannot write {row.path}")
                f.write(json.dumps(row.asDict()) + "\n")
                count += 1
        return FileWritten(name, count)

    def commit(self, messages):
        staged = len(os.listdir(self.staging))
        trace(f"JsonLogWriter.commit: {len(messages)} msgs, {staged} staged")
        old = read_manifest(self.root)
        for m in messages:                                  # 1. move the staged files into place
            os.replace(os.path.join(self.staging, m.file), os.path.join(self.root, m.file))
        new_files, new_rows = [m.file for m in messages], sum(m.rows for m in messages)
        files = new_files if self.overwrite else old["files"] + new_files
        rows = new_rows if self.overwrite else old["rows"] + new_rows
        write_manifest(self.root, {"files": files, "rows": rows, "commits": old["commits"] + 1,
                                   "last_mode": "overwrite" if self.overwrite else "append"})   # 2. publish
        if self.overwrite:                                  # 3. only now delete what it replaced
            for name in old["files"]:
                path = os.path.join(self.root, name)
                if os.path.exists(path):
                    os.remove(path)
        shutil.rmtree(self.staging, ignore_errors=True)    # 4. remove the staging folder

    def abort(self, messages):
        trace(f"JsonLogWriter.abort({messages!r})")
        left = sorted(os.listdir(self.staging)) if os.path.isdir(self.staging) else []
        with open(os.path.join(BASE, "abort.json"), "w") as f:
            json.dump({"messages": [m is None for m in messages], "staged_files": len(left)}, f)
        shutil.rmtree(self.staging, ignore_errors=True)


class TracedArrowWriter(DataSourceArrowWriter):
    def __init__(self, options):
        self.root = options["path"]

    def write(self, batches):
        """`batches` is an iterator of pyarrow.RecordBatch, not of Row."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        from pyspark import TaskContext
        batches = list(batches)
        sizes = [b.num_rows for b in batches]
        task = TaskContext.get().partitionId()
        if not batches:
            return FileWritten(None, 0)
        os.makedirs(self.root, exist_ok=True)
        name = f"part-{task:05d}.parquet"
        pq.write_table(pa.Table.from_batches(batches), os.path.join(self.root, name))
        trace(f"arrow write: batches {sizes}")
        return FileWritten(name, sum(sizes))

    def commit(self, messages):
        write_manifest(self.root, {"files": [m.file for m in messages if m.file],
                                   "rows": sum(m.rows for m in messages), "commits": 1,
                                   "last_mode": "arrow"})


class ArrowParquetDataSource(DataSource):
    @classmethod
    def name(cls):
        return "arrowparquet"

    def writer(self, schema, overwrite):
        return TracedArrowWriter(self.options)


def load_requests(spark):
    """The access log as (ip, path, status, bytes), spread over 3 tasks."""
    lines = spark.read.text(str(MESSY / "logs" / "access.log"))
    return lines.select(
        F.regexp_extract("value", LOG_PATTERN, 1).alias("ip"),
        F.regexp_extract("value", LOG_PATTERN, 2).alias("path"),
        F.regexp_extract("value", LOG_PATTERN, 3).cast("int").alias("status"),
        F.regexp_extract("value", LOG_PATTERN, 4).cast("long").alias("bytes"),
    ).repartition(3)


def condition(e):
    get = getattr(e, "getCondition", None)
    return (get() if get else None) or type(e).__name__


def first_message(e):
    m = re.search(r"\] (.*?)(?: SQLSTATE|$)", str(e).splitlines()[0])
    return (m.group(1) if m else str(e).splitlines()[0])[:84]


spark = session("ch82-writers", spark_sql_session_timeZone="UTC")
spark.dataSource.register(JsonLogDataSource)
spark.dataSource.register(ArrowParquetDataSource)
requests = load_requests(spark).cache()
out = os.path.join(BASE, "requests")


def disk():
    """Visible part files next to the manifest, and the files left in _staging."""
    part = sorted(f for f in os.listdir(out) if f.startswith("part-")) if os.path.isdir(out) else []
    st = os.path.join(out, "_staging")
    staged = sum(len(fs) for _, _, fs in os.walk(st)) if os.path.isdir(st) else 0
    return part, staged


def summary():
    m = read_manifest(out)
    part, staged = disk()
    return (f"manifest rows {m['rows']:>3}, files {len(m['files'])}, commits {m['commits']}, "
            f"part files on disk {len(part)}, staged {staged}")


if "--retry" in sys.argv:
    # local[4,2]: two attempts per task. The first attempt of one task fails, the second succeeds.
    print("master:", spark.sparkContext.master)
    requests.write.format("jsonlog").mode("append").option("fail_on", "/login").save(out)
    for line in take_trace():
        print(" ", line.split(" pid ")[0].rstrip(), "|", line.split(None, 3)[-1].split("  ")[-1].strip())
    m = read_manifest(out)
    print("manifest files:", m["files"])
    print(summary())
    sys.exit(0)

print("requests:", requests.count(), "rows in", requests.rdd.getNumPartitions(), "partitions\n")

# 1. Who runs what.
take_trace()
requests.write.format("jsonlog").mode("append").save(out)
print("driver (this script)                   pid", os.getpid())
for line in take_trace():
    print(line)
print("after append 1:", summary())

# 2. Append again, then overwrite.
requests.write.format("jsonlog").mode("append").save(out)
print("after append 2:", summary())
print("Spark's JSON reader on the folder:", spark.read.json(out).count(), "rows")
requests.write.format("jsonlog").mode("overwrite").save(out)
print("after overwrite:", summary())
print("files on disk:", sorted(os.listdir(out)))
take_trace()

# 3. A failing job: the first task that sees /login raises.
try:
    requests.write.format("jsonlog").mode("append").option("fail_on", "/login").save(out)
except Exception as e:
    print("\nfailed write:", condition(e))
abort = json.load(open(os.path.join(BASE, "abort.json")))
print("abort(): messages that are None:", abort["messages"], "| staged files before cleanup:",
      abort["staged_files"])
print("after the failed append:", summary())
for line in take_trace():
    if "abort" in line or "commit" in line:
        print(" ", line.split(" pid ")[0].rstrip())

# 4. What Spark refuses.
print()
for label, w in [("mode error (the default)", requests.write.format("jsonlog")),
                 ("mode ignore", requests.write.format("jsonlog").mode("ignore"))]:
    try:
        w.save(out)
        print(f"{label:<26} accepted")
    except Exception as e:
        print(f"{label:<26} {condition(e)}")


class NoMessage(JsonLogWriter):
    def write(self, rows):
        for _ in rows:
            pass
        return None                                         # not a WriterCommitMessage


class NoMessageSource(JsonLogDataSource):
    @classmethod
    def name(cls):
        return "nomessage"

    def writer(self, schema, overwrite):
        return NoMessage(self.options, overwrite)


class ReadOnly(DataSource):
    @classmethod
    def name(cls):
        return "readonly"

    def schema(self):
        return "x int"


spark.dataSource.register(NoMessageSource)
spark.dataSource.register(ReadOnly)
for label, fmt in [("write() returns None", "nomessage"), ("source without writer()", "readonly")]:
    try:
        requests.write.format(fmt).mode("append").save(os.path.join(BASE, fmt))
        print(f"{label:<26} accepted")
    except Exception as e:
        cond = condition(e)
        inner = re.findall(r"\[(DATA_SOURCE_[A-Z_]+|NOT_IMPLEMENTED)\]", str(e))
        print(f"{label:<26} {cond}" + (f" / {inner[-1]}" if inner and inner[-1] != cond else ""))
take_trace()

# 5. The Arrow writer: batches of spark.sql.execution.arrow.maxRecordsPerBatch rows.
for size in ("10000", "50"):
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", size)
    pq_out = os.path.join(BASE, f"parquet-{size}")
    requests.write.format("arrowparquet").mode("append").save(pq_out)
    print(f"\nmaxRecordsPerBatch={size}:")
    for line in sorted(take_trace(), key=lambda l: l.split()[-3]):
        print(" ", line.split(" pid ")[0].rstrip(), "|", line.split("pid")[1].split(None, 1)[1])
back = spark.read.parquet(pq_out)
print("Parquet back:", back.count(), "rows;", dict(sorted((r[0], r[1]) for r in back.groupBy("status").count().collect())))
