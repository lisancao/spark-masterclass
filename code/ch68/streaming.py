"""Chapter 68: a stream reader over a growing folder and an idempotent stream writer, with checkpoints.

Run from code/:  uv run python ch68/streaming.py
"""
import json, os, re, shutil, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql.datasource import (DataSource, DataSourceStreamReader, DataSourceStreamWriter,
                                    InputPartition, SimpleDataSourceStreamReader, WriterCommitMessage)
from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxFiles, SupportsTriggerAvailableNow
from common.book import session, MESSY

SCHEMA = "name string, kind string, size long, mtime_ns long"
KINDS = {".eml": "email", ".pdf": "pdf", ".md": "text", ".png": "image", ".wav": "audio"}
BASE = tempfile.mkdtemp(prefix="ch68-stream-")
CALLS = os.path.join(BASE, "calls.log")


def note(line):
    with open(CALLS, "a") as f:
        f.write(line + "\n")


def take_calls():
    if not os.path.exists(CALLS):
        return []
    lines = open(CALLS).read().splitlines()
    os.remove(CALLS)
    return lines


def list_inbox(root):
    """Every visible file as a (mtime_ns, name) key, oldest first."""
    return sorted((e.stat().st_mtime_ns, e.name) for e in os.scandir(root)
                  if e.is_file() and not e.name.startswith((".", "_")))


def key_of(offset):
    return (offset["mtime_ns"], offset["name"])


def offset_of(key):
    return {"v": 1, "mtime_ns": key[0], "name": key[1]}


def short(offset):
    return offset["name"] or "(start)"


class FilePartition(InputPartition):
    def __init__(self, name):
        self.name = name


class InboxStreamReader(DataSourceStreamReader):
    def __init__(self, options):
        self.root = options["path"]
        self.max_files = int(options.get("maxFilesPerTrigger", "2"))
        self.target = None

    def initialOffset(self):
        note("initialOffset()")
        return offset_of((0, ""))

    def getDefaultReadLimit(self):
        return ReadMaxFiles(self.max_files)

    def latestOffset(self, start, limit):
        new = [k for k in list_inbox(self.root) if k > key_of(start)]
        if self.target is not None:
            new = [k for k in new if k <= self.target]
        if not isinstance(limit, ReadAllAvailable):
            new = new[:limit.max_files]
        end = offset_of(new[-1]) if new else start          # end == start: no new batch
        note(f"latestOffset({short(start)}, {type(limit).__name__}) -> {short(end)}")
        return end

    def partitions(self, start, end):
        lo, hi = key_of(start), key_of(end)
        parts = [FilePartition(n) for (t, n) in list_inbox(self.root) if lo < (t, n) <= hi]
        note(f"partitions({short(start)}, {short(end)}) -> {len(parts)}")
        return parts

    def read(self, partition):
        st = os.stat(os.path.join(self.root, partition.name))
        kind = KINDS.get(os.path.splitext(partition.name)[1].lower(), "other")
        yield (partition.name, kind, st.st_size, st.st_mtime_ns)

    def commit(self, end):
        note(f"reader.commit({short(end)})")


class InboxAvailableNowReader(InboxStreamReader, SupportsTriggerAvailableNow):
    def prepareForTriggerAvailableNow(self):
        keys = list_inbox(self.root)                        # freeze "now" for this run
        self.target = keys[-1] if keys else (0, "")
        note(f"prepareForTriggerAvailableNow() -> {self.target[1]}")


class Staged(WriterCommitMessage):
    def __init__(self, file, rows):
        self.file, self.rows = file, rows


class BatchFolderWriter(DataSourceStreamWriter):
    """Publishes micro-batch N as the folder batch=N, and skips a batch id it already published."""
    def __init__(self, options):
        self.root = options["path"]
        self.staging = os.path.join(self.root, "_staging")

    def write(self, rows):
        import uuid
        os.makedirs(self.staging, exist_ok=True)
        name = f"{uuid.uuid4().hex}.jsonl"
        count = 0
        with open(os.path.join(self.staging, name), "w") as f:
            for row in rows:
                f.write(json.dumps(row.asDict()) + "\n")
                count += 1
        return Staged(name, count)

    def commit(self, messages, batchId):
        target = os.path.join(self.root, f"batch={batchId:05d}")
        if os.path.isdir(target):                          # a replay of a published batch
            for m in messages:
                os.remove(os.path.join(self.staging, m.file))
            note(f"writer.commit(batch {batchId}): already published, replay skipped")
            return
        tmp = os.path.join(self.root, f"_tmp-batch={batchId:05d}")
        shutil.rmtree(tmp, ignore_errors=True)               # what a crash in an earlier try left
        os.makedirs(tmp)
        for m in messages:
            staged = os.path.join(self.staging, m.file)
            if m.rows:
                os.replace(staged, os.path.join(tmp, m.file))
            else:
                os.remove(staged)
        with open(os.path.join(tmp, "_SUMMARY.json"), "w") as f:
            json.dump({"batch": batchId, "rows": sum(m.rows for m in messages)}, f)
        os.replace(tmp, target)                            # one rename publishes the whole batch
        note(f"writer.commit(batch {batchId}): {sum(m.rows for m in messages)} rows published")

    def abort(self, messages, batchId):
        for m in messages:
            if m is not None and os.path.exists(os.path.join(self.staging, m.file)):
                os.remove(os.path.join(self.staging, m.file))


class InboxDataSource(DataSource):
    @classmethod
    def name(cls):
        return "inbox"

    def schema(self):
        return SCHEMA

    def streamReader(self, schema):
        cls = InboxAvailableNowReader if self.options.get("availableNow", "true") == "true" \
            else InboxStreamReader
        return cls(self.options)

    def streamWriter(self, schema, overwrite):
        return BatchFolderWriter(self.options)


spark = session("ch68-streaming", spark_sql_session_timeZone="UTC")
spark.dataSource.register(InboxDataSource)
inbox = os.path.join(BASE, "inbox")
os.makedirs(inbox)
T0 = 1_790_000_000_000_000_000          # a fixed time in nanoseconds, so runs repeat exactly


def drop(src, name, second):
    dst = os.path.join(inbox, name)
    shutil.copy(src, dst)
    t = T0 + second * 1_000_000_000
    os.utime(dst, ns=(t, t))


def run(sink, checkpoint, trigger, **opts):
    reader = spark.readStream.format("inbox").option("path", inbox).option("maxFilesPerTrigger", 3)
    for k, v in opts.items():
        reader = reader.option(k, v)
    q = (reader.load().writeStream.format("inbox").option("path", sink)
         .option("checkpointLocation", checkpoint).trigger(**trigger).start())
    if "availableNow" in trigger:
        q.awaitTermination()
    else:
        q.processAllAvailable()
        q.stop()
    return [(p.batchId, p.numInputRows) for p in q.recentProgress if p.numInputRows]


for i in range(4):
    drop(MESSY / "mail" / f"ticket_{i}.eml", f"ticket_{i}.eml", i)
sink, cp = os.path.join(BASE, "sink"), os.path.join(BASE, "checkpoint")

# 1. First run: Trigger.AvailableNow, at most 3 files per batch.
take_calls()
print("run 1, availableNow:", run(sink, cp, {"availableNow": True}))
for line in take_calls():
    print("  ", line)

# 2. The checkpoint.
print("\ncheckpoint folders:", sorted(d for d in os.listdir(cp) if not d.startswith(".")))
print("offsets/:", sorted(f for f in os.listdir(os.path.join(cp, "offsets")) if not f.startswith(".")),
      " commits/:", sorted(f for f in os.listdir(os.path.join(cp, "commits")) if not f.startswith(".")))
print("offsets/1:")
for line in open(os.path.join(cp, "offsets", "1")).read().splitlines():
    print("  ", line if len(line) < 86 else line[:83] + "...")

# 3. New files, a restart with a processing-time trigger: the stream goes on from batch 2.
drop(MESSY / "pdf" / "invoice_1001.pdf", "invoice_1001.pdf", 10)
for j, doc in enumerate(["returns", "shipping", "privacy"]):
    drop(MESSY / "docs" / f"{doc}.md", f"{doc}.md", 11 + j)
print("\nrun 2, restart, processingTime:", run(sink, cp, {"processingTime": "0 seconds"}))
take_calls()

# 4. Replay batch 3: remove its commit record, then restart.
os.remove(os.path.join(cp, "commits", "3"))
if os.path.exists(os.path.join(cp, "commits", ".3.crc")):
    os.remove(os.path.join(cp, "commits", ".3.crc"))
print("run 3, after deleting commits/3:", run(sink, cp, {"availableNow": True}))
for line in take_calls():
    if "writer.commit" in line:
        print("  ", line)
rows = spark.read.json(os.path.join(sink, "batch=*", "*.jsonl"))
print("sink:", rows.count(), "rows,", rows.select("name").distinct().count(), "distinct files,",
      "folders", sorted(d for d in os.listdir(sink) if d.startswith("batch=")))

# 5. Without SupportsTriggerAvailableNow, AvailableNow reads the whole backlog in one batch.
print("\nthe same 8 files, a new checkpoint:")
print("  with SupportsTriggerAvailableNow:   ",
      run(os.path.join(BASE, "s2"), os.path.join(BASE, "c2"), {"availableNow": True}))
print("  without SupportsTriggerAvailableNow:",
      run(os.path.join(BASE, "s3"), os.path.join(BASE, "c3"), {"availableNow": True}, availableNow="false"))
take_calls()

# 6. Real-time mode (new in 4.3) and a Python stream reader: our sink, then the console sink.
for sink_fmt in ("inbox", "console"):
    try:
        q = (spark.readStream.format("inbox").option("path", inbox).load()
             .writeStream.format(sink_fmt).outputMode("update").option("path", os.path.join(BASE, "s4"))
             .option("checkpointLocation", os.path.join(BASE, f"c4-{sink_fmt}"))
             .trigger(realTime="5 seconds").start())
        q.awaitTermination(15)
        q.stop()
        print(f"\nrealTime trigger, {sink_fmt} sink: ran;", "exception:", q.exception())
    except Exception as e:
        text = " ".join(str(e).split())
        found = list(re.finditer(r"\[([A-Z_.]+)\] ([^[]*?)(?= SQLSTATE|\[|$)", text))
        m = found[-1] if found else None            # the innermost condition
        print(f"\nrealTime trigger, {sink_fmt} sink:", m.group(1) if m else type(e).__name__)
        import textwrap
        print(textwrap.fill(m.group(2) if m else text[:300], 84, initial_indent="   ",
                            subsequent_indent="   ", max_lines=3))


# 7. SimpleDataSourceStreamReader: read() returns the rows and the next offset in one call.
class Counter(SimpleDataSourceStreamReader):
    def __init__(self, options):
        self.stuck = options.get("stuck") == "true"

    def initialOffset(self):
        return {"n": 0}

    def read(self, start):
        n = start["n"]
        if n >= 12:
            return iter([]), start                  # the end of this demo stream: no new batch
        if self.stuck:
            return iter([(n,)]), {"n": n}          # rows, but the offset does not move
        return iter([(i,) for i in range(n, n + 4)]), {"n": n + 4}

    def readBetweenOffsets(self, start, end):
        return iter([(i,) for i in range(start["n"], end["n"])])


class CounterSource(DataSource):
    @classmethod
    def name(cls):
        return "simplecounter"

    def schema(self):
        return "n int"

    def simpleStreamReader(self, schema):
        return Counter(self.options)


spark.dataSource.register(CounterSource)
for stuck in ("false", "true"):
    q = (spark.readStream.format("simplecounter").option("stuck", stuck).load()
         .writeStream.format("memory").queryName(f"counter_{stuck}")
         .option("checkpointLocation", os.path.join(BASE, f"cc-{stuck}")).start())
    try:
        q.processAllAvailable()
        q.stop()
        print(f"\nsimple reader, stuck={stuck}: batches",
              [(p.batchId, p.numInputRows) for p in q.recentProgress][:3], "...")
    except Exception as e:
        q.stop()
        conds = re.findall(r"\[([A-Z_]+)\]", str(e))
        print(f"simple reader, stuck={stuck}:", next((c for c in conds if "SIMPLE" in c), conds[:2]))
