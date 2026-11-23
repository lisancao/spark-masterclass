"""Chapter 65: a parallel reader for a custom format: one row per file, header bytes parsed in tasks.

Run from code/:  uv run python ch65/docs_source.py
"""
import datetime, os, shutil, struct, sys, tempfile, wave
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from common.book import session, plan, MESSY

spark = session("ch65-docs")

KINDS = {".png": "image", ".wav": "audio", ".pdf": "pdf", ".eml": "email", ".md": "text",
         ".txt": "text", ".log": "log", ".json": "json", ".xml": "xml"}
HEADER_COLUMNS = {"width", "height", "seconds"}
SCHEMA = ("path string, name string, kind string, size long, modified timestamp, "
          "width int, height int, seconds double")


def kind_of(path):
    return KINDS.get(os.path.splitext(path)[1].lower(), "other")


def list_files(root, pattern):
    """Absolute, sorted (path, size) pairs. stat() reads metadata, not the file."""
    return sorted((str(p.resolve()), p.stat().st_size) for p in Path(root).glob(pattern) if p.is_file())


def plan_files(files, n):
    """Largest file first, each into the group with the smallest total so far."""
    groups = [[] for _ in range(max(n, 1))]
    totals = [0] * len(groups)
    for path, size in sorted(files, key=lambda f: (-f[1], f[0])):
        i = totals.index(min(totals))
        groups[i].append(path)
        totals[i] += size
    return [g for g in groups if g]


def round_robin(files, n):
    groups = [[] for _ in range(n)]
    for i, (path, _) in enumerate(files):
        groups[i % n].append(path)
    return groups


def png_size(head):
    if head[:8] == b"\x89PNG\r\n\x1a\n" and head[12:16] == b"IHDR" and len(head) >= 24:
        return struct.unpack(">II", head[16:24])
    return None, None


def wav_seconds(path):
    try:
        with wave.open(path) as w:
            return w.getnframes() / w.getframerate()
    except (wave.Error, EOFError, ZeroDivisionError):
        return None


def describe(path, wanted, audit):
    st = os.stat(path)
    row = {"path": path, "name": os.path.basename(path), "kind": kind_of(path), "size": st.st_size,
           "modified": datetime.datetime.fromtimestamp(st.st_mtime, tz=datetime.timezone.utc),
           "width": None, "height": None, "seconds": None}
    if wanted & HEADER_COLUMNS and row["kind"] in ("image", "audio"):
        if audit:                                   # one line per file the task opens
            with open(os.path.join(audit, f"opened-{os.getpid()}.log"), "a") as f:
                f.write(path + "\n")
        if row["kind"] == "image":
            with open(path, "rb") as f:
                row["width"], row["height"] = png_size(f.read(24))
        else:
            row["seconds"] = wav_seconds(path)
    return row


class FilesPartition(InputPartition):
    def __init__(self, files):
        self.files = files                          # paths only, never file contents


class DocsDataSource(DataSource):
    @classmethod
    def name(cls):
        return "docs"

    def schema(self):
        return SCHEMA

    def reader(self, schema):
        return DocsReader(self.options, schema)


class DocsReader(DataSourceReader):
    def __init__(self, options, schema):
        if "path" not in options:
            raise ValueError("option 'path' is required: the folder to read")
        self.root = os.path.abspath(options["path"])
        if not os.path.isdir(self.root):
            raise ValueError(f"option 'path' must be an existing folder, got {options['path']!r}")
        self.pattern = options.get("glob", "**/*")
        self.max_partitions = int(options.get("partitions", "2"))
        self.audit = options.get("audit_dir")
        self.columns = [f.name for f in schema.fields]
        unknown = set(self.columns) - set(c.split()[0] for c in SCHEMA.split(", "))
        if unknown:
            raise ValueError(f"docs has no column(s) {sorted(unknown)}")

    def partitions(self):
        if self.audit:
            with open(os.path.join(self.audit, "planned.log"), "a") as f:
                f.write("partitions()\n")
        files = list_files(self.root, self.pattern)
        return [FilesPartition(g) for g in plan_files(files, self.max_partitions)]

    def read(self, partition):
        if not isinstance(partition, FilesPartition):
            return
        wanted = set(self.columns)
        for path in partition.files:
            row = describe(path, wanted, self.audit)
            yield tuple(row[c] for c in self.columns)


# A scratch copy of the eight sample folders, so the run can add a file later.
sample = tempfile.mkdtemp(prefix="ch65-docs-")
for folder in ["audio", "docs", "events", "images", "logs", "mail", "pdf", "xml"]:
    shutil.copytree(MESSY / folder, os.path.join(sample, folder))

files = list_files(sample, "**/*")
sizes = dict(files)
print(f"{len(files)} files, {sum(sizes.values()):,} bytes; largest:",
      [(os.path.basename(p), s) for p, s in sorted(files, key=lambda f: -f[1])[:3]])
for label, groups in (("round robin", round_robin(files, 3)), ("largest first", plan_files(files, 3))):
    print(f"{label:<14}", [(len(g), sum(sizes[p] for p in g)) for g in groups], "(files, bytes) per task")

with open(MESSY / "images/products/red_16x8.png", "rb") as f:
    print("\npng_size(red_16x8.png) =", png_size(f.read(24)))
with open(MESSY / "images/scans/corrupt.png", "rb") as f:
    print("png_size(corrupt.png)  =", png_size(f.read(24)))
print("wav_seconds(silence.wav) =", wav_seconds(str(MESSY / "audio/silence.wav")),
      " wav_seconds(ticket_0.eml) =", wav_seconds(str(MESSY / "mail/ticket_0.eml")))

spark.dataSource.register(DocsDataSource)
audit = tempfile.mkdtemp(prefix="ch65-audit-")


def opened():
    """Files opened by tasks since the last call, then clear the audit."""
    n = 0
    for f in Path(audit).glob("opened-*.log"):
        n += len(f.read_text().splitlines())
        f.unlink()
    return n


def planned():
    p = Path(audit, "planned.log")
    n = len(p.read_text().splitlines()) if p.exists() else 0
    p.unlink(missing_ok=True)
    return n


def docs(**opts):
    r = spark.read.format("docs").option("path", sample).option("partitions", 3).option("audit_dir", audit)
    for k, v in opts.items():
        r = r.option(k, v)
    return r


df = docs().load()
print()
(df.groupBy("kind").agg(F.count("*").alias("files"), F.sum("size").alias("bytes"))
   .orderBy("kind").show())
print(f"that query opened {opened()} files and planned {planned()} time(s)\n")

print("Bytes per task, from spark_partition_id():")
(df.groupBy(F.spark_partition_id().alias("task"))
   .agg(F.count("*").alias("files"), F.sum("size").alias("bytes")).orderBy("task").show())
opened()

df.where("kind = 'image'").select("name", "width", "height").orderBy("name").show()
print(f"opened {opened()} files\n")

df.select("name").count()
print(f"select('name').count() opened {opened()} files")
print(f"partitions() ran {planned()} more time(s) for the next three queries on df")
print([l.strip()[:84] + " ..." for l in plan(df.select("name"), "formatted").splitlines()
       if "ReadSchema" in l][0])
lean = docs().schema("name string, kind string, size long").load()
lean.count()
print(f".schema('name string, kind string, size long') then count() opened {opened()} files")

# The planned partitions belong to the DataFrame: a file added later is not read by it.
opened(); planned()
before = df.count()
Path(sample, "logs", "late.log").write_text("arrived after planning\n")
print(f"\ncount: {before}; after adding late.log, same DataFrame: {df.count()}; "
      f"fresh load(): {docs().load().count()}")
print(f"partitions() ran {planned()} time(s) for those three actions")

print("\nEmpty folder:", docs().option("path", tempfile.mkdtemp()).load().count(), "rows")
try:
    docs().option("path", "no/such/folder").load().count()
except Exception as e:
    print("Bad path:", [l for l in str(e).splitlines() if l.startswith("ValueError")][0])

# A file removed after planning: the task's os.stat() fails.
os.remove(os.path.join(sample, "docs", "privacy.md"))
try:
    df.count()
except Exception as e:
    lines = str(e).splitlines()
    cond = e.getCondition() if hasattr(e, "getCondition") else None
    print("privacy.md removed, same DataFrame:", cond)
    print("  ", [l for l in lines if l.startswith("FileNotFoundError")][0].replace(sample, "<sample>"))
print("fresh load():", docs().load().count(), "rows")
