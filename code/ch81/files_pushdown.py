"""Chapter 81: filter pushdown into a file source: the contract, the proof, and the 4.3.0 plan-reuse trap.

Run from code/:  uv run python ch81/files_pushdown.py
"""
import os, re, shutil, struct, sys, tempfile, uuid
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.datasource import (DataSource, DataSourceReader, EqualTo, GreaterThan, GreaterThanOrEqual,
                                    In, InputPartition, LessThan, LessThanOrEqual, StringEndsWith,
                                    StringStartsWith)
from common.book import session, plan, MESSY

spark = session("ch81-files", spark_sql_python_filterPushdown_enabled="true")

KINDS = {".png": "image", ".wav": "audio", ".pdf": "pdf", ".eml": "email", ".md": "text",
         ".txt": "text", ".log": "log", ".json": "json", ".xml": "xml"}
SIZE_TESTS = {GreaterThan: lambda s, v: s > v, GreaterThanOrEqual: lambda s, v: s >= v,
              LessThan: lambda s, v: s < v, LessThanOrEqual: lambda s, v: s <= v}


def kind_of(path):
    return KINDS.get(os.path.splitext(path)[1].lower(), "other")


def list_files(root):
    """Absolute, sorted (path, size) pairs, as in Chapter 79."""
    return sorted((str(p.resolve()), p.stat().st_size) for p in Path(root).glob("**/*") if p.is_file())


def plan_files(files, n):
    """Largest file first, into the lightest group (Chapter 79)."""
    groups, totals = [[] for _ in range(max(n, 1))], [0] * max(n, 1)
    for path, size in sorted(files, key=lambda f: (-f[1], f[0])):
        i = totals.index(min(totals))
        groups[i].append(path)
        totals[i] += size
    return [g for g in groups if g]


class FilesPartition(InputPartition):
    def __init__(self, files):
        self.files = files


class FilesDataSource(DataSource):
    @classmethod
    def name(cls):
        return "files"

    def schema(self):
        return "path string, name string, kind string, size long, width int, height int"

    def reader(self, schema):
        return FilesReader(self.options)


class FilesReader(DataSourceReader):
    def __init__(self, options):
        self.root = os.path.abspath(options["path"])
        self.n = int(options.get("partitions", "3"))
        self.audit = options.get("audit_dir")
        self.kinds = None          # from EqualTo / In on kind
        self.prefixes, self.suffixes, self.size_tests = [], [], []

    def pushFilters(self, filters):
        """Take over the filters that the file listing can decide. Yield every other one back."""
        for f in filters:
            if isinstance(f, EqualTo) and f.attribute == ("kind",):
                self.kinds = {f.value} if self.kinds is None else self.kinds & {f.value}
            elif isinstance(f, In) and f.attribute == ("kind",):
                self.kinds = set(f.value) if self.kinds is None else self.kinds & set(f.value)
            elif isinstance(f, StringStartsWith) and f.attribute == ("name",):
                self.prefixes.append(f.value)
            elif isinstance(f, StringEndsWith) and f.attribute == ("name",):
                self.suffixes.append(f.value)
            elif type(f) in SIZE_TESTS and f.attribute == ("size",):
                self.size_tests.append((type(f), f.value))
            else:
                yield f

    def matches(self, path, size):
        name = os.path.basename(path)
        return ((self.kinds is None or kind_of(path) in self.kinds)
                and all(name.startswith(p) for p in self.prefixes)
                and all(name.endswith(s) for s in self.suffixes)
                and all(SIZE_TESTS[t](size, v) for t, v in self.size_tests))

    def partitions(self):
        if self.audit:
            with open(os.path.join(self.audit, "planned.log"), "a") as f:
                f.write("partitions()\n")
        files = [(p, s) for p, s in list_files(self.root) if self.matches(p, s)]
        return [FilesPartition(g) for g in plan_files(files, self.n)]

    def read(self, partition):
        if not isinstance(partition, FilesPartition):
            return
        opened = []
        for path in partition.files:
            with open(path, "rb") as f:
                head = f.read(24)
            opened.append(path)
            w = h = None
            if head[:8] == b"\x89PNG\r\n\x1a\n" and head[12:16] == b"IHDR":
                w, h = struct.unpack(">II", head[16:24])
            yield (path, os.path.basename(path), kind_of(path), os.path.getsize(path), w, h)
        if self.audit:
            with open(os.path.join(self.audit, f"opened-{uuid.uuid4().hex}.log"), "w") as log:
                log.write("".join(p + "\n" for p in opened))


root = tempfile.mkdtemp(prefix="ch81-files-")
for folder in ["audio", "docs", "events", "images", "logs", "mail", "pdf", "xml"]:
    shutil.copytree(MESSY / folder, os.path.join(root, folder))
ALL = list_files(root)
audit = tempfile.mkdtemp(prefix="ch81-audit-")
spark.dataSource.register(FilesDataSource)


def opened():
    n = 0
    for f in Path(audit).glob("opened-*.log"):
        n += len(f.read_text().splitlines())
        f.unlink()
    return n


def files(fmt="files"):
    return spark.read.format(fmt).option("path", root).option("audit_dir", audit).load()


def spark_filter(df):
    """The condition of Spark's own Filter operator above the scan, or '-' if there is none."""
    lines = [l.strip() for l in plan(df, "simple").splitlines() if "Filter" in l and "Scan" not in l]
    return re.sub(r"#\d+L?", "", lines[0].split("Filter ", 1)[1]) if lines else "-"


def pushed(df):
    m = re.search(r"PushedFilters: \[(.*?)\]", plan(df, "formatted"))
    return m.group(1) if m else "(none shown)"


print(f"{len(ALL)} files under the sample root\n")
print(f"{'query':<34} {'rows':>4} {'opened':>6}  Spark's Filter")
QUERIES = [
    ("kind = 'image'", F.col("kind") == "image"),
    ("kind IN ('audio', 'pdf')", F.col("kind").isin("audio", "pdf")),
    ("name LIKE '%.eml'", F.col("name").endswith(".eml")),
    ("size > 4000", F.col("size") > 4000),
    ("kind = 'image' AND width > 12", (F.col("kind") == "image") & (F.col("width") > 12)),
    ("kind = 'audio' OR size < 100", (F.col("kind") == "audio") | (F.col("size") < 100)),
    ("lower(kind) = 'image'", F.lower("kind") == "image"),
]
opened()
for label, cond in QUERIES:
    q = files().filter(cond)
    n = q.count()
    print(f"{label:<34} {n:>4} {opened():>6}  {spark_filter(q)}")

q = files().filter((F.col("kind") == "image") & (F.col("width") > 12))
print("\nexplain('formatted') of the mixed query:")
for l in plan(q, "formatted").splitlines():
    if l.startswith(("PushedFilters", "ReadSchema")) or "BatchScan" in l and "(1)" in l:
        print("  " + re.sub(r"#\d+L?", "", l.strip())[:86])
opened()


# The contract: a filter you do not yield back is a filter Spark no longer checks.
class AcceptsAll(FilesReader):
    def pushFilters(self, filters):
        list(super().pushFilters(filters))      # takes the ones it can apply...
        return iter([])                         # ...and claims the rest too


class LyingSource(FilesDataSource):
    @classmethod
    def name(cls):
        return "lying"

    def reader(self, schema):
        return AcceptsAll(self.options)


spark.dataSource.register(LyingSource)
print("\nA reader that yields nothing back, but applies only what it understands:")
for label, cond in (("kind = 'image' AND width > 12", (F.col("kind") == "image") & (F.col("width") > 12)),
                    ("lower(kind) = 'image'", F.lower("kind") == "image")):
    good, bad = files().filter(cond), files("lying").filter(cond)
    print(f"  {label:<32} correct: {good.count():>2}  lying: {bad.count():>2}  "
          f"lying plan's Filter: {spark_filter(bad)}")
opened()


# Push and return: a coarse check in the source, and Spark checks again.
class Coarse(FilesReader):
    def pushFilters(self, filters):
        for f in filters:
            if isinstance(f, StringStartsWith) and f.attribute == ("name",):
                self.prefixes.append(f.value[:1])   # only the first letter: a coarse index
            yield f                                 # and give every filter back to Spark


class CoarseSource(FilesDataSource):
    @classmethod
    def name(cls):
        return "coarse"

    def reader(self, schema):
        return Coarse(self.options)


spark.dataSource.register(CoarseSource)
q = files("coarse").filter(F.col("name").startswith("tick"))
print(f"\npush and return, name LIKE 'tick%': {q.count()} rows, {opened()} files opened, "
      f"Spark's Filter: {spark_filter(q)}")


# The 4.3.0 trap: the planned read is stored on the relation and shared by later scans.
def planned():
    p = Path(audit, "planned.log")
    n = len(p.read_text().splitlines()) if p.exists() else 0
    p.unlink(missing_ok=True)
    return n


planned()
print("\nOne DataFrame, a pushed filter first, then other queries on the same DataFrame:")
df = files()
print("  df.filter(kind = 'image').count():", df.filter(F.col("kind") == "image").count())
print("  df.count():                       ", df.count(), f"(the folder has {len(ALL)} files)")
print("  df.filter(kind = 'audio').count():", df.filter(F.col("kind") == "audio").count())
print("  partitions() calls for the three: ", planned())
fresh = files()
print("  fresh load(), count():            ", fresh.count())
u = files()
print("  fresh d, d.union(d.filter(kind = 'image')).count():",
      u.union(u.filter(F.col("kind") == "image")).count(), f"(right answer: {len(ALL) + 5})")
