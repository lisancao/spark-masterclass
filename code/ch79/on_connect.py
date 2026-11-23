"""Chapter 79: the same protocol over Spark Connect: when schema() runs, what a fresh load() sees,
and where a relative path points.

Start the server first:  ch79/server.sh start
Run from code/:          uv run python ch79/on_connect.py
"""
import os, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader


def trace(path, what):
    with open(path, "a") as f:
        f.write(f"{what:<26} pid {os.getpid():<7} cwd {short(os.getcwd())}\n")


def short(cwd):
    """Shorten Spark's per-session folders: /tmp/spark-<id>/userFiles-<id>/<session id>."""
    import re
    return re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<id>", cwd.replace(os.path.expanduser("~"), "~"))


class Folder(DataSource):
    """One row per file in the folder of option 'path'. Every method traces itself."""
    @classmethod
    def name(cls):
        return "folder"

    def schema(self):
        trace(self.options["log"], "DataSource.schema")
        return "name string"

    def reader(self, schema):
        return FolderReader(self.options)


class FolderReader(DataSourceReader):
    def __init__(self, options):
        self.log = options["log"]
        self.path = options["path"]                 # as the user typed it: maybe relative
        trace(self.log, "DataSourceReader.__init__")
        if not os.path.isdir(self.path):
            raise ValueError(f"no folder {os.path.abspath(self.path)}")
        self.files = sorted(os.listdir(self.path))

    def read(self, partition):
        trace(self.log, "read")
        for name in self.files:
            yield (name,)


def show_log(log, title):
    lines = open(log).read().splitlines() if os.path.exists(log) else []
    print(f"--- {title}")
    print("\n".join(lines) if lines else "(nothing ran)")
    open(log, "w").close()


spark = SparkSession.builder.remote(os.environ.get("SPARK_REMOTE", "sc://localhost:10290")).getOrCreate()
print("client:", type(spark).__module__, "| server: Spark", spark.version, "| client pid", os.getpid())
spark.dataSource.register(Folder)

work = Path(tempfile.mkdtemp(prefix="ch79-connect-"))
log = str(work / "trace.log")
inbox = work / "inbox"
inbox.mkdir()
for name in ("a.txt", "b.txt"):
    (inbox / name).write_text(name)

df = spark.read.format("folder").option("log", log).option("path", str(inbox)).load()
show_log(log, "after load()")
print("df.columns:", df.columns)
show_log(log, "after df.columns")
print("count:", df.count())
show_log(log, "after count()")

(inbox / "c.txt").write_text("late")
print("\nc.txt added.")
print("same DataFrame, count:", df.count())
fresh = spark.read.format("folder").option("log", log).option("path", str(inbox)).load()
print("fresh load(), same options, count:", fresh.count())
show_log(log, "planning calls for those two counts")
other = spark.read.format("folder").option("log", log).option("path", str(inbox)).option("v", 2).load()
print("fresh load(), one more option, count:", other.count())

print("\nA relative path, from the client's folder", short(str(work)))
os.chdir(work)
try:
    spark.read.format("folder").option("log", log).option("path", "inbox").load().count()
except Exception as e:
    line = [l for l in str(e).splitlines() if l.startswith("ValueError")][0]
    print("path='inbox':", short(line))
open(log, "w").close()
print("absolute path, count:",
      spark.read.format("folder").option("log", log).option("path", os.path.abspath("inbox")).load().count())
spark.stop()
