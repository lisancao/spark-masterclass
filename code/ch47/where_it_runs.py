"""Chapter 47: the smallest Python data source, and which process runs each of its methods.

Run from code/:  uv run python ch47/where_it_runs.py
"""
import os, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition, CaseInsensitiveDict
from common.book import session, plan

spark = session("ch47-where-it-runs")


# 1. The smallest data source: a name, a schema, a reader that yields tuples.
class Hello(DataSource):
    def schema(self):
        return "greeting string"

    def reader(self, schema):
        return HelloReader()


class HelloReader(DataSourceReader):
    def read(self, partition):
        yield ("hello",)
        yield ("world",)


spark.dataSource.register(Hello)
spark.read.format("Hello").load().show()
print("format('HELLO') also works:", spark.read.format("HELLO").load().count(), "rows\n")

# 2. Options arrive as strings in a case-insensitive dict.
opts = CaseInsensitiveDict({"Rows": "5"})
print("CaseInsensitiveDict:", opts["rows"], opts.get("ROWS"), "rows" in opts, opts.get("prefix", "row"), "\n")


# 3. A traced source: every method appends one line (method, pid) to a log file.
def trace(path, what):
    with open(path, "a") as f:
        f.write(f"{what:<28} pid {os.getpid()}\n")


class Part(InputPartition):
    def __init__(self, i):
        self.i = i


class Traced(DataSource):
    @classmethod
    def name(cls):
        return "traced"

    def __init__(self, options):
        super().__init__(options)
        trace(options["log"], "DataSource.__init__")

    def schema(self):
        trace(self.options["log"], "DataSource.schema")
        return "id int, opt string"

    def reader(self, schema):
        trace(self.options["log"], "DataSource.reader")
        return TracedReader(self.options)


class TracedReader(DataSourceReader):
    def __init__(self, options):
        self.log = options["log"]
        self.opt = options.get("someoption")
        trace(self.log, "DataSourceReader.__init__")

    def partitions(self):
        trace(self.log, "DataSourceReader.partitions")
        return [Part(i) for i in range(3)]

    def read(self, partition):
        trace(self.log, f"read(partition {partition.i})")
        yield (partition.i, f"{self.opt!r}")


log = os.path.join(tempfile.mkdtemp(prefix="ch47-trace-"), "trace.log")
spark.dataSource.register(Traced)


def show_log(title):
    print(f"--- {title}")
    lines = open(log).read().splitlines() if os.path.exists(log) else []
    print("\n".join(lines) if lines else "(nothing ran)")
    open(log, "w").close()


print(f"driver (this script)         pid {os.getpid()}\n")
df = spark.read.format("traced").option("log", log).option("SomeOption", 42).load()
show_log("after load()")
print(df.collect())
show_log("after the first collect()")
df.count()
show_log("after a second action on the same DataFrame")

print("\nThe plan names the scan as a Python scan:")
print(plan(df, "simple").strip())
