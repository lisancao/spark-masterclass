"""Chapter 88, Steps 8-9: map and grouped operators, and how a Python data source is planned.

Run from code/:  uv run python ch88/map_ops.py
"""
import os, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, EqualTo, InputPartition
from common.book import session, plan, ROOT
from logs import access_parquet

spark = session("ch88-map-ops")
df = access_parquet(spark)

def show(title, q, mode="simple"):
    print(f"--- {title}")
    for line in plan(q, mode).splitlines()[1:]:
        if line.strip():
            print(re.sub(r"Batched: true, DataFilters: .*?(PushedFilters: \[.*?\]).*", r"... \1", line))
    print()

show("mapInPandas, then a filter on its output",
     df.mapInPandas(lambda it: it, df.schema).filter("status = 200"))
show("a filter, then mapInPandas",
     df.filter("status = 200").mapInPandas(lambda it: it, df.schema))
show("groupBy().applyInPandas()",
     df.groupBy("status").applyInPandas(lambda p: p.head(1), df.schema))


# A small Python data source over this book's data folder: one row per file.
DATA = ROOT / "code" / "data"

class FilesSource(DataSource):
    @classmethod
    def name(cls):
        return "files"
    def schema(self):
        return "path string, kind string, size long"
    def reader(self, schema):
        return FilesReader()

class FilesReader(DataSourceReader):
    def __init__(self):
        self.kind = None
    def pushFilters(self, filters):
        for f in filters:
            if isinstance(f, EqualTo) and f.attribute == ("kind",):
                self.kind = f.value          # accepted: read() applies it
            else:
                yield f                      # declined: Spark keeps it in a Filter above the scan
    def partitions(self):
        return [InputPartition(d) for d in sorted(os.listdir(DATA))]
    def read(self, partition):
        for f in sorted((DATA / partition.value).iterdir()):
            kind = f.suffix.lstrip(".")
            if self.kind is None or kind == self.kind:
                yield (f"{partition.value}/{f.name}", kind, f.stat().st_size)

spark.dataSource.register(FilesSource)
spark.conf.set("spark.sql.python.filterPushdown.enabled", "true")

files = spark.read.format("files").load()
q = files.filter("kind = 'json'").select("path")
show("Python data source, simple mode", q)
show("Python data source, formatted mode (scan node only)", q, "formatted")
print(q.collect())
