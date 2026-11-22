"""Chapter 50: does 4.3's single-task execution change a Python data source scan? (It does not.)

Run from code/:  uv run python ch50/feather_check.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from common.book import session, plan, ZONES

FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"
spark = session("ch50-feather", **{FLAG.replace(".", "_"): "true"})


class Part(InputPartition):
    def __init__(self, i):
        self.i = i


class Three(DataSource):
    def schema(self):
        return "id int"

    def reader(self, schema):
        return ThreeReader()


class ThreeReader(DataSourceReader):
    def partitions(self):
        return [Part(i) for i in range(3)]

    def read(self, partition):
        yield (partition.i,)


class One(Three):
    def reader(self, schema):
        return OneReader()


class OneReader(DataSourceReader):          # no partitions(): one InputPartition(None)
    def read(self, partition):
        yield (0,)


spark.dataSource.register(Three)
spark.dataSource.register(One)
print(f"{FLAG} = {spark.conf.get(FLAG)}")
for name in ("Three", "One"):
    df = spark.read.format(name).load()
    q = df.groupBy("id").count()
    shuffles = plan(q, "simple").count("Exchange")
    print(f"{name:<6} scan partitions: {df.rdd.getNumPartitions()}, Exchange operators in groupBy plan: {shuffles}")
zones = spark.read.option("header", True).csv(str(ZONES))
print(f"the zones CSV file, scan partitions: {zones.rdd.getNumPartitions()}, Exchange operators in groupBy plan:",
      plan(zones.groupBy("Borough").count(), "simple").count("Exchange"))
