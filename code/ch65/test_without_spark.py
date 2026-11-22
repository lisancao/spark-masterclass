"""Chapter 65: test a data source as plain Python, with the checks that Spark makes, and no JVM.

Run from code/:  uv run python ch65/test_without_spark.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark import cloudpickle
from pyspark.sql.datasource import CaseInsensitiveDict, InputPartition
from pyspark.sql.types import LongType, Row, StringType, StructField, StructType
from counter_source import CounterDataSource, CounterReader, RangePartition, plan_ranges

# 1. A schema without a session: build StructType objects. fromDDL needs a running Spark.
try:
    StructType.fromDDL("id long, label string")
except Exception as e:
    print("StructType.fromDDL without a session:", str(e).splitlines()[0][:70])
SCHEMA = StructType([StructField("id", LongType()), StructField("label", StringType())])


def read_without_spark(source_cls, options, schema=SCHEMA):
    """Plan and read like Spark does, in this process, with the same checks Spark makes."""
    source = source_cls(CaseInsensitiveDict({k: str(v) for k, v in options.items()}))
    reader = source.reader(schema)
    parts = reader.partitions()
    if not isinstance(parts, list):
        raise TypeError(f"partitions() must return a list, got {type(parts).__name__}")
    if not all(isinstance(p, InputPartition) for p in parts):
        raise TypeError("every partition must be an InputPartition")
    parts = parts or [InputPartition(None)]                # what Spark does with []
    reader, parts = cloudpickle.loads(cloudpickle.dumps((reader, parts)))   # the trip to a task
    rows = []
    for p in parts:
        for row in reader.read(p):
            if not isinstance(row, (tuple, list, Row)):
                raise TypeError(f"read() yielded a {type(row).__name__}, not a tuple")
            if len(row) != len(schema.fields):
                raise ValueError(f"row {row!r} has {len(row)} values, schema has {len(schema.fields)}")
            rows.append(tuple(row))
    return len(parts), rows


# 2. The planner is a plain function: test its edges directly.
assert plan_ranges(10, 3) == [(0, 4), (4, 7), (7, 10)]
assert plan_ranges(2, 5) == [(0, 1), (1, 2)]               # never more ranges than rows
assert plan_ranges(0, 2) == []
print("plan_ranges: 3 checks pass")

# 3. The whole source, as Spark would run it.
print("rows=5, partitions=2:", read_without_spark(CounterDataSource, {"rows": 5, "partitions": 2}))
print("rows=0:", read_without_spark(CounterDataSource, {"rows": 0}))
swapped = StructType([StructField("label", StringType()), StructField("id", LongType())])
print("user schema (label, id):", read_without_spark(CounterDataSource, {"rows": 2}, swapped)[1])
for bad in ({}, {"rows": "ten"}, {"rows": 3, "partitions": 0}):
    try:
        read_without_spark(CounterDataSource, bad)
    except ValueError as e:
        print(f"options {bad}: ValueError: {e}")


# 4. A mistake that Spark would catch at the first action, caught here first.
class GeneratorReader(CounterReader):
    def partitions(self):
        return (RangePartition(s, e) for s, e in plan_ranges(self.rows, self.partition_count))


class GeneratorSource(CounterDataSource):
    def reader(self, schema):
        return GeneratorReader(self.options, schema)


try:
    read_without_spark(GeneratorSource, {"rows": 3})
except TypeError as e:
    print("generator partitions: TypeError:", e)
print("\nNo JVM started.")
