"""Chapter 67: which filters Spark offers a Python data source, in what form, and when.

Run from code/:  uv run python ch67/what_arrives.py
"""
import json, os, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql.datasource import DataSource, DataSourceReader
from common.book import session, plan

FLAG = "spark.sql.python.filterPushdown.enabled"
spark = session("ch67-what-arrives", spark_sql_session_timeZone="UTC")
LOG = os.path.join(tempfile.mkdtemp(prefix="ch67-filters-"), "pushed.jsonl")


def show(f):
    """One filter object as text: Class(column, value)."""
    name = type(f).__name__
    col = ".".join(f.attribute) if hasattr(f, "attribute") else None
    if name == "Not":
        return f"Not({show(f.child)})"
    if hasattr(f, "value"):
        v = f.value
        if hasattr(v, "tzinfo"):
            v = f"datetime:{v.isoformat()} ({type(v.tzinfo).__name__} {v.tzinfo})"
        else:
            v = f"{type(v).__name__}:{v!r}" if not isinstance(v, (str, tuple)) else repr(v)
        return f"{name}({col}, {v})"
    return f"{name}({col})"


class Probe(DataSource):
    """Records every filter it is offered, and accepts none of them."""
    @classmethod
    def name(cls):
        return "probe"

    def schema(self):
        return ("id int, kind string, amount decimal(10,2), ts timestamp, "
                "tags array<string>, info struct<city:string>")

    def reader(self, schema):
        return ProbeReader(self.options)


class ProbeReader(DataSourceReader):
    def __init__(self, options):
        self.log = options["log"]

    def pushFilters(self, filters):
        with open(self.log, "a") as f:
            f.write(json.dumps([show(x) for x in filters]) + "\n")
        yield from filters                       # accept nothing: Spark keeps every filter

    def read(self, partition):
        return iter([])


spark.dataSource.register(Probe)
df = spark.read.format("probe").option("log", LOG).load()
df.createOrReplaceTempView("t")


def offered(where):
    open(LOG, "w").close()
    spark.sql(f"SELECT * FROM t WHERE {where}").collect()
    calls = [json.loads(l) for l in open(LOG)]
    return "pushFilters() not called" if not calls else ", ".join(calls[0]) or "[] (called, empty)"


# 1. With the flag off (the default), a reader that has pushFilters() fails every query.
print(f"{FLAG} = {spark.conf.get(FLAG)} (default)")
try:
    spark.sql("SELECT * FROM t").collect()
except Exception as e:
    import re
    print("  SELECT * FROM t:", str(e).splitlines()[0].split(":")[0])
    import textwrap
    cond, msg = re.findall(r"\[(DATA_SOURCE_[A-Z_]+)\] ([^\n]*)", str(e))[-1]
    print(textwrap.fill(f"[{cond}] {msg}", 86, initial_indent="    ", subsequent_indent="    "))
spark.conf.set(FLAG, "true")
print(f"{FLAG} = true\n")

# 2. What arrives for each condition.
CONDITIONS = [
    "kind = 'image'", "kind <> 'image'", "kind <=> 'image'", "id > 3", "NOT (id > 3)",
    "id BETWEEN 2 AND 5", "id IN (1, 2, 3)", "kind IS NULL", "kind LIKE 'im%'",
    "kind LIKE '%age'", "kind LIKE '%ma%'", "kind LIKE 'i%e'", "lower(kind) = 'image'",
    "id + 1 > 3", "id > 1.5", "amount > 100", "ts >= '2026-10-01 10:00:00'",
    "info.city = 'Hanoi'", "array_contains(tags, 'x')", "kind = 'a' OR id = 1",
    "id > 1 AND (kind = 'a' OR id = 2)",
]
print(f"{'WHERE':<34} filters offered to pushFilters()")
for c in CONDITIONS:
    print(f"{c:<34} {offered(c)}")

# 3. pushFilters() runs in planning: explain() calls it, before any task.
open(LOG, "w").close()
plan(spark.sql("SELECT * FROM t WHERE id = 7"))
print("\nexplain() alone: pushFilters() calls:", len(open(LOG).read().splitlines()))


# 4. Spark matches the filters you yield back by object identity, not by value.
class CopyReader(ProbeReader):
    def pushFilters(self, filters):
        import copy
        for f in filters:
            yield copy.deepcopy(f)               # an equal filter, but a different object


class CopyProbe(Probe):
    @classmethod
    def name(cls):
        return "copyprobe"

    def reader(self, schema):
        return CopyReader(self.options)


spark.dataSource.register(CopyProbe)
try:
    spark.read.format("copyprobe").option("log", LOG).load().where("id = 7").collect()
    print("yield a copy: accepted")
except Exception as e:
    import re, textwrap
    cond, msg = re.findall(r"\[(DATA_SOURCE_[A-Z_]+)\] ([^\n]*)", str(e))[-1]
    print("\nyield a copy of a filter:")
    print(textwrap.fill(f"[{cond}] {msg}", 86, initial_indent="    ", subsequent_indent="    "))
