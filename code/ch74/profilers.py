"""Chapter 74, Step 10: the Python UDF profiler and the Python data source profiler (4.2+).

Profiles a cheap row UDF, a slow row UDF and a pandas UDF with spark.sql.pyspark.udf.profiler=perf,
measures what the profiler itself costs (median of 5 runs after a warmup, wall time of collect()),
then profiles a small Python data source with spark.sql.pyspark.dataSource.profiler=perf.

Run from code/:  flock $HOME/spark-masterclass/data/.bench.lock uv run python ch74/profilers.py
"""
import io
import re
import statistics
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition  # noqa: E402

spark = session("profilers", spark_master="local[4]", spark_ui_showConsoleProgress="false")


@F.udf("long")
def cheap(x):
    return x + 1


@F.udf("long")
def slow(x):
    h = 0
    for i in range(300):
        h = (h * 31 + x + i) % 1000003
    return h


@F.pandas_udf("long")
def vectorized(s):
    return s + 1


df = spark.range(0, 200_000, numPartitions=4)


def wall(u, runs=5):
    df.select(F.sum(u("id"))).collect()                       # warmup
    times = []
    for _ in range(runs):
        t = time.perf_counter()
        df.select(F.sum(u("id"))).collect()
        times.append(time.perf_counter() - t)
    return statistics.median(times)


def top_lines(text, n=4):
    """The header and the first rows of a cProfile table, with paths shortened."""
    rows = [l for l in text.splitlines() if l.strip()]
    keep = [l for l in rows if "Profile of" in l] + [l for l in rows if "function calls" in l][:1] \
        + [l for l in rows if "ncalls" in l][:1]
    body = [l for l in rows if re.match(r"\s+\d", l)][:n]
    return [re.sub(r"\S*/(site-packages|lib/python3\.\d+)/", "", l) for l in keep + body]


print("200,000 rows, 4 partitions; median wall time of 5 runs after a warmup")
print(f"{'udf':12} {'profiler off':>13} {'perf':>8} {'ratio':>6}")
for name, u in [("cheap", cheap), ("slow", slow), ("pandas", vectorized)]:
    spark.conf.unset("spark.sql.pyspark.udf.profiler")
    off = wall(u)
    spark.conf.set("spark.sql.pyspark.udf.profiler", "perf")
    on = wall(u)
    print(f"{name:12} {off:12.2f}s {on:7.2f}s {on / off:6.1f}")
spark.conf.unset("spark.sql.pyspark.udf.profiler")

spark.profile.clear()
spark.conf.set("spark.sql.pyspark.udf.profiler", "perf")
df.select(F.sum(slow("id"))).collect()
spark.conf.unset("spark.sql.pyspark.udf.profiler")
buf = io.StringIO()
with redirect_stdout(buf):
    spark.profile.show(type="perf")
print("\nspark.profile.show(type='perf') for the slow UDF, first lines:")
for line in top_lines(buf.getvalue()):
    print(" ", line[:96])


# --- The Python data source profiler -------------------------------------------------------------
class Counter(DataSource):
    @classmethod
    def name(cls):
        return "counter"

    def schema(self):
        return "n long"

    def reader(self, schema):
        return CounterReader()


class CounterReader(DataSourceReader):
    def partitions(self):
        return [InputPartition(i) for i in range(4)]

    def read(self, partition):
        for n in range(partition.value * 50_000, (partition.value + 1) * 50_000):
            yield (n,)


spark.dataSource.register(Counter)
spark.profile.clear()
spark.conf.set("spark.sql.pyspark.dataSource.profiler", "perf")
total = spark.read.format("counter").load().agg(F.sum("n")).first()[0]
spark.conf.unset("spark.sql.pyspark.dataSource.profiler")
buf = io.StringIO()
with redirect_stdout(buf):
    spark.profile.show(type="perf")
print(f"\ndata source profiler: sum = {total}")
parts = buf.getvalue().split("=" * 60)
for head, body in zip(parts[1::2], parts[2::2]):
    calls = next(l.strip() for l in body.splitlines() if "function calls" in l)
    first = next((re.sub(r"\S*/(site-packages|lib/python3\.\d+)/", "", l.strip())
                  for l in body.splitlines() if re.match(r"\s+\d", l)), "")
    print(f"  {head.strip():34} {calls}")
    print(f"      top: {first[:84]}")
spark.stop()
