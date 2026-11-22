"""Chapter 52: the same questions asked of the Python docs source and of jdocs.

Run from code/:  uv run python ch52/capability.py
(Timings: wrap the run in flock $HOME/spark-masterclass/data/.bench.lock.)
"""
import statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import JAR, docs_root, short, make_pydocs  # noqa: E402
from common.book import session  # noqa: E402

spark = session("ch52-capability", spark_jars=str(JAR), spark_sql_session_timeZone="UTC",
                spark_sql_python_filterPushdown_enabled="true")   # off by default in 4.3.0
spark.dataSource.register(make_pydocs(pushdown=True))
root = docs_root()


def src(fmt):
    return spark.read.format(fmt).option("path", root).load()


def physical(df):
    return short(df._jdf.queryExecution().executedPlan().toString())


def scan_info(df):
    """The scan node of the formatted plan: what the source was asked to produce."""
    txt = short(df._jdf.queryExecution().explainString(
        spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")))
    block = txt[txt.index("(1) BatchScan"):].split("\n\n")[0]
    keep = [l for l in block.splitlines()
            if l.startswith(("(1) BatchScan", "Output", "PushedFilters", "ReadSchema"))]
    return "\n".join(keep[:4])


questions = {
    "filter + two columns": lambda d: d.filter("kind = 'image'").select("name", "width"),
    "limit(3)": lambda d: d.select("name").limit(3),
    "count(*)": lambda d: d.groupBy().count(),
}
for title, q in questions.items():
    for fmt in ("pydocs", "jdocs"):
        df = q(src(fmt))
        df.collect()
        print(f"\n== {title}: {fmt}")
        print(physical(df).rstrip())
        if title.startswith("filter"):
            print("-- formatted scan node:")
            print(scan_info(df))

print("\n== statistics the optimizer sees, before any filter")
for fmt in ("pydocs", "jdocs"):
    print(f"{fmt:7} {src(fmt)._jdf.queryExecution().optimizedPlan().stats().toString()}")

print("\n== time per query: select name, size (26 files), median of 15 after 3 warm-up runs")
for fmt in ("pydocs", "jdocs"):
    times = []
    for i in range(18):
        t = time.perf_counter()
        src(fmt).select("name", "size").collect()       # a new DataFrame each run
        if i >= 3:
            times.append(time.perf_counter() - t)
    print(f"{fmt:7} {statistics.median(times) * 1000:6.1f} ms")
spark.stop()
