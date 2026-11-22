"""Chapter 24, Step 2: one shared Connect server, two sessions. What is isolated and what is shared.

Run from code/:  uv run python ch24/isolation.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ROOT  # noqa: E402
from connect_util import server, session, close, error_class  # noqa: E402
from pyspark.sql.connect import functions as F  # noqa: E402
from pyspark.sql.types import LongType  # noqa: E402
import contextlib  # noqa: E402
import io  # noqa: E402

WAREHOUSE = ROOT / "data" / "ch24" / "warehouse"
url = server(f"spark.sql.warehouse.dir={WAREHOUSE}")
a, b = session(url, user_id="alice"), session(url, user_id="bob")


def check(label, fn):
    try:
        print(f"  {label:44} {fn()}")
    except Exception as e:
        print(f"  {label:44} {error_class(e)}")


print("session A sets state:")
a.range(5).createOrReplaceTempView("t")
a.conf.set("spark.sql.shuffle.partitions", "7")
a.udf.register("plus_one", lambda x: x + 1, LongType())   # a DDL string needs an active session
a.range(3).createOrReplaceGlobalTempView("g")
a.sql("DROP TABLE IF EXISTS shared_tbl")
a.range(4).write.saveAsTable("shared_tbl")
cached = a.range(1000).withColumn("sq", F.col("id") * F.col("id"))
cached.cache().count()

print("what session B sees:")
check("temp view t", lambda: b.table("t").count())
check("spark.sql.shuffle.partitions", lambda: b.conf.get("spark.sql.shuffle.partitions"))
check("UDF plus_one", lambda: b.sql("SELECT plus_one(1)").first()[0])
check("global temp view global_temp.g", lambda: b.table("global_temp.g").count())
check("table shared_tbl", lambda: b.table("shared_tbl").count())
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    b.range(1000).withColumn("sq", F.col("id") * F.col("id")).explain()
check("A's cache, used by B's identical query", lambda: "InMemoryTableScan" in buf.getvalue())
check("B sets spark.executor.memory", lambda: b.conf.set("spark.executor.memory", "8g"))
check("B sets spark.sql.warehouse.dir", lambda: b.conf.set("spark.sql.warehouse.dir", "/tmp/x"))
check("B reads a file under the server's /etc", lambda: b.read.text("/etc/hostname").count())
check("B calls spark.stop() then A runs a query", lambda: (b.stop(), a.range(3).count())[1])

a.sql("DROP TABLE IF EXISTS shared_tbl")
close(a)
