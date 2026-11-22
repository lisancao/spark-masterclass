"""Chapter 82: the same capability probes in every client, here in PySpark (pyspark-client 4.3.0)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.book import TRIPS  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

WORK = os.environ["WORK"]
BIG = "SELECT id, repeat('x', 100) AS pad FROM range(200000)"   # about 20 MB of results


def report(name, body):
    try:
        print(f"{name:<12} works   {body()}")
    except Exception as e:  # noqa: BLE001
        print(f"{name:<12} breaks  {str(e).splitlines()[0]}")


def error_case():
    try:
        spark.read.parquet(str(TRIPS)).select("revenue").collect()
        return "no error"
    except Exception as e:  # noqa: BLE001
        return f"{e.getCondition().split('.')[0]}, SQLSTATE {e.getSqlState()}"


def write_read():
    spark.range(10).write.mode("overwrite").parquet(f"{WORK}/python")
    return f"read back {spark.read.parquet(f'{WORK}/python').count()} rows"


def config():
    spark.conf.set("spark.sql.shuffle.partitions", "7")
    return repr(spark.conf.get("spark.sql.shuffle.partitions"))


def temp_view():
    spark.range(5).createOrReplaceTempView("five")
    return list(spark.sql("SELECT count(*) AS n FROM five").collect()[0])


def arrow():
    t = spark.range(1000).toArrow()
    return f"pyarrow.Table, {len(t.to_batches())} batch(es), {t.num_rows} rows"


def explain():
    import contextlib
    import io
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        spark.range(3).explain()
    return next(line.strip() for line in buf.getvalue().splitlines() if "Range" in line)


with SparkSession.builder.remote(os.environ.get("SPARK_REMOTE", "sc://localhost:15106")).getOrCreate() as spark:
    print(f"python (pyspark 4.3.0) -> server {spark.version}")
    report("sql", lambda: list(spark.sql("SELECT 1 AS x, 'a' AS y").collect()[0]))
    report("schema", lambda: (lambda c: f"{len(c)} columns, first {c[0]}")(spark.read.parquet(str(TRIPS)).columns))
    report("local_data", lambda: f"count {spark.createDataFrame([(1, 'Toronto'), (2, 'Hanoi')], 'id bigint, city string').count()}")
    report("temp_view", temp_view)
    report("write_read", write_read)
    report("config", config)
    report("error", error_case)
    report("arrow", arrow)
    report("explain", explain)
    report("zip", lambda: f"count {spark.range(3).zip(spark.range(3).withColumnRenamed('id', 'id2')).count()}")
    report("big_result", lambda: f"{len(spark.sql(BIG).collect())} rows")
