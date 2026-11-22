"""Chapter 30: what happens when you load Delta 4.4.1 (built for Spark 4.2) into Spark 4.3.0.

Run from code/:  uv run python ch30/delta_on_43.py   (downloads the Delta jars through Ivy once)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from common.book import ui_port
from common.lake import WORK

conf = {
    # the jars that `--packages io.delta:delta-spark_4.2_2.13:4.4.1` resolves, from the Ivy cache
    "spark.jars": ",".join(str(p) for p in sorted((Path.home() / ".ivy2.5.2" / "jars").glob("io.delta_*4.4.1.jar"))),
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}
b = SparkSession.builder.master("local[2]").appName("ch30-delta-on-43").config("spark.ui.port", ui_port())
for k, v in conf.items():
    b = b.config(k, v)
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "+ io.delta:delta-spark_4.2_2.13:4.4.1")
try:
    spark.range(3).write.format("delta").mode("overwrite").save(str(WORK / "ch30-delta-on-43"))
    print("write: OK")
except Exception as e:
    msg = str(e)
    line = next((l for l in msg.splitlines() if "Error:" in l or "Exception:" in l), msg.splitlines()[0])
    print("write failed:", line.strip()[:200])
