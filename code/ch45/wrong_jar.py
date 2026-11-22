"""Chapter 45: the newest released runtime (Iceberg 1.12.0, built for Spark 4.1) on Spark 4.3.0.

Run from code/:  uv run python ch45/wrong_jar.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session
from ice import WORK, tidy

OFFICIAL = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"   # from Maven Central

spark = session("ch45-wrong-jar", **{
    "spark.jars.packages": OFFICIAL,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.type": "hadoop",
    "spark.sql.catalog.lake.warehouse": f"file://{WORK}/wrong-jar",
})
print(f"Spark {spark.version}, runtime {OFFICIAL}")
try:
    spark.sql("CREATE TABLE lake.db.t (id BIGINT) USING iceberg")
    print("CREATE TABLE worked")
except Exception as e:
    print("CREATE TABLE failed:")
    cause = next(l for l in tidy(e).splitlines() if "Error" in l and "o4" not in l)
    name, _, msg = cause.lstrip(": ").partition(": ")
    print(f"  {name}:")
    first, _, rest = msg.partition(", because ")   # wrap the long message for the page
    print(f"    {first},\n    because {rest.split(' (')[0]}")
