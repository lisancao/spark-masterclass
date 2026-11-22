"""Chapter 32: the two product tables built the imperative way, in a Classic session.

Run from code/:  uv run python ch32/imperative.py
"""
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import DATA, session

wh = DATA / "work" / "part06" / "ch32-imperative"
shutil.rmtree(wh, ignore_errors=True)
spark = session("ch32-imperative", **{"spark.sql.warehouse.dir": str(wh)})
products_csv = str(Path(__file__).resolve().parents[1] / "data" / "retail" / "products.csv")


def build():
    # The order is ours to get right: products first, then the summary that reads it.
    (spark.read.option("header", True)
     .schema("product_id STRING, name STRING, category STRING, list_price DECIMAL(10,2)")
     .csv(products_csv)
     .write.saveAsTable("products"))
    (spark.table("products").groupBy("category")
     .agg(F.count("*").alias("products"), F.avg("list_price").alias("avg_list_price"))
     .write.saveAsTable("products_by_category"))


build()
spark.table("products_by_category").orderBy("category").show()
try:
    build()   # the second run of the same job
except Exception as e:
    print("second run:", e.getCondition())
