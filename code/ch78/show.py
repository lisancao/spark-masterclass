"""Chapter 78: print the gold table that the pipeline wrote (run from the pipeline folder)."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
rows = (spark.read.parquet("spark-warehouse/gold_daily_revenue").orderBy("day")
        .selectExpr("CAST(day AS STRING) AS day", "revenue").collect())   # Python dates stop at year 9999
print("gold_daily_revenue:", [(r.day, r.revenue) for r in rows][:3], f"... {len(rows)} rows")
spark.stop()
