"""Chapter 46: look at the tables of the taxi_etl pipeline through the same Connect server.

  python look.py sc://localhost:15104
"""
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.remote(sys.argv[1]).getOrCreate()
spark.sql("SHOW TABLES").select("tableName").show()
for t in ["raw_trips", "clean_trips", "zones", "borough_daily"]:
    print(f"{t:14} {spark.table(t).count():>9,} rows")
print()
spark.sql("""SELECT * FROM borough_daily
             WHERE borough IN ('Manhattan', 'Queens', 'Brooklyn')
             ORDER BY pickup_date, trips DESC""").show()
