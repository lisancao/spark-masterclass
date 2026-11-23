"""Chapter 19, Step 3: the same join-then-group plan on pyspark 4.2.0 (run in a separate venv with pyspark==4.2.0):
  BOOK_DATA=... <venv-4.2.0>/bin/python ch19/plan42.py
"""
import os, re, pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[4]").config("spark.ui.enabled","false").config("spark.sql.autoBroadcastJoinThreshold","-1").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
D=os.environ["BOOK_DATA"]
spark.read.parquet(D+"/nyc-taxi/yellow").createOrReplaceTempView("trips")
spark.read.option("header",True).option("inferSchema",True).csv(D+"/nyc-taxi/taxi_zone_lookup.csv").createOrReplaceTempView("zones")
df=spark.sql("SELECT t.PULocationID, count(*) AS trips FROM trips t JOIN zones z ON t.PULocationID = z.LocationID GROUP BY t.PULocationID")
df.collect()
import io, contextlib
b=io.StringIO()
with contextlib.redirect_stdout(b): df.explain()
print(pyspark.__version__)
for l in b.getvalue().split("Initial Plan")[0].splitlines():
    if "Aggregate" in l or "SortMergeJoin" in l: print(re.sub(r"#\d+","",l)[:100])
