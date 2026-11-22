"""Chapter 76, Step 2: the same SRID check over Spark Connect (start it with ch76/connect_server.sh).

Run from code/:  uv run python ch76/connect_srid.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from geo76 import attempt

spark = SparkSession.builder.remote("sc://localhost:15103").getOrCreate()   # ch76/connect_server.sh start
PT = "X'0101000000000000000000F03F0000000000000040'"
print("Spark Connect session:", type(spark).__module__, spark.version)
for srid in [3857, 2263]:
    df = spark.sql(f"SELECT st_geomfromwkb({PT}, {srid}) AS g")
    attempt(f"{srid}: df.schema", lambda d=df: d.schema[0].dataType, 16)
    attempt(f"{srid}: collect()", lambda d=df: d.collect()[0].g.getSrid(), 16)
    attempt(f"{srid}: st_srid in SQL", lambda: spark.sql(
        f"SELECT st_srid(st_geomfromwkb({PT}, {srid}))").first()[0], 16)
