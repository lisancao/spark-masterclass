"""Chapter 18: make Connect the default for a local script, with spark.api.mode (Spark 4.0 and later).

The full pyspark package starts a local Connect server for the application and stops it at the end.
Run from code/:  uv run python ch18/api_mode.py
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F
from common.book import TRIPS

spark = (SparkSession.builder.master("local[*]")
         .config("spark.api.mode", "connect")       # the default is "classic"
         .getOrCreate())
print("session class:", type(spark).__module__ + "." + type(spark).__name__)
trips = spark.read.parquet(str(TRIPS))
print("trips:", f"{trips.count():,}", "| server-side session:", spark.session_id[:8])
try:
    trips.select("revenue")
    print("select('revenue') raised nothing: analysis waits for an action, as in any Connect session")
finally:
    spark.stop()
