"""Chapter 6: check the install, then start a first session and look at the data.

Run from code/:  uv run python ch06/check_env.py
"""
import platform, shutil, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyspark
java = shutil.which("java")
jv = subprocess.run([java, "-version"], capture_output=True, text=True).stderr.splitlines()[0] if java else "no java on PATH"
print(f"python  {platform.python_version()}\npyspark {pyspark.__version__}\njava    {jv}")

from common.book import session, TRIPS
spark = session("ch06-first-session")
print(f"\nSpark {spark.version} on {spark.sparkContext.master}, "
      f"{spark.sparkContext.defaultParallelism} cores, UI at {spark.sparkContext.uiWebUrl}")

trips = spark.read.parquet(str(TRIPS))
print(f"\n{trips.count():,} trips, {len(trips.columns)} columns")
trips.printSchema()
trips.select("tpep_pickup_datetime", "PULocationID", "DOLocationID",
             "trip_distance", "fare_amount", "tip_amount", "payment_type").show(5)
trips.describe("trip_distance", "fare_amount", "passenger_count").show()
