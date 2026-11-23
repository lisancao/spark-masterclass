"""Chapter 18: the same query from an environment with only pyspark-client (no JVM, no jars).

  uv venv /tmp/thin && VIRTUAL_ENV=/tmp/thin uv pip install pyspark-client==4.3.0
  /tmp/thin/bin/python ch18/thin_client.py
"""
import importlib.metadata, importlib.util, os, pathlib
import pyspark
from pyspark.sql import SparkSession, functions as F

home = pathlib.Path(pyspark.__file__).parent
print("pyspark-client", importlib.metadata.version("pyspark-client"))
print("pyspark/jars exists:", (home / "jars").exists(), "| py4j importable:",
      importlib.util.find_spec("py4j") is not None,)
url = f"sc://localhost:{os.environ.get('CONNECT_PORT', '15002')}"
with SparkSession.builder.remote(url).create() as spark:
    trips = spark.read.parquet(os.environ["BOOK_DATA"] + "/nyc-taxi/yellow")
    print("server", spark.version, "| trips:", f"{trips.count():,}")
