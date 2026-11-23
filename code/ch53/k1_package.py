"""Step 1: the Kafka connector is a package. What Spark resolves for it, and what it gives you.

Run from code/:  uv run python ch53/k1_package.py
"""
import subprocess
import sys
from pathlib import Path

from k89 import spark_session, error_text, KAFKA_PACKAGE, PLAIN

print("1. Without the package:")
probe = ("from pyspark.sql import SparkSession\n"
         "s = SparkSession.builder.master('local[1]').config('spark.ui.enabled','false').getOrCreate()\n"
         "s.sparkContext.setLogLevel('OFF')\n"
         "try:\n    s.read.format('kafka').option('kafka.bootstrap.servers','x:1').option('subscribe','t').load()\n"
         "except Exception as e:\n    print(type(e).__name__, str(e).splitlines()[0][:170])\n")
r = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True)
print("  ", r.stdout.strip())

spark = spark_session("ch53-package")
print(f"\n2. spark.jars.packages = {KAFKA_PACKAGE}")
print("   jars that Spark resolved and put on the classpath:")
for p in sorted(spark.sparkContext.getConf().get("spark.jars").split(",")):
    print("    ", Path(p).name)

print("\n3. The source schema, with and without includeHeaders:")
base = spark.read.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "trips")
print("   default:        ", ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in base.load().schema))
hdr = base.option("includeHeaders", "true").load().schema["headers"]
print("   includeHeaders: + headers", hdr.dataType.simpleString())
spark.stop()
