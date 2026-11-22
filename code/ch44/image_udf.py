"""Chapter 44, Step 11: a default @udf inside the official apache/spark:4.3.0-python3 image.
Run by image.sh, inside the container."""
import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
print("python", sys.version.split()[0])
for arrow in ("true", "false"):
    spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", arrow)
    plus = F.udf(lambda x: x + 1, "long")
    try:
        print(f"pythonUDF.arrow.enabled={arrow}:", [r[0] for r in spark.range(3).select(plus("id")).collect()])
    except Exception as e:
        line = next((l.strip() for l in str(e).splitlines() if "Error" in l and "pyarrow" in l), str(e)[:150])
        print(f"pythonUDF.arrow.enabled={arrow}: failed:", line[:150])
spark.stop()
