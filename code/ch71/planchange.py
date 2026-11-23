"""Chapter 71: watch jext.PngRule and the 4.3 transpiler's ConvertToCatalyst rule fire.

The plan change log goes to the driver's log (stderr), so run:
  uv run python ch71/planchange.py 2>&1 | sed -n '/=== Applying Rule/,/^$/p'
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ROOT, session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

JAR = Path(__file__).resolve().parent / "jext" / "target" / "jext-0.1.0.jar"
spark = session("ch71-planchange", spark_jars=str(JAR), spark_sql_extensions="jext.Extensions",
                spark_sql_experimental_optimizer_transpilePyUDFs="true")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.planChangeLog.level", "WARN")
spark.conf.set("spark.sql.planChangeLog.rules",
               "jext.PngRule,org.apache.spark.sql.catalyst.optimizer.ConvertToCatalyst")


@F.udf("boolean", useArrow=False)
def is_png(b):
    return b is not None and bytes(b[:8]) == b"\x89PNG\r\n\x1a\n"


@F.udf("long", useArrow=False)
def plus_one(x):
    return x + 1


imgs = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(
    str(ROOT / "code" / "data" / "messy" / "images"))
imgs.select(is_png("content").alias("png")).collect()
spark.range(3).select(plus_one("id").alias("y")).collect()
spark.stop()
