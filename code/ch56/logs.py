"""Chapter 56's running example: the web server log, parsed with a regex and written to Parquet.

Imported by every script in ch56. 200 requests; 98 have status 200, and 31 of those are API calls.
"""
import sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import ROOT

ACCESS_LOG = ROOT / "code" / "data" / "logs" / "access.log"
LOG = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+)'


def access_parquet(spark):
    """Parse the log once, write it to Parquet in a temp dir, and read it back (a real file scan)."""
    out = tempfile.mkdtemp(prefix="ch56-") + "/access"
    raw = spark.read.text(str(ACCESS_LOG))
    (raw.select(F.regexp_extract("value", LOG, 1).alias("ip"),
                F.regexp_extract("value", LOG, 4).alias("path"),
                F.regexp_extract("value", LOG, 5).cast("int").alias("status"))
        .coalesce(1).write.parquet(out))
    return spark.read.parquet(out)


def _area(path):
    # "/api/orders/42?session=s9" -> "api"; "/?session=s1" -> "home"
    return path.split("?")[0].split("/")[1] or "home"


# Pickle this module's functions by value, so Python workers don't need to import `logs`.
from pyspark import cloudpickle
cloudpickle.register_pickle_by_value(sys.modules[__name__])
