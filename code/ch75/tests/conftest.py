"""One session for the whole test run. MODE=classic (default) or MODE=connect.

classic: a local[2] session with a small number of shuffle partitions.
connect: a Connect session against SPARK_CONNECT_URL, or a local Connect server that pyspark
         starts in this process with .remote("local[2]").
"""
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # import fares


@pytest.fixture(scope="session")
def spark():
    mode = os.environ.get("MODE", "classic")
    if mode == "connect":
        url = os.environ.get("SPARK_CONNECT_URL", "local[2]")
        b = SparkSession.builder.remote(url)
    else:
        b = (SparkSession.builder.master("local[2]")
             .config("spark.ui.enabled", "false"))
    s = (b.appName("ch75-tests")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate())
    fares_py = str(Path(__file__).resolve().parents[1] / "fares.py")
    if mode == "connect":
        s.addArtifacts(fares_py, pyfile=True)     # the server's Python workers import fares
    else:
        s.sparkContext.addPyFile(fares_py)        # the executors' Python workers import fares
    yield s
    s.stop()


@pytest.fixture
def trips(spark):
    """Four hand-made trips with the columns that fares.py reads."""
    return spark.createDataFrame(
        [("2026-06-01 08:00:00", "2026-06-01 08:12:00", 2.1, 132, 1, 12.5, 3.0),
         ("2026-06-01 09:00:00", "2026-06-01 09:30:00", 17.0, 132, 2, 70.0, 0.0),
         ("2026-06-01 10:00:00", "2026-06-01 10:05:00", 0.0, 161, 1, 5.0, 1.0),
         ("2026-06-01 11:00:00", "2026-06-01 11:20:00", 3.4, 161, 1, -8.0, 0.0)],
        "pickup string, dropoff string, trip_distance double, PULocationID int, payment_type int, "
        "fare_amount double, tip_amount double",
    ).selectExpr("CAST(pickup AS TIMESTAMP) AS tpep_pickup_datetime",
                 "CAST(dropoff AS TIMESTAMP) AS tpep_dropoff_datetime", "*").drop("pickup", "dropoff")
