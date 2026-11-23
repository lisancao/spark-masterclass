"""Chapter 94: tests for fares.py with pyspark.testing."""
import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from fares import band, clean_trips, fare_band, zone_summary


@pytest.mark.parametrize("fare, expected", [(None, "invalid"), (-1.0, "invalid"), (9.99, "under $10"),
                                            (10.0, "$10 to $30"), (30.0, "$10 to $30"), (30.01, "over $30")])
def test_band_without_spark(fare, expected):
    assert band(fare) == expected


def test_fare_band_udf(spark):
    df = spark.createDataFrame([(9.99,), (30.0,), (None,)], "fare double")
    assertDataFrameEqual(df.select(fare_band("fare").alias("band")),
                         [Row(band="under $10"), Row(band="$10 to $30"), Row(band="invalid")])


def test_clean_trips_rows(trips):
    out = clean_trips(trips).select("PULocationID", "fare_amount", "minutes")
    assertDataFrameEqual(out, [Row(PULocationID=132, fare_amount=70.0, minutes=30.0),   # any order
                               Row(PULocationID=132, fare_amount=12.5, minutes=12.0)])


def test_zone_summary_schema(trips):
    expected = StructType([StructField("zone", IntegerType()), StructField("trips", LongType()),
                           StructField("avg_fare", DoubleType()), StructField("avg_tip_pct", DoubleType())])
    assertSchemaEqual(zone_summary(clean_trips(trips)).schema, expected)    # nullability ignored


def test_zone_summary_values(trips):
    out = zone_summary(clean_trips(trips))
    assertDataFrameEqual(out, [Row(zone=132, trips=2, avg_fare=41.25, avg_tip_pct=24.0000001)],
                         rtol=1e-5)    # 100 * 3.0 / 12.5 = 24.0
