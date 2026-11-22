"""Chapter 10: which aggregate operator a GROUP BY gets under each collation.

Run from code/:  uv run python ch10/collation_plan.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan

spark = session("ch10-collation-plan")
cities = spark.createDataFrame([("Montréal",), ("MONTREAL",), ("Toronto",)], "city STRING")
print("aggregate operators of groupBy(collate(city, c)).count():")
for coll in ["UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI_AI"]:
    p = plan(cities.groupBy(F.collate("city", coll).alias("c")).count(), "simple")
    print(f"  {coll:<14} {re.findall(r'(HashAggregate|SortAggregate|ObjectHashAggregate)', p)}")
