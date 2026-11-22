"""Chapter 32: print the Auto CDC targets of the crm pipeline, read straight from the Iceberg catalog.

Run from code/ after common/pipeline.sh has run ch32/crm (see two_drops.sh).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.lake import lake_session, WORK

work = WORK / sys.argv[1] if len(sys.argv) > 1 else WORK / "ch32-crm"
spark = lake_session("ch32-show", work / "lake-warehouse")
spark.sparkContext.setLogLevel("ERROR")

print("customer_changes (bronze), by file:")
spark.sql("SELECT _source_file, count(*) AS events, min(seq) AS min_seq, max(seq) AS max_seq "
          "FROM lake.crm.customer_changes GROUP BY 1 ORDER BY 1").show(truncate=False)
print("customers_current (SCD Type 1):")
spark.table("lake.crm.customers_current").orderBy("customer_id").show(truncate=False)
print("customers_history (SCD Type 2):")
(spark.table("lake.crm.customers_history")
 .orderBy("customer_id", "__START_AT").drop("__spark_autocdc_metadata").show(truncate=False))
print("customers_city_history (SQL, TRACK HISTORY ON (city)):")
(spark.table("lake.crm.customers_city_history")
 .orderBy("customer_id", "__START_AT").drop("__spark_autocdc_metadata").show(truncate=False))
