"""Chapter 34: what Auto CDC keeps next to its targets (the auxiliary state table and the commits).

Run from code/ after ch34/two_drops.sh.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.lake import lake_session, WORK, tidy

spark = lake_session("ch34-internals", WORK / "ch34-crm" / "lake-warehouse")
spark.sparkContext.setLogLevel("ERROR")

print("tables in lake.crm:")
spark.sql("SHOW TABLES IN lake.crm").select("tableName").show(truncate=False)

print("the SCD Type 2 auxiliary state table:")
aux = spark.table("lake.crm.__spark_autocdc_aux_state_customers_history")
print(aux.schema.simpleString())
aux.orderBy("customer_id").show(truncate=False)

print("the SCD Type 1 auxiliary state table (tombstones):")
spark.table("lake.crm.__spark_autocdc_aux_state_customers_current").show(truncate=False)

print("commits on customers_history (Iceberg snapshots):")
spark.sql("""SELECT snapshot_id, operation,
                    summary['added-records'] AS added, summary['deleted-records'] AS deleted,
                    summary['total-records'] AS total
             FROM lake.crm.customers_history.snapshots ORDER BY committed_at""").show(truncate=False)

print("table properties on customers_history:")
for r in spark.sql("SHOW TBLPROPERTIES lake.crm.customers_history").collect():
    print(f"  {r.key} = {tidy(r.value)}")
