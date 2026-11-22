"""The SCD Type 1 flow of ch32/crm, with its target in the default catalog (a Parquet table)."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CHANGE_SCHEMA = "customer_id STRING, name STRING, city STRING, op STRING, seq INT"


@dp.table
def customer_changes():
    return spark.readStream.schema(CHANGE_SCHEMA).json("data/customer_changes")


dp.create_streaming_table("customers_current")
dp.create_auto_cdc_flow(
    target="customers_current",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "seq"],
    stored_as_scd_type=1,
)
