"""Customer change events, applied twice: as the latest state (SCD Type 1) and as history (SCD Type 2)."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CHANGE_SCHEMA = "customer_id STRING, name STRING, city STRING, op STRING, seq INT"


@dp.table(comment="Customer change events as they arrived")
def customer_changes():
    return (spark.readStream.schema(CHANGE_SCHEMA).json("data/landing")
            .select("*", F.col("_metadata.file_name").alias("_source_file")))


dp.create_streaming_table("customers_current", comment="SCD Type 1: the latest row per customer")

dp.create_auto_cdc_flow(
    target="customers_current",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "seq", "_source_file"],
    stored_as_scd_type=1,
)

dp.create_streaming_table("customers_history", comment="SCD Type 2: every version of every customer")

dp.create_auto_cdc_flow(
    target="customers_history",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "seq", "_source_file"],
    stored_as_scd_type=2,
)
