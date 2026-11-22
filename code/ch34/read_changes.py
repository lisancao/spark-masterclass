"""Chapter 34: reading changes on Spark 4.3.0. The engine-level CHANGES clause, the Iceberg
changelog view, and a changelog built by hand from two snapshots.

Run from code/:  uv run python ch34/read_changes.py
"""
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.lake import lake_session, WORK

warehouse = WORK / "ch34-changes"
shutil.rmtree(warehouse, ignore_errors=True)
spark = lake_session("ch34-changes", warehouse)
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE TABLE lake.shop.inventory (sku STRING, qty INT) USING iceberg")
spark.sql("INSERT INTO lake.shop.inventory VALUES ('widget', 100), ('gadget', 50), ('mug', 7)")
spark.sql("UPDATE lake.shop.inventory SET qty = 90 WHERE sku = 'widget'")
spark.sql("DELETE FROM lake.shop.inventory WHERE sku = 'gadget'")
snaps = [r.snapshot_id for r in spark.sql(
    "SELECT snapshot_id FROM lake.shop.inventory.snapshots ORDER BY committed_at").collect()]
print("snapshots, oldest first:", snaps, "\n")

print("1. The engine-level CDC surface (CHANGES, DataFrameReader.changes):")
spark.sql("CREATE TABLE default.plain_inventory (sku STRING, qty INT) USING parquet")
for label, run in [
    ("CHANGES on a Parquet table", lambda: spark.sql(
        "SELECT * FROM default.plain_inventory CHANGES FROM VERSION 0").collect()),
    ("CHANGES on an Iceberg table", lambda: spark.sql(
        f"SELECT * FROM lake.shop.inventory CHANGES FROM VERSION {snaps[0]}").collect()),
    ("read.changes() on an Iceberg table", lambda: spark.read.option(
        "startingVersion", str(snaps[0])).changes("lake.shop.inventory").collect()),
]:
    try:
        run()
        print(f"  {label}: worked")
    except Exception as e:
        print(f"  {label}:\n    {e.getCondition()}: {str(e).split('] ', 1)[1].split(' SQLSTATE')[0]}")

print("\n2. Iceberg's own changelog view (a stored procedure):")
spark.sql("""CALL lake.system.create_changelog_view(
               table => 'shop.inventory', changelog_view => 'inventory_changes',
               compute_updates => true, identifier_columns => array('sku'))""").collect()
spark.sql("SELECT * FROM inventory_changes ORDER BY _change_ordinal, sku").show()

print("3. The same changes from two snapshots, with time travel and a full outer join:")
before = spark.sql(f"SELECT * FROM lake.shop.inventory VERSION AS OF {snaps[0]}").alias("b")
after = spark.sql(f"SELECT * FROM lake.shop.inventory VERSION AS OF {snaps[-1]}").alias("a")
diff = (before.join(after, F.col("b.sku") == F.col("a.sku"), "full_outer")
        .select(F.coalesce("a.sku", "b.sku").alias("sku"),
                F.col("b.qty").alias("qty_before"), F.col("a.qty").alias("qty_after"),
                F.when(F.col("b.sku").isNull(), "insert")
                 .when(F.col("a.sku").isNull(), "delete")
                 .when(~F.col("a.qty").eqNullSafe(F.col("b.qty")), "update")
                 .alias("_change_type"))
        .where("_change_type IS NOT NULL"))
diff.orderBy("sku").show()
