"""Chapter 33: the Iceberg snapshots of silver_orders after the failed refresh, and the last good one.

Run from code/ after ch33/gate.sh.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.lake import lake_session, WORK

spark = lake_session("ch33-snapshots", WORK / "ch33-quality-ice" / "lake-warehouse")
snaps = spark.sql("""SELECT snapshot_id, operation, summary['added-records'] AS added,
                            summary['deleted-records'] AS deleted, summary['total-records'] AS total
                     FROM lake.retail.silver_orders.snapshots ORDER BY committed_at""")
snaps.show(truncate=False)
rows = snaps.collect()
good = [r for r in rows if r.total not in (None, "0")][-1]
n = spark.sql(f"SELECT count(*) FROM lake.retail.silver_orders VERSION AS OF {good.snapshot_id}").first()[0]
print(f"rows now: {spark.table('lake.retail.silver_orders').count()}   rows in the last good snapshot: {n}")
