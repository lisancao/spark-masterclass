"""Chapter 31: snapshots, time travel, row-level deletes, tags and branches on lake.nyc.trips.

Run tables.py and evolve.py first. Run from code/:  uv run python ch31/snapshots.py
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ice import ice_session, err

spark = ice_session("ch31-snapshots", {"lake": "hadoop"})
T = "lake.nyc.trips"


def snapshots():
    spark.sql(f"""SELECT snapshot_id, parent_id, operation,
                         summary['added-data-files'] AS add_files, summary['deleted-data-files'] AS del_files,
                         summary['added-delete-files'] AS add_deletes, summary['total-records'] AS total_rows
                  FROM {T}.snapshots ORDER BY committed_at""").show(truncate=False)


print("== The snapshots so far")
snapshots()
first, second = [r[0] for r in spark.sql(f"SELECT snapshot_id FROM {T}.snapshots ORDER BY committed_at").collect()]
t_between = spark.sql(f"SELECT committed_at FROM {T}.snapshots WHERE snapshot_id = {first}").first()[0]

print("== Time travel")
print("now:                ", spark.table(T).count())
print(f"VERSION AS OF first: ", spark.sql(f"SELECT count(*) FROM {T} VERSION AS OF {first}").first()[0])
print(f"TIMESTAMP AS OF '{t_between}':",
      spark.sql(f"SELECT count(*) FROM {T} TIMESTAMP AS OF '{t_between}'").first()[0])
print("option snapshot-id: ", spark.read.option("snapshot-id", first).table(T).count())
print("the old schema too: ", spark.sql(f"SELECT * FROM {T} VERSION AS OF {first}").columns[8:10])

print("\nnew in 4.3, a write with a time-travel option:")
try:
    spark.table(T).limit(1).writeTo(T).option("versionAsOf", str(first)).append()
    print("  append worked")
except Exception as e:
    print("  " + err(e, 100))

print("\n== DELETE, copy-on-write (the default)")
neg = spark.sql(f"SELECT count(*) FROM {T} WHERE fare < 0").first()[0]
print(f"rows with fare < 0: {neg:,}")
spark.sql(f"DELETE FROM {T} WHERE fare < 0")
print("== DELETE, merge-on-read")
spark.sql(f"ALTER TABLE {T} SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')")
spark.sql(f"DELETE FROM {T} WHERE trip_distance > 500")
snapshots()
spark.sql(f"SELECT content, file_format, count(*) AS files, sum(record_count) AS records "
          f"FROM {T}.delete_files GROUP BY content, file_format").show()

print("== Tags and branches")
spark.sql(f"ALTER TABLE {T} CREATE TAG june_load AS OF VERSION {first} RETAIN 365 DAYS")
spark.sql(f"ALTER TABLE {T} CREATE BRANCH audit")
spark.sql(f"INSERT INTO {T}.branch_audit SELECT * FROM {T} WHERE tpep_pickup_datetime "
          f">= '2026-06-30' LIMIT 1000")
print("main:              ", spark.table(T).count())
print("branch audit:      ", spark.table(f"{T}.branch_audit").count())
print("tag june_load:     ", spark.table(f"{T}.tag_june_load").count())
print("VERSION AS OF 'audit':", spark.sql(f"SELECT count(*) FROM {T} VERSION AS OF 'audit'").first()[0])
spark.sql(f"SELECT name, type, snapshot_id, max_reference_age_in_ms FROM {T}.refs ORDER BY name").show()

print("== Write-audit-publish: spark.wap.branch sends plain INSERTs to the branch")
spark.sql(f"ALTER TABLE {T} SET TBLPROPERTIES ('write.wap.enabled' = 'true')")
spark.conf.set("spark.wap.branch", "audit")
spark.sql(f"INSERT INTO {T} SELECT * FROM {T}.branch_audit LIMIT 10")
spark.conf.unset("spark.wap.branch")
print("main:", spark.table(T).count(), " audit:", spark.table(f"{T}.branch_audit").count())
spark.sql(f"CALL lake.system.fast_forward('nyc.trips', 'main', 'audit')").show(truncate=False)
print("main after fast_forward:", spark.table(T).count())

print("== Roll back main to the second snapshot")
spark.sql(f"CALL lake.system.rollback_to_snapshot('nyc.trips', {second})").show()
print("main:", spark.table(T).count())
spark.sql(f"SELECT snapshot_id, is_current_ancestor FROM {T}.history ORDER BY made_current_at").show()
spark.sql(f"SELECT count(*) AS metadata_files FROM {T}.metadata_log_entries").show()
