"""Chapter 45: metadata tables and the maintenance procedures, with the files on disk after each step.

Run tables.py, evolve.py and snapshots.py first. Run from code/:  uv run python ch45/maintenance.py
"""
import os
import shutil
import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ice import WORK, count_files, err, ice_session, tidy

spark = ice_session("ch45-maintenance", {"lake": "hadoop"})
T = "lake.nyc.trips"
TABLE = WORK / "hadoop" / "nyc" / "trips"


def on_disk(label):
    d, m = count_files(TABLE)
    live = spark.sql(f"SELECT count(*) FROM {T}.files").first()[0]
    snaps = spark.sql(f"SELECT count(*) FROM {T}.snapshots").first()[0]
    mans = spark.sql(f"SELECT count(*) FROM {T}.manifests").first()[0]
    print(f"{label:<30} disk: {d:>3} data, {m:>3} metadata | live data files {live:>3}, "
          f"manifests {mans:>2}, snapshots {snaps:>2}")


def call(sql):
    print(sql)
    for row in spark.sql(sql).collect():
        print("  " + ", ".join(f"{k}={tidy(v)}" for k, v in row.asDict().items()))


oldest = spark.sql(f"SELECT snapshot_id FROM {T}.snapshots ORDER BY committed_at LIMIT 1").first()[0]
print("== Metadata tables: the table describes itself")
for name in ["snapshots", "history", "refs", "metadata_log_entries", "manifests", "files",
             "entries", "partitions", "all_data_files", "all_manifests"]:
    print(f"  {T}.{name:<22} {spark.sql(f'SELECT * FROM {T}.{name}').count():>4} rows")
spark.sql(f"""SELECT path, added_snapshot_id, partition_spec_id AS spec,
                     added_data_files_count AS added, existing_data_files_count AS existing
              FROM {T}.manifests""").selectExpr(
    "regexp_extract(path, '[^/]+$', 0) AS manifest", "added_snapshot_id", "spec", "added",
    "existing").show(truncate=False)
spark.sql(f"""SELECT partition, record_count, file_count FROM {T}.partitions
              ORDER BY partition.tpep_pickup_datetime_month NULLS FIRST,
                       partition.tpep_pickup_datetime_day LIMIT 3""").show(truncate=False)

print("== Small files: eight small INSERTs")
on_disk("before")
for h in range(8):
    spark.sql(f"""INSERT INTO {T} SELECT * FROM {T}
                  WHERE tpep_pickup_datetime >= '2026-06-30 {h:02d}:00:00'
                    AND tpep_pickup_datetime <  '2026-06-30 {h:02d}:05:00'""")
on_disk("after 8 INSERTs")
spark.sql(f"""SELECT spec_id, count(*) AS files, sum(record_count) AS rows,
                     round(avg(file_size_in_bytes) / 1048576, 2) AS avg_mib
              FROM {T}.files GROUP BY spec_id ORDER BY spec_id""").show()

print("== rewrite_data_files: compact, and move old files to the current spec")
call(f"CALL lake.system.rewrite_data_files(table => 'nyc.trips')")
spark.sql(f"""SELECT spec_id, count(*) AS files, sum(record_count) AS rows,
                     round(avg(file_size_in_bytes) / 1048576, 2) AS avg_mib
              FROM {T}.files GROUP BY spec_id ORDER BY spec_id""").show()
on_disk("after rewrite_data_files")

print("== rewrite_manifests")
call(f"CALL lake.system.rewrite_manifests('nyc.trips')")
on_disk("after rewrite_manifests")

print("== expire_snapshots: the branch and the tag keep their snapshots")
spark.sql(f"SELECT name, type, snapshot_id FROM {T}.refs ORDER BY name").show()
now = spark.sql("SELECT CAST(current_timestamp() AS STRING)").first()[0]
call(f"CALL lake.system.expire_snapshots(table => 'nyc.trips', older_than => TIMESTAMP '{now}', "
     f"retain_last => 1)")
on_disk("after expire_snapshots")
spark.sql(f"ALTER TABLE {T} DROP BRANCH audit")
spark.sql(f"ALTER TABLE {T} DROP TAG june_load")
call(f"CALL lake.system.expire_snapshots(table => 'nyc.trips', older_than => current_timestamp(), "
     f"retain_last => 1)")
on_disk("after DROP BRANCH/TAG, expire")
print("time travel to an expired snapshot:")
try:
    spark.sql(f"SELECT count(*) FROM {T} VERSION AS OF {oldest}").first()
except Exception as e:
    print("  " + err(e, 110))


print("\n== Old metadata.json files: a table property removes them at commit")
meta_json = lambda: len(list((TABLE / "metadata").glob("*.metadata.json")))
print("metadata.json files:", meta_json())
spark.sql(f"""ALTER TABLE {T} SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true',
                                                  'write.metadata.previous-versions-max' = '5')""")
print("after one commit with previous-versions-max = 5:", meta_json())

print("\n== remove_orphan_files: a file that no metadata refers to")
stray = TABLE / "data" / "left-by-a-failed-job.parquet"
shutil.copy(next((TABLE / "data").rglob("*.parquet")), stray)
call(f"CALL lake.system.remove_orphan_files(table => 'nyc.trips', dry_run => true)")
try:
    spark.sql(f"CALL lake.system.remove_orphan_files(table => 'nyc.trips', "
              f"older_than => current_timestamp())").collect()
except Exception as e:
    print("older_than => now:\n  " + err(e, 300).replace(". ", ".\n  "))
four_days_ago = time.time() - 4 * 86400
os.utime(stray, (four_days_ago, four_days_ago))
print("\nthe same file, with its modification time set four days back:")
call(f"CALL lake.system.remove_orphan_files(table => 'nyc.trips', dry_run => true)")
spark.sql(f"CALL lake.system.remove_orphan_files(table => 'nyc.trips')").collect()
on_disk("after remove_orphan_files")
print("\nrows now:", f"{spark.table(T).count():,}")
