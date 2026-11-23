"""Chapter 71, Step 5: local disk I/O encryption. Runs one shuffle and one DISK_ONLY cache with
spark.io.encryption.enabled off or on, then prints the first bytes of the files Spark left in
spark.local.dir while the application still runs.

    python io_encryption.py false|true
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import TRIPS, session, work  # noqa: E402

from pyspark import StorageLevel  # noqa: E402

on = sys.argv[1]
local = work(f"ch71-localdir-{on}")
spark = session(f"ch71-io-{on}", spark_local_dir=str(local), spark_io_encryption_enabled=on,
                spark_sql_shuffle_partitions="4", spark_sql_adaptive_enabled="false")
df = spark.read.parquet(str(TRIPS)).select("PULocationID", "payment_type")
df.groupBy("PULocationID").count().collect()                     # writes shuffle files
spark.range(0, 200_000).selectExpr("concat('zone-', id) AS name").persist(StorageLevel.DISK_ONLY).count()

print(f"spark.io.encryption.enabled={on}")
for kind, pattern in (("shuffle data", "shuffle_0_*_0.data"), ("cached block", "rdd_*_0")):
    f = sorted(local.rglob(pattern))[0]
    head = f.read_bytes()[:24]
    text = "".join(chr(b) if 32 <= b < 127 else "." for b in head)
    print(f"  {kind:<13} {f.name:<22} {f.stat().st_size:>9,} B  {head.hex(' ', 4)}  |{text}|")
spark.stop()
