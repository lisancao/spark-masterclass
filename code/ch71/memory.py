"""Chapter 71: execution memory takes space from storage memory, measured.

Local mode, a 1 GiB driver heap: the unified pool is (heap - 300 MiB) * 0.6. The script fills
storage with a MEMORY_ONLY cache, then runs a large sort that needs execution memory, and reads
the cached partitions and the spill from the REST API before and after.

Classic only. Run from code/:  uv run python ch71/memory.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark import StorageLevel
from pyspark.sql import functions as F
from common.book import session, ui_api, TRIPS

spark = session("ch71-memory", spark_master="local[4]", spark_ui_showConsoleProgress="false",
                spark_sql_adaptive_enabled="false", spark_sql_shuffle_partitions="4")
sc = spark.sparkContext
MiB = 2**20


def pool():
    e = [x for x in ui_api("executors") if x["id"] == "driver"][0]
    return e["maxMemory"] / MiB, e["memoryUsed"] / MiB


def cached():
    r = ui_api("storage/rdd")
    if not r:
        return "nothing cached"
    r = r[0]
    return (f"{r['numCachedPartitions']}/{r['numPartitions']} partitions cached, "
            f"{r['memoryUsed'] / MiB:.1f} MiB in memory")


print(f"unified pool (maxMemory): {pool()[0]:.1f} MiB, storageFraction 0.5 protects {pool()[0] / 2:.1f} MiB")
trips = spark.read.parquet(str(TRIPS))
wide = trips.unionAll(trips).repartition(32).persist(StorageLevel.MEMORY_ONLY)   # about 325 MiB
sc.setJobDescription("fill storage")
wide.count()
print(f"after the cache:  {cached()}   storage in use {pool()[1]:.1f} MiB")

sc.setJobDescription("big sort")
trips.orderBy("trip_distance", "tpep_pickup_datetime").write.format("noop").mode("overwrite").save()
print(f"after the sort:   {cached()}   storage in use {pool()[1]:.1f} MiB")
job = [j for j in ui_api("jobs") if j.get("description") == "big sort"]
spill = sum(st["memoryBytesSpilled"] for j in job for sid in j["stageIds"]
            for st in ui_api(f"stages/{sid}") if st["status"] == "COMPLETE")
print(f"spill during the sort: {spill / MiB:.1f} MiB (in memory size)")

