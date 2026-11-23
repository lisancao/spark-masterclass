"""Chapter 84: INSERT OVERWRITE on a partitioned table, static against dynamic
(spark.sql.sources.partitionOverwriteMode), with the files that each run keeps, adds and deletes.

Run from code/:  uv run python ch84/partition_overwrite.py > ch84/output/partition_overwrite.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, TRIPS  # noqa: E402
from ddl import runner  # noqa: E402

W = work("ch84-overwrite")
WH = W / "warehouse"
spark = session("ch84-overwrite", **{"spark.sql.warehouse.dir": str(WH), "spark.sql.shuffle.partitions": "1"})
q = runner(spark)
spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips_raw")
spark.read.option("header", True).csv(str(TRIPS.parent / "taxi_zone_lookup.csv")).createOrReplaceTempView("zones")

DAY = """SELECT z.Borough AS borough, count(*) AS trips, to_date(t.tpep_pickup_datetime) AS day
         FROM trips_raw t JOIN zones z ON t.PULocationID = z.LocationID
         WHERE t.tpep_pickup_datetime >= '{a}' AND t.tpep_pickup_datetime < '{b}'
         GROUP BY z.Borough, to_date(t.tpep_pickup_datetime)"""
THREE_DAYS = DAY.format(a="2026-06-01", b="2026-06-04")
RERUN = DAY.format(a="2026-06-02", b="2026-06-03")          # one day, computed again


def snapshot(t):
    root = WH / t
    return {str(p.relative_to(root)) for p in root.rglob("*.parquet")}


def diff(before, after):
    days = sorted({p.split("/")[0] for p in before | after})
    for d in days:
        b = {p for p in before if p.startswith(d + "/")}
        a = {p for p in after if p.startswith(d + "/")}
        state = ("kept" if a == b else "replaced" if a and b else "deleted" if b else "added")
        print(f"    {d:<15} files before {len(b)}  after {len(a)}  -> {state}")


def fresh(t):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"CREATE TABLE {t} (borough STRING, trips BIGINT, day DATE) USING parquet PARTITIONED BY (day)")
    spark.sql(f"INSERT INTO {t} {THREE_DAYS}")


def run(label, mode, sql, t="by_day"):
    fresh(t)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", mode)
    before = snapshot(t)
    print(f"\n== {label}")
    print(f"   partitionOverwriteMode = {mode}")
    q(sql.format(t=t), show=False)
    diff(before, snapshot(t))
    rows = spark.sql(f"SELECT day, count(*) AS boroughs, sum(trips) AS trips FROM {t} GROUP BY day ORDER BY day")
    rows.show(truncate=False)


print("== the table before each run: three days, one partition folder per day")
fresh("by_day")
q("SELECT day, count(*) AS boroughs, sum(trips) AS trips FROM by_day GROUP BY day ORDER BY day")
q("SET spark.sql.sources.partitionOverwriteMode")

run("1. INSERT OVERWRITE with no PARTITION clause, static mode", "STATIC",
    "INSERT OVERWRITE TABLE {t} " + RERUN)
run("2. the same statement, dynamic mode", "DYNAMIC",
    "INSERT OVERWRITE TABLE {t} " + RERUN)
run("3. a static PARTITION value: the same result in the two modes", "STATIC",
    "INSERT OVERWRITE TABLE {t} PARTITION (day = '2026-06-02') "
    + RERUN.replace(", to_date(t.tpep_pickup_datetime) AS day", ""))
run("4. an empty result, dynamic mode: no partition changes", "DYNAMIC",
    "INSERT OVERWRITE TABLE {t} " + DAY.format(a="2027-01-01", b="2027-01-02"))
run("5. an empty result, static mode: the table is empty", "STATIC",
    "INSERT OVERWRITE TABLE {t} " + DAY.format(a="2027-01-01", b="2027-01-02"))

print("\n== 6. the DataFrame API: insertInto and the partitionOverwriteMode option")
for session_mode, option in [("STATIC", "dynamic"), ("DYNAMIC", None)]:
    fresh("by_day")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", session_mode)
    before = snapshot("by_day")
    w = spark.sql(RERUN).select("borough", "trips", "day").write.mode("overwrite")
    if option:
        w = w.option("partitionOverwriteMode", option)
    w.insertInto("by_day")
    print(f"   session: {session_mode}, writer option: {option}")
    diff(before, snapshot("by_day"))
fresh("by_day")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")
before = snapshot("by_day")
q("INSERT OVERWRITE TABLE by_day WITH ('partitionOverwriteMode' = 'dynamic') " + RERUN, show=False)
print("   session: STATIC, INSERT ... WITH ('partitionOverwriteMode' = 'dynamic')")
diff(before, snapshot("by_day"))

print("\n== 7. a path with partitionBy, mode('overwrite')")
out = W / "paths" / "by_day"
spark.sql(THREE_DAYS).write.partitionBy("day").parquet(str(out))
for mode in ["static", "dynamic"]:
    spark.sql(THREE_DAYS).write.mode("overwrite").partitionBy("day").parquet(str(out))
    spark.sql(RERUN).write.mode("overwrite").option("partitionOverwriteMode", mode) \
        .partitionBy("day").parquet(str(out))
    days = sorted(p.name for p in out.iterdir() if p.is_dir())
    print(f"   option {mode:<8} folders after: {days}")

print("\n== 8. during a dynamic overwrite: the new files wait in a staging folder")
import threading  # noqa: E402
import time  # noqa: E402
from pyspark.sql.functions import udf  # noqa: E402

fresh("by_day")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")


@udf("bigint")
def slow(x):
    time.sleep(0.5)                # slow down the tasks, so that the folder can be seen mid-write
    return x


spark.udf.register("slow", slow)
seen, stop = [], threading.Event()


def watch():
    root = WH / "by_day"
    while not stop.is_set():
        for p in root.rglob("*.parquet"):
            rel = str(p.relative_to(root))
            if rel.startswith(".spark-staging") and "_temporary" not in rel:
                seen.append(rel)
        time.sleep(0.05)


t = threading.Thread(target=watch)
t.start()
spark.sql("INSERT OVERWRITE TABLE by_day SELECT borough, slow(trips), day FROM (" + RERUN + ")")
stop.set()
t.join()
import re  # noqa: E402
names = sorted({re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<id>", s) for s in seen})
print("  files seen under the table folder while the job ran (outside _temporary):")
for n in names:
    print("   ", n)
print("  after the commit:", sorted(p.name for p in (WH / "by_day").iterdir()))
