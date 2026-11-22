"""Chapter 15, part 4: what an extraction costs. One million taxi trips stored five ways (typed
Parquet, JSON text, VARIANT unshredded, VARIANT shredded), and one query on each.
Medians of 5 runs after one warm-up. Run under the shared benchmark lock:

    flock $HOME/spark-masterclass/data/.bench.lock uv run python ch15/speed.py
"""
import os
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, TRIPS, WORK

spark = session("ch15-speed", spark_sql_session_timeZone="UTC")
OUT = WORK / "ch15" / "speed"
trips = (spark.read.parquet(str(TRIPS)).where(F.col("_metadata.row_index") < 1_000_000)
         .select("VendorID", "passenger_count", "trip_distance", "payment_type", "fare_amount",
                 "tip_amount", "total_amount", "PULocationID", "DOLocationID"))


def build(name, df, **conf):
    path = OUT / name
    if not path.exists():
        for k, v in conf.items():
            spark.conf.set(k, v)
        df.write.parquet(str(path))
        for k in conf:
            spark.conf.unset(k)
    return str(path), sum(f.stat().st_size for f in path.glob("*.parquet")) / 1e6


as_json = trips.select(F.to_json(F.struct(*trips.columns)).alias("j"))
paths = {
    "typed": build("typed", trips),
    "json": build("json", as_json),
    "variant_plain": build("variant_plain", as_json.select(F.parse_json("j").alias("v")),
                           **{"spark.sql.variant.writeShredding.enabled": "false"}),
    "variant_shred": build("variant_shred", as_json.select(F.parse_json("j").alias("v"))),
}


def median_s(fn, runs=5):
    fn()
    ts = []
    for _ in range(runs):
        t = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t)
    return statistics.median(ts)


def q(path, fare, pay):
    """Average fare of card trips (payment_type = 1)."""
    df = spark.read.parquet(path)
    return lambda: df.where(pay(df) == 1).agg(F.avg(fare(df))).collect()


cases = [
    ("typed columns", "typed", lambda d: d.fare_amount, lambda d: d.payment_type),
    ("JSON text, get_json_object", "json",
     lambda d: F.get_json_object("j", "$.fare_amount").cast("double"),
     lambda d: F.get_json_object("j", "$.payment_type").cast("int")),
    ("JSON text, from_json", "json",
     lambda d: F.from_json("j", "fare_amount DOUBLE, payment_type INT").fare_amount,
     lambda d: F.from_json("j", "fare_amount DOUBLE, payment_type INT").payment_type),
    ("VARIANT unshredded, v:path::type", "variant_plain",
     lambda d: F.expr("v:fare_amount::double"), lambda d: F.expr("v:payment_type::int")),
    ("VARIANT unshredded, try_variant_get", "variant_plain",
     lambda d: F.try_variant_get("v", "$.fare_amount", "double"),
     lambda d: F.try_variant_get("v", "$.payment_type", "int")),
    ("VARIANT shredded, v:path::type", "variant_shred",
     lambda d: F.expr("v:fare_amount::double"), lambda d: F.expr("v:payment_type::int")),
    ("VARIANT shredded, try_variant_get", "variant_shred",
     lambda d: F.try_variant_get("v", "$.fare_amount", "double"),
     lambda d: F.try_variant_get("v", "$.payment_type", "int")),
]
print(f"input: 1,000,000 taxi trips, 9 columns, {os.cpu_count()} cores, local[*]")
for name, (_, mb) in paths.items():
    print(f"  {name:14} {mb:6.1f} MB on disk")
print("query: avg(fare_amount) WHERE payment_type = 1")
answers = set()
for label, key, fare, pay in cases:
    fn = q(paths[key][0], fare, pay)
    answers.add(round(fn()[0][0], 6))
    print(f"  {label:40} {median_s(fn):6.2f} s")
print("all answers equal:", len(answers) == 1, sorted(answers))
