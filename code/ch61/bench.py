"""Chapter 61, Step 8: what the binary format buys. One field from one million taxi trips stored three ways:
JSON text, VARIANT without shredding, and VARIANT with shredding (the 4.3 default). One warm-up run, then the
median of five runs for each query. Run from code/, on a quiet machine:
  flock $BOOK_DATA/.bench.lock uv run python ch61/bench.py
"""
import glob, os, statistics, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, TRIPS

spark = session("ch61-bench")
tmp = tempfile.mkdtemp(prefix="ch61-bench-")
N = 1_000_000
trips = spark.read.parquet(str(TRIPS)).limit(N)
src = trips.selectExpr("to_json(struct(*)) AS j").cache()
src.count()

src.write.parquet(f"{tmp}/json")
src.selectExpr("parse_json(j) AS v").write.parquet(f"{tmp}/shredded")
spark.conf.set("spark.sql.variant.writeShredding.enabled", "false")
src.selectExpr("parse_json(j) AS v").write.parquet(f"{tmp}/plain")
spark.conf.unset("spark.sql.variant.writeShredding.enabled")
src.unpersist()

def size(d):
    return sum(os.path.getsize(f) for f in glob.glob(f"{d}/*.parquet")) / 2**20

def timed(sql):
    spark.sql(sql).collect()                       # warm-up
    ts = []
    for _ in range(5):
        t = time.perf_counter(); r = spark.sql(sql).collect(); ts.append(time.perf_counter() - t)
    return statistics.median(ts), r[0][0]

print(f"{N:,} trips as JSON text (one object of up to {len(trips.columns)} fields each; to_json drops NULL fields); "
      f"local[*] on {os.cpu_count()} cores\n")
print(f"files: JSON text {size(tmp + '/json'):.1f} MiB, VARIANT {size(tmp + '/plain'):.1f} MiB, "
      f"VARIANT shredded {size(tmp + '/shredded'):.1f} MiB\n")
queries = [
    ("JSON text, get_json_object", f"SELECT round(sum(CAST(get_json_object(j, '$.total_amount') AS DOUBLE)), 2) FROM parquet.`{tmp}/json`"),
    ("JSON text, parse_json + variant_get", f"SELECT round(sum(variant_get(parse_json(j), '$.total_amount', 'double')), 2) FROM parquet.`{tmp}/json`"),
    ("VARIANT, variant_get", f"SELECT round(sum(variant_get(v, '$.total_amount', 'double')), 2) FROM parquet.`{tmp}/plain`"),
    ("VARIANT shredded, variant_get", f"SELECT round(sum(variant_get(v, '$.total_amount', 'double')), 2) FROM parquet.`{tmp}/shredded`"),
    ("VARIANT, to_json of the whole value", f"SELECT sum(length(to_json(v))) FROM parquet.`{tmp}/plain`"),
]
for label, sql in queries:
    t, r = timed(sql)
    print(f"  {label:<38} median {t:6.2f} s   result {r}")
