"""Load the taxi month and the zones into PostgreSQL with the JDBC writer.

The server logs only DDL during the load (log_statement=ddl), so the log shows the CREATE TABLE
that Spark sends, without 3.8 million INSERT lines. Then psql adds a key and an index.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import log_level, PROPS, URL, Log, pg_session, psql  # noqa: E402
from common.book import TRIPS, ZONES  # noqa: E402

spark = pg_session("ch39-load")
trips = spark.read.parquet(str(TRIPS))
trips = trips.toDF(*[c.lower() for c in trips.columns])          # plain names, no quotes in SQL
zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
zones = zones.toDF(*[c.lower() for c in zones.columns])

log_level("ddl")
with Log() as log:
    t = time.perf_counter()
    trips.repartition(8).write.jdbc(URL, "trips", mode="overwrite", properties=PROPS)
    load_s = time.perf_counter() - t
    zones.write.jdbc(URL, "zones", mode="overwrite", properties=PROPS)
log_level("all")

print("== the DDL that Spark sent (server log)")
for s in log.lines:
    if s.upper().startswith(("CREATE", "DROP")):
        print("  " + s[:400])
print(f"\nloaded {trips.count():,} trips with 8 tasks in {load_s:.1f} s (batchsize 1000, not a benchmark)")

psql("""
ALTER TABLE trips ADD COLUMN trip_id bigint GENERATED ALWAYS AS IDENTITY;
ALTER TABLE trips ADD PRIMARY KEY (trip_id);
CREATE INDEX trips_pickup ON trips (tpep_pickup_datetime);
ALTER TABLE zones ADD PRIMARY KEY (locationid);
ANALYZE trips; ANALYZE zones;
""")
print("\n== the table in PostgreSQL (psql \\d trips, first columns)")
print("\n".join(psql("\\d trips").splitlines()[:12]))
print(psql("SELECT count(*) AS trips, min(trip_id), max(trip_id), "
           "min(tpep_pickup_datetime)::date AS first_day, max(tpep_pickup_datetime)::date AS last_day FROM trips"))
