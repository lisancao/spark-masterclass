"""Chapter 16, Step 12: what the 4.3.0 types do not do (grouping, ordering, equality, the cache),
and the Arrow cache serializer that accepts them. Pass --arrow-cache to start the session with it.

Run from code/:  uv run python ch16/limits.py; uv run python ch16/limits.py --arrow-cache
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from geo76 import attempt
from common.book import session, work

W = work("ch16", clean=False)
ARROW = "--arrow-cache" in sys.argv
conf = {"spark_sql_cache_serializer": "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer"} if ARROW else {}
spark = session("ch16-limits", **conf)
print("cache serializer:", spark.conf.get("spark.sql.cache.serializer"))
z = spark.sql(f"SELECT location_id, borough, st_geomfromwkb(wkb, 3857) AS g FROM parquet.`{W / 'zones.parquet'}`")
z.createOrReplaceTempView("z")

if not ARROW:
    print("\n== 1. ordering, equality and grouping")
    attempt("ORDER BY g", lambda: spark.sql("SELECT * FROM z ORDER BY g").first(), 30)
    attempt("WHERE g = g", lambda: spark.sql("SELECT count(*) FROM z WHERE g = g").first(), 30)
    attempt("GROUP BY g", lambda: spark.sql("SELECT g, count(*) FROM z GROUP BY g").count(), 30)
    attempt("SELECT DISTINCT g", lambda: spark.sql("SELECT DISTINCT g FROM z").count(), 30)
    attempt("GROUP BY st_asbinary(g)", lambda: spark.sql(
        "SELECT st_asbinary(g), count(*) FROM z GROUP BY 1").count(), 30)
    attempt("max_by(g, location_id) per borough", lambda: spark.sql(
        "SELECT borough, st_srid(max_by(g, location_id)) FROM z GROUP BY borough").count(), 30)
    attempt("collect_list(g) per borough", lambda: spark.sql(
        "SELECT borough, size(collect_list(g)) FROM z GROUP BY borough").count(), 30)
    attempt("JOIN ON g", lambda: spark.sql("SELECT count(*) FROM z a JOIN z b ON a.g = b.g").first(), 30)

print("\n== 2. the in-memory cache")
attempt("cache() then count()", lambda: z.cache().count(), 30)
z.unpersist()
attempt("cache() of st_asbinary(g)", lambda: spark.sql("SELECT location_id, st_asbinary(g) AS wkb FROM z")
        .cache().count(), 30)
if ARROW:
    c = z.cache()
    attempt("cached rows, then st_srid(g)", lambda: (c.count(), c.selectExpr("max(st_srid(g))").first()[0]), 30)
    attempt("cached plan has InMemoryRelation", lambda: "InMemoryRelation" in c._jdf.queryExecution().optimizedPlan().toString(), 30)

if not ARROW:
    print("\n== 3. printing and JSON")
    spark.sql("SELECT location_id, g FROM z WHERE location_id = 161").show(truncate=False)
    attempt("to_json(named_struct('g', g))", lambda: spark.sql(
        "SELECT to_json(named_struct('g', g)) FROM z").first()[0], 30)
    attempt("to_json of st_asbinary(g)", lambda: spark.sql(
        "SELECT to_json(named_struct('g', st_asbinary(g))) FROM z WHERE location_id = 161").first()[0][:60], 30)
