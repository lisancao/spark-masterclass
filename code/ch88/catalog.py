"""The JDBC catalog (JDBCTableCatalog): PostgreSQL schemas and tables as Spark namespaces and tables.

Each SQL statement in Spark, and the statements that PostgreSQL received for it.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import Log, pg_session, psql  # noqa: E402

spark = pg_session("ch88-catalog")
psql("DROP SCHEMA IF EXISTS reports CASCADE")


def run(sql, show_rows=True, n=8):
    print(f"spark-sql> {' '.join(sql.split())}")
    with Log() as log:
        rows = spark.sql(sql).collect()
    if show_rows:
        for r in rows[:n]:
            print("  " + " | ".join(str(x) for x in r))
        if len(rows) > n:
            print(f"  ... ({len(rows)} rows)")
    for s in log.lines:
        if "pg_catalog" in s or "information_schema" in s.lower() or s.startswith("SELECT 1 AS mark"):
            continue
        if s.upper().startswith(("SELECT", "CREATE", "ALTER", "DROP", "INSERT", "COMMENT", "TRUNCATE")):
            print("  postgres> " + s[:200])
    print()


run("SHOW NAMESPACES IN pg")
run("SHOW TABLES IN pg.public")
run("DESCRIBE TABLE pg.public.trips", n=6)
run("CREATE NAMESPACE pg.reports")
run("""CREATE TABLE pg.reports.daily (day DATE, trips BIGINT, revenue DOUBLE)""", show_rows=False)
run("""INSERT INTO pg.reports.daily
       SELECT to_date(tpep_pickup_datetime) AS day, count(*), round(sum(total_amount), 2)
       FROM pg.public.trips
       WHERE tpep_pickup_datetime >= '2026-06-01' AND tpep_pickup_datetime < '2026-06-04'
       GROUP BY 1""", show_rows=False)
run("SELECT * FROM pg.reports.daily ORDER BY day")
run("ALTER TABLE pg.reports.daily ADD COLUMN note STRING", show_rows=False)
run("SHOW CREATE TABLE pg.reports.daily", n=1)
run("DROP TABLE pg.reports.daily", show_rows=False)
run("DROP NAMESPACE pg.reports", show_rows=False)
