"""The JDBC write path: save modes, truncate, table options, transactions per task, upserts.

Small DataFrames (a few rows), so that the server log shows every statement.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import PROPS, URL, Log, pg_session, psql  # noqa: E402

spark = pg_session("ch88-writes")
from pyspark.sql import functions as F  # noqa: E402

zones = spark.table("pg.public.zones").where("locationid <= 6").select("locationid", "borough", "zone")


def statements(log, skip_insert_after=2):
    """The statements of one write, with repeated INSERT lines folded."""
    shown, inserts = [], 0
    for s in log.lines:
        if "pg_catalog" in s or s.startswith("SET application_name"):
            continue
        if s.upper().startswith("INSERT") and "VALUES" in s:
            inserts += 1
            if inserts > skip_insert_after:
                continue
        shown.append(s)
    for s in shown:
        print("   " + s.strip()[:240])
    if inserts > skip_insert_after:
        print(f"   ... ({inserts} INSERT executions in all)")


def write(title, df, mode, **opts):
    print(f"== {title}")
    w = df.write.format("jdbc").option("url", URL).options(**PROPS).option("dbtable", "zone_copy").mode(mode)
    for k, v in opts.items():
        w = w.option(k, v)
    with Log() as log:
        w.save()
    statements(log)
    print()


write("mode=overwrite (the default way: DROP and CREATE)", zones.coalesce(1), "overwrite")
write("mode=overwrite, truncate=true", zones.coalesce(1), "overwrite", truncate="true")
write("mode=overwrite, truncate=true, cascadeTruncate=true", zones.coalesce(1), "overwrite",
      truncate="true", cascadeTruncate="true")
psql("DROP TABLE IF EXISTS zone_copy")
write("createTableColumnTypes and createTableOptions", zones.coalesce(1), "overwrite",
      createTableColumnTypes="borough VARCHAR(20), zone VARCHAR(60)",
      createTableOptions="WITH (fillfactor = 70)")
write("mode=append, batchsize=4, isolationLevel=READ_COMMITTED", zones.coalesce(1), "append",
      batchsize="4", isolationLevel="READ_COMMITTED")
write("mode=append, isolationLevel=NONE (no transaction)", zones.coalesce(1), "append",
      isolationLevel="NONE")
write("sessionInitStatement on a write (a read option)", zones.coalesce(1), "append",
      sessionInitStatement="SET application_name = 'from_session_init'")
print("== mode=errorifexists on a table that exists")
try:
    zones.write.jdbc(URL, "zone_copy", mode="errorifexists", properties=PROPS)
except Exception as e:
    print("   " + str(e).splitlines()[0][:160])
print()

# Each task writes in its own transaction. One task fails, the others stay committed.
psql("DROP TABLE IF EXISTS keyed; CREATE TABLE keyed (id int PRIMARY KEY, v text); "
     "INSERT INTO keyed VALUES (700, 'already there')")
rows = spark.range(0, 800, 1, 8).select(F.col("id").cast("int"), F.lit("new").alias("v"))
print("== append 800 rows in 8 tasks to a table that has id 700 (in task 7)")
try:
    rows.write.jdbc(URL, "keyed", mode="append", properties={**PROPS, "batchsize": "50"})
except Exception as e:
    import re
    m = re.search(r"ERROR: (duplicate key value violates unique constraint \"\w+\")", str(e))
    t = re.search(r"(Task \d+ in stage [\d.]+ failed \d+ times)", str(e))
    print("   the job failed: " + (t.group(1) if t else "?") + ": " + (m.group(1) if m else "?"))
print("   rows in keyed after the failed job, by task range:")
print("\n".join("   " + ln for ln in psql(
    "SELECT id / 100 AS task, count(*) FROM keyed GROUP BY 1 ORDER BY 1").splitlines()))

# Upsert: Spark has no upsert mode for JDBC. Write a staging table, then one INSERT .. ON CONFLICT.
print("== upsert through a staging table")
psql("DROP TABLE IF EXISTS fares; CREATE TABLE fares (payment_type bigint PRIMARY KEY, avg_fare float8, n bigint);"
     "INSERT INTO fares VALUES (1, 0, 0), (99, -1, -1)")
agg = spark.table("pg.public.trips").groupBy("payment_type").agg(
    F.avg("fare_amount").alias("avg_fare"), F.count("*").alias("n"))
with Log() as log:
    agg.write.jdbc(URL, "fares_stage", mode="overwrite", properties=PROPS)
    # one statement on one connection, from the driver (classic sessions only: it uses the JVM)
    jvm = spark._jvm
    conn = jvm.java.sql.DriverManager.getConnection(URL, PROPS["user"], PROPS["password"])
    try:
        stmt = conn.createStatement()
        n = stmt.executeUpdate(
            "INSERT INTO fares SELECT * FROM fares_stage ON CONFLICT (payment_type) "
            "DO UPDATE SET avg_fare = EXCLUDED.avg_fare, n = EXCLUDED.n")
    finally:
        conn.close()
statements(log)
print(f"   rows inserted or updated: {n}")
print("\n".join("   " + ln for ln in psql("SELECT * FROM fares ORDER BY payment_type").splitlines()))

print("\n== MERGE INTO a table of the JDBC catalog")
try:
    spark.sql("""MERGE INTO pg.public.fares t USING pg.public.fares_stage s
                 ON t.payment_type = s.payment_type
                 WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""").collect()
except Exception as e:
    print("   " + str(e).splitlines()[0][:200])
