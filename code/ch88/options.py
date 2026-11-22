"""Read options around the query: sessionInitStatement, query and prepareQuery, queryTimeout,
customSchema, hint, and the dialect that Spark picks for a URL.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import PROPS, URL, Log, pg_session  # noqa: E402

spark = pg_session("ch88-options")


def reader(**opts):
    r = spark.read.format("jdbc").option("url", URL).options(**PROPS)
    for k, v in opts.items():
        r = r.option(k, v)
    return r


def logged(title, df, keep=("SET", "SELECT", "WITH", "BEGIN", "COMMIT")):
    print(f"== {title}")
    with Log() as log:
        rows = df.collect()
    for s in log.lines:
        if s.startswith("SELECT 1 AS mark") or "pg_catalog" in s:
            continue
        if s.upper().startswith(keep):
            print("   postgres> " + s[:220])
    return rows


rows = logged("sessionInitStatement runs before the read, on each connection",
              reader(dbtable="trips", sessionInitStatement="SET work_mem = '64MB'",
                     partitionColumn="trip_id", lowerBound=1, upperBound=3837248, numPartitions=2)
              .where("fare_amount > 400").select("trip_id"))
print(f"   rows: {len(rows)}\n")

rows = logged("query: Spark wraps your SELECT as a subquery",
              reader(query="SELECT payment_type, count(*) AS n FROM trips GROUP BY payment_type")
              .where("n > 1000"))
print(f"   rows: {sorted((r[0], r[1]) for r in rows)}\n")

rows = logged("prepareQuery and query: a WITH clause in front of the subquery",
              reader(prepareQuery="WITH big AS (SELECT * FROM trips WHERE total_amount > 500)",
                     query="SELECT count(*) AS n FROM big"))
print(f"   rows: {[r[0] for r in rows]}\n")

print("== queryTimeout=1 on a query that takes 3 seconds")
try:
    reader(query="SELECT pg_sleep(3) AS slept", queryTimeout=1).collect()
except Exception as e:
    msg = [ln for ln in str(e).splitlines() if "cancel" in ln.lower()]
    print("   " + (msg[0].strip() if msg else str(e).splitlines()[0])[:160])
print()

print("== customSchema: read the fare as DECIMAL(10,2) and the id as STRING")
df = reader(dbtable="zones", customSchema="locationid STRING").limit(1)
print("   " + ", ".join(f"{f.name}: {f.dataType.simpleString()}" for f in df.schema.fields))
df = reader(dbtable="trips", customSchema="fare_amount DECIMAL(10,2)").select("fare_amount").limit(1)
print("   " + ", ".join(f"{f.name}: {f.dataType.simpleString()}" for f in df.schema.fields))
print()

print("== hint on PostgreSQL")
try:
    reader(dbtable="trips", hint="/*+ INDEX(trips trips_pickup) */").limit(1).collect()
except Exception as e:
    print("   " + str(e).splitlines()[0][:180])
print()

print("== the dialect for each URL (JdbcDialects.get)")
D = spark._jvm.org.apache.spark.sql.jdbc.JdbcDialects
for url in ["jdbc:postgresql://db/x", "jdbc:mysql://db/x", "jdbc:sqlserver://db", "jdbc:oracle:thin:@db",
            "jdbc:derby:memory:x", "jdbc:h2:mem:x", "jdbc:databricks://host", "jdbc:snowflake://acct",
            "jdbc:sqlite:x.db", "jdbc:duckdb:"]:
    print(f"   {url:28} {D.get(url).getClass().getName().split('.')[-1]}")
print()
print("== JdbcDialects.getBuiltInDialect with a name that does not exist (new in 4.3.0)")
try:
    D.getBuiltInDialect("sqlite")
except Exception as e:
    print("   " + str(e).splitlines()[0][:240].split(": ", 1)[-1])
