"""What Spark pushes into PostgreSQL: the jdbc format (DSv1) against the JDBC catalog (DSv2).

For each query: the Pushed* lines of the plan and the SELECT that PostgreSQL received.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import PROPS, URL, Log, pg_session  # noqa: E402
from common.book import plan  # noqa: E402

spark = pg_session("ch88-pushdown")
from pyspark.sql import functions as F  # noqa: E402

v1 = spark.read.jdbc(URL, "trips", properties=PROPS)       # the jdbc format: a DSv1 relation
v2 = spark.table("pg.public.trips")                         # the same table through the JDBC catalog


def show(title, df, table="trips"):
    print(f"-- {title}")
    lines = plan(df, "formatted").splitlines()
    scan = [ln.strip() for ln in lines if ln.strip().startswith(("Scan JDBCRelation", "BatchScan", "Scan "))
            and "(" in ln][:1]
    pushed = [ln.strip() for ln in lines if ln.strip().startswith("Pushed")]
    for s in scan:
        print("   scan:  " + s[:110])
    for p in pushed:
        if not p.endswith(": []"):
            print("   " + p[:150])
    with Log() as log:
        rows = df.collect()
    for s in log.selects(table):
        print("   SQL:   " + " ".join(s.split())[:900])
    print(f"   cursor fetches of 1000 rows: {log.fetches}, rows in the result: {len(rows)}")
    print()


def both(title, f, table="trips"):
    print(f"== {title}")
    show("jdbc format (DSv1)", f(v1), table)
    show("JDBC catalog (DSv2)", f(v2), table)


both("filter and column pruning",
     lambda d: d.where((F.col("fare_amount") > 300) & (F.col("payment_type") == 1)).select("trip_id", "fare_amount"))
both("a filter with a function (upper)",
     lambda d: d.where(F.upper("store_and_fwd_flag") == "Y").select("trip_id"))
both("aggregate: average fare by payment type",
     lambda d: d.groupBy("payment_type").agg(F.count("*").alias("n"), F.avg("fare_amount").alias("avg_fare")))
both("limit", lambda d: d.select("trip_id", "fare_amount").limit(5))
both("offset and limit", lambda d: d.select("trip_id").orderBy("trip_id").offset(10).limit(3))
both("top-N: the five highest fares", lambda d: d.select("trip_id", "fare_amount").orderBy(F.desc("fare_amount")).limit(5))

print("== TABLESAMPLE (SQL)")
show("JDBC catalog (DSv2)", spark.sql("SELECT trip_id FROM pg.public.trips TABLESAMPLE (0.01 PERCENT)"))

print("== join of trips and zones")
q = """SELECT z.borough, count(*) AS n FROM pg.public.trips t JOIN pg.public.zones z
       ON t.pulocationid = z.locationid WHERE t.fare_amount > 200 GROUP BY z.borough"""
show("spark.sql.optimizer.datasourceV2JoinPushdown=false (the default)", spark.sql(q), None)
spark.conf.set("spark.sql.optimizer.datasourceV2JoinPushdown", "true")
show("spark.sql.optimizer.datasourceV2JoinPushdown=true", spark.sql(q), None)
spark.conf.set("spark.sql.optimizer.datasourceV2JoinPushdown", "false")

print("== turn a pushdown off")
for k, v in {"": "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog", ".url": URL,
             ".user": PROPS["user"], ".password": PROPS["password"], ".driver": PROPS["driver"],
             ".pushDownAggregate": "false"}.items():
    spark.conf.set("spark.sql.catalog.pg_noagg" + k, v)              # a second catalog, read lazily
show("catalog pg_noagg with pushDownAggregate=false",
     spark.table("pg_noagg.public.trips").groupBy("payment_type").count())
off = (spark.read.format("jdbc").option("url", URL).options(**PROPS).option("dbtable", "trips")
       .option("pushDownPredicate", "false").load())
show("jdbc format with pushDownPredicate=false", off.where("fare_amount > 300").select("trip_id"))
