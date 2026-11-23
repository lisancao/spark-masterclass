"""Chapter 66: a database as a source: SQLite affinity, JDBC key ranges, and a Python source with quantiles.

Run from code/:  uv run python ch66/database_source.py
"""
import os, pickle, sqlite3, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark import cloudpickle
from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSourceReader
from common.book import session
import shopdb, sqlite_source
from sqlite_source import SqliteDataSource, SqliteReader, connect_ro, range_partitions

cloudpickle.register_pickle_by_value(sqlite_source)   # ship the module's code with the classes

# 1. SQLite's types are suggestions: each value keeps its own storage class.
conn = sqlite3.connect(":memory:")
conn.execute('CREATE TABLE t (a DECIMAL(10,2), q INT, s VARCHAR(10), fp "FLOATING POINT", n)')
conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?)", [
    ("12.50", "7", 42, "2.0", "five"),
    ("N/A", "seven", "x", 2.7, 5),
    ("12,50", " 8", None, "1.5", b"\x00")])
for row in conn.execute("SELECT a, typeof(a), q, typeof(q), s, typeof(s), fp, typeof(fp), typeof(n) FROM t"):
    print(row)
print("affinity:", {d: sqlite_source.affinity(d) for d in ["DECIMAL(10,2)", "INT", "VARCHAR(10)",
                                                         "FLOATING POINT", ""]})
for bad in (lambda: conn.execute("SELECT ?", (__import__("decimal").Decimal("1.5"),)),
            lambda: pickle.dumps(conn)):
    try:
        bad()
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
print("SQLite", sqlite3.sqlite_version)

# 2. The shop database, and the schema the source derives from it.
db = os.path.join(tempfile.mkdtemp(prefix="ch66-shop-"), "shop.db")
print(f"\nshop.db: {shopdb.build(db)} orders")
spark = session("ch66-database", spark_sql_session_timeZone="UTC")
spark.dataSource.register(SqliteDataSource)


def orders(**opts):
    r = spark.read.format("sqlite").option("path", db).option("table", "orders")
    for k, v in opts.items():
        r = r.option(k, v)
    return r.load()


orders().printSchema()
conn = connect_ro(db)
print("Stored values and what the source made of them:")
raw = {r[0]: r for r in conn.execute(
    "SELECT order_id, amount, qty, placed_at, gift, note FROM orders WHERE order_id IN (1, 2, 3, 13, 30, 47)")}
shown = orders().where("order_id IN (1, 2, 3, 13, 30, 47)").orderBy("order_id")
for r in shown.select("order_id", "amount", "qty", F.date_format("placed_at", "MM-dd HH:mm").alias("t"),
                      "gift", "note").collect():
    print(f"  {raw[r.order_id][1:]!s:<50} -> {r.amount}, {r.qty}, {r.t}, {r.gift}, {r.note}")

# 3. A connection on the reader does not pickle.
class BadReader(SqliteReader):
    def __init__(self, options, schema):
        super().__init__(options, schema)
        self.conn = sqlite3.connect(self.path)          # do not do this


class BadSource(SqliteDataSource):
    @classmethod
    def name(cls):
        return "badsqlite"

    def reader(self, schema):
        return BadReader(self.options, schema)


spark.dataSource.register(BadSource)
try:
    spark.read.format("badsqlite").option("path", db).option("table", "orders").load().count()
except Exception as e:
    import re
    print("\nconnection on self:", re.search(r"TypeError: cannot pickle '[^']+' object", str(e)).group(0))

# 4. Spark's JDBC source with Derby (its jar ships in PySpark): ranges are even in key space.
url = "jdbc:derby:memory:shop;create=true"
props = {"driver": "org.apache.derby.jdbc.EmbeddedDriver"}
ids = list(range(1, 1001)) + [1_000_000, 1_000_001]
(spark.createDataFrame([(i, f"c{i % 7}") for i in ids] + [(None, "nokey")], "ID int, CUSTOMER string")
    .write.jdbc(url, "orders", mode="overwrite", properties=props))


def per_partition(df):
    counts = dict(df.groupBy(F.spark_partition_id()).count().collect())
    return [counts.get(i, 0) for i in range(df.rdd.getNumPartitions())]


jdf = spark.read.jdbc(url, "orders", column="ID", lowerBound=1, upperBound=1000001,
                      numPartitions=4, properties=props)
print("\nJDBC, lowerBound=1 upperBound=1000001 numPartitions=4:")
for p in jdf._jdf.queryExecution().toRdd().partitions():    # classic sessions only
    print("  ", p.whereClause())
print("rows per partition:", per_partition(jdf), "of", jdf.count())
narrow = spark.read.jdbc(url, "orders", column="ID", lowerBound=100, upperBound=200,
                         numPartitions=4, properties=props)
print("bounds 100..200 (not a filter):", narrow.count(), "rows,", per_partition(narrow))
tiny = spark.read.jdbc(url, "orders", column="ID", lowerBound=1, upperBound=3,
                       numPartitions=8, properties=props)
print("bounds 1..3 with numPartitions=8:", tiny.rdd.getNumPartitions(), "partitions")
by_hand = spark.read.jdbc(url, "orders", properties=props, predicates=[
    "ID < 500 OR ID IS NULL", "ID >= 500 AND ID < 1000", "ID >= 1000"])
print("predicates:", per_partition(by_hand))

# 5. The same ranges in the Python source, then quantile ranges.
print("\nrange_partitions('order_id', [100, 200]):")
for w, p in range_partitions("order_id", [100, 200]):
    print("  ", w, p)
stride = orders(partitionColumn="order_id", numPartitions=4)
print("Python source, min..max stride ranges:", per_partition(stride))
quant = orders(partitionColumn="order_id", numPartitions=4, bounds="quantile")
print("Python source, quantile ranges:      ", per_partition(quant))
print("quantile split points:", sqlite_source.quantile_bounds(conn, "orders", "order_id", 4))
print(conn.execute("EXPLAIN QUERY PLAN SELECT order_id FROM orders WHERE order_id IS NOT NULL "
                   "ORDER BY order_id LIMIT 1 OFFSET 300").fetchall())
conn.close()

# 6. One read, many moments: change the table between two ranges (plain Python, no Spark).
from pyspark.sql.datasource import CaseInsensitiveDict
from pyspark.sql.types import StructType, StructField, LongType, StringType


def read_two_ranges(use_snapshot):
    shopdb.build(db)
    schema = StructType([StructField("order_id", LongType()), StructField("customer", StringType())])
    reader = SqliteReader(CaseInsensitiveDict({"path": db, "table": "orders",
                                               "partitionColumn": "order_id", "numPartitions": "2",
                                               "bounds": "quantile"}), schema)
    parts = reader.partitions()
    if not use_snapshot:
        for p in parts:
            p.snapshot = 10 ** 9
    rows = list(reader.read(parts[0]))
    w = sqlite3.connect(db)                       # another program writes between the two tasks
    w.execute("UPDATE orders SET order_id = 1300 WHERE order_id = 1")
    w.execute("INSERT INTO orders (customer) VALUES ('new')")       # gets the next rowid
    w.commit()
    w.close()
    rows += list(reader.read(parts[1]))
    return len(rows), sum(1 for r in rows if r[1] == "ANA" and r[0] in (1, 1300))


print("\n1207 rows when planned. Rows read, and copies of order 1:")
print("  no snapshot mark:  ", read_two_ranges(False))
print("  rowid <= max(rowid):", read_two_ranges(True))
