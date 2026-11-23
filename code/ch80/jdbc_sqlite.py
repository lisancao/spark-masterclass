"""Chapter 80: the same SQLite table through Spark's JDBC source and the third-party sqlite-jdbc driver.

Run from code/:  uv run python ch80/jdbc_sqlite.py   (downloads org.xerial:sqlite-jdbc from Maven Central)
"""
import os, re, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.book import session
import shopdb

DRIVER = "org.xerial:sqlite-jdbc:3.50.3.0"
db = os.path.join(tempfile.mkdtemp(prefix="ch80-jdbc-"), "shop.db")
shopdb.build(db)
spark = session("ch80-jdbc", spark_jars_packages=DRIVER, spark_sql_session_timeZone="UTC")
print("driver:", DRIVER)
df = (spark.read.format("jdbc").option("url", f"jdbc:sqlite:{db}").option("dbtable", "orders")
      .option("driver", "org.sqlite.JDBC").load())
types = dict(df.dtypes)
for col in ["qty", "gift", "placed_at", "amount", "note"]:
    try:
        vals = [r[0] for r in df.select(col).orderBy("order_id").limit(400).collect()]
        sample = sorted({str(v) for v in vals})[:5]
        print(f"{col:<10} {types[col]:<14} reads; some values: {sample}")
    except Exception as e:
        m = re.search(r"java\.sql\.SQLException: [^\n]*", str(e))
        print(f"{col:<10} {types[col]:<14} fails: {m.group(0)[:60] if m else str(e).splitlines()[0][:60]}")
import sqlite3
c = sqlite3.connect(db)
print("stored qty values that are not integers:",
      c.execute("SELECT order_id, qty FROM orders WHERE typeof(qty) <> 'integer' ORDER BY order_id LIMIT 4").fetchall())
ids = [r[0] for r in c.execute("SELECT order_id FROM orders WHERE typeof(qty) <> 'integer' ORDER BY order_id LIMIT 4")]
print("JDBC reads them as:", [(r.order_id, r.qty) for r in df.select("order_id", "qty").where(df.order_id.isin(ids)).orderBy("order_id").collect()])
print("gift 'yes' rows, JDBC values:", sorted({str(r.gift) for r in df.select("order_id", "gift").where(df.order_id.isin(
    [r[0] for r in c.execute("SELECT order_id FROM orders WHERE gift = 'yes'")])).select("gift").collect()}))
