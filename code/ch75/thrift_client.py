"""Chapter 75: a Python program that queries the Thrift server, with PyHive (no JVM, no PySpark).

Start the server first:  ch75/thrift.sh start
Run from code/:  uv run --with 'pyhive[hive_pure_sasl]' python ch75/thrift_client.py
"""
import getpass
import os

from pyhive import hive

conn = hive.connect(host="127.0.0.1", port=int(os.environ.get("THRIFT_PORT", "15102")),
                    username=getpass.getuser())
cur = conn.cursor()
cur.execute("SELECT current_catalog(), current_database(), version()")
print("session:", cur.fetchone())
cur.execute("SELECT Zone FROM taxi.zones WHERE Borough = %(b)s ORDER BY Zone LIMIT 3", {"b": "EWR"})
print("columns:", [d[0] for d in cur.description])
print("rows:   ", cur.fetchall())
cur.execute("SET spark.sql.shuffle.partitions=4")
cur.execute("SET spark.sql.shuffle.partitions")
print("a SET in this connection:", cur.fetchall())
conn.close()
