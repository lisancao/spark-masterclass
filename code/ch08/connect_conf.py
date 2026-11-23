"""Chapter 8: configs over Spark Connect. What a client may set, and what it can see.

Start the server first:  ch08/connect_server.sh start
Run from code/:  uv run python ch08/connect_conf.py > ch08/output/connect_conf.txt
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from common.book import error_text

URL = f"sc://localhost:{os.environ.get('CONNECT_PORT', '15102')}"
K = "spark.sql.shuffle.partitions"

print("== 1. the builder sends its configs after the session starts")
a = (SparkSession.builder.remote(URL)
     .config(K, "22")                                       # runtime SQL: applied
     .config("spark.sql.warehouse.dir", "/tmp/client-wants")  # static SQL: refused, no error
     .config("spark.executor.memory", "8g")                 # core: refused, no error
     .create())
for k in [K, "spark.sql.warehouse.dir", "spark.executor.memory"]:
    print(f"  {k:<30} {a.conf.get(k, '(not set)')}")

print("\n== 2. each client session has its own runtime configs")
b = SparkSession.builder.remote(URL).create()
print(f"  session a: {K} = {a.conf.get(K)}")
print(f"  session b: {K} = {b.conf.get(K)}   (the server's --conf value)")
b.conf.set(K, "23")
b.sql("SET spark.sql.ansi.enabled=false")
print(f"  after b sets 23 and ANSI off: a = {a.conf.get(K)}, {a.conf.get('spark.sql.ansi.enabled')}"
      f" | b = {b.conf.get(K)}, {b.conf.get('spark.sql.ansi.enabled')}")
b.sql("RESET")
print(f"  after RESET in b: b = {b.conf.get(K)}")

print("\n== 3. spark.conf.set of a static or core config fails, as in Classic")
for k, v in [("spark.sql.warehouse.dir", "/tmp/x"), ("spark.executor.memory", "8g")]:
    try:
        a.conf.set(k, v)
    except Exception as e:
        print(f"  {k}:")
        print(error_text(e, width=86, lines=2))

print("\n== 4. a client cannot read a redacted config (new in 4.3)")
try:
    print("  value:", a.conf.get("spark.myteam.db.password"))
except Exception as e:
    print(error_text(e, width=86, lines=2))
print("  with a default:", a.conf.get("spark.myteam.db.password", "(hidden)"))
print("  in SET:", a.sql("SET spark.myteam.db.password").first()["value"])

print("\n== 5. what the client can find out about the server")
for k in ["spark.master", "spark.app.name", "spark.connect.grpc.binding.port"]:
    print(f"  {k:<32} {a.conf.get(k, '(not set)')}")
print("  SET -v rows:", a.sql("SET -v").count())
print("  isModifiable(spark.sql.shuffle.partitions):", a.conf.isModifiable(K))
print("  isModifiable(spark.sql.warehouse.dir):     ", a.conf.isModifiable("spark.sql.warehouse.dir"))
a.stop()
b.stop()
