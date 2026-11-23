"""Chapter 73, Step 11: ADD FILE, ADD ARCHIVE and ADD JAR in SQL, and LIST, on a cluster with
separate executor JVMs (local-cluster). Builds its own small file, zip and jar first.
"""
import os
import subprocess
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session, work  # noqa: E402

import pyspark  # noqa: E402
from pyspark import SparkFiles  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType  # noqa: E402

W = work("ch73-sqlres")
here = Path(__file__).resolve().parent

# 1. the three resources
(W / "rates.csv").write_text("1,standard\n2,JFK\n3,Newark\n")
with zipfile.ZipFile(W / "lookup.zip", "w") as z:
    z.writestr("zones/boroughs.txt", "Manhattan\nBrooklyn\nQueens\n")
jars = Path(pyspark.__file__).parent / "jars"
cp = ":".join(str(j) for j in jars.glob("spark-sql-api_*.jar"))
subprocess.run(["javac", "-d", str(W / "classes"), "-cp", cp, str(here / "sqlres/book/Shout.java")], check=True)
subprocess.run(["jar", "cf", str(W / "shout.jar"), "-C", str(W / "classes"), "."], check=True)

spark = session("ch73-sqlres", spark_master="local-cluster[2,1,1024]")


def sql(stmt):
    print(f"spark-sql> {stmt.replace(str(W), '$W')}")
    rows = spark.sql(stmt).collect()
    for r in rows:
        print("  " + str(r[0]).replace(str(W), "$W"))


sql(f"ADD FILE {W}/rates.csv")
sql(f"ADD ARCHIVE {W}/lookup.zip")
sql(f"ADD JAR {W}/shout.jar")
sql("LIST FILES")
sql("LIST ARCHIVES")
sql("LIST JARS")


# 2. what each executor sees
@F.udf(StringType())
def on_executor(_):
    import os as o
    from pyspark import SparkFiles as SF
    rates = open(SF.get("rates.csv")).read().split()[1]
    borough = open(o.path.join("lookup.zip", "zones", "boroughs.txt")).read().split()[0]
    return f"pid {o.getpid()}  SparkFiles.get -> {rates}  ./lookup.zip/zones -> {borough}"


print("\non the executors:")
for r in spark.range(0, 4, numPartitions=4).select(on_executor("id").alias("x")).distinct().collect():
    print("  " + r.x)

# 3. a class from the added jar, as a SQL function
spark.udf.registerJavaFunction("shout", "book.Shout", StringType())
sql("SELECT shout(borough) FROM VALUES ('queens') AS t(borough)")

# 4. resources belong to the SparkContext, not to one session
other = spark.newSession()
print(f"\nnew session: LIST JARS -> {len(other.sql('LIST JARS').collect())} jar(s), "
      f"LIST FILES -> {len(other.sql('LIST FILES').collect())} file(s)")
print(f"driver: SparkFiles root {SparkFiles.getRootDirectory().replace(os.sep + 'tmp', '/tmp')[:5]}..., "
      f"rates.csv there: {os.path.exists(SparkFiles.get('rates.csv'))}")
spark.stop()
