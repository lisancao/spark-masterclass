"""Chapter 49: print what a pipeline run left in the catalog, read with a Classic session.

  python ch49/show_tables.py <work folder> [table ...]
The work folder is the copy that common/pipeline.sh ran in. With a Hive metastore there
(metastore_db/), the script reads the tables through it. With LAKE=1 it reads the Iceberg
catalog `lake` at <work>/lake-warehouse, database `retail`.
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

work = Path(sys.argv[1])
tables = sys.argv[2:]
os.chdir(work)   # the Derby metastore and spark-warehouse/ are relative to the work folder
if os.environ.get("LAKE"):
    from common.lake import lake_session
    spark = lake_session("ch49-show", work / "lake-warehouse")
    prefix = "lake.retail."
else:
    from common.book import session
    spark = session("ch49-show", **{"spark.sql.catalogImplementation": "hive",
                                    "spark.sql.warehouse.dir": str(work / "spark-warehouse")})
    prefix = ""
    print("tables:", sorted(r.tableName for r in spark.sql("SHOW TABLES").collect()))
for t in tables:
    df = spark.table(prefix + t)
    print(f"{t}: {df.count()} rows")
    if os.environ.get("SHOW"):
        df.show(truncate=False)
