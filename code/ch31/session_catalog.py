"""Chapter 31: SparkSessionCatalog puts Iceberg tables and plain Spark tables in one catalog.

Needs the Hive Metastore container:  code/ch31/catalogs.sh up
Run from code/:  uv run python ch31/session_catalog.py
"""
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ZONES
from ice import WORK, err, ice_session, tidy

spark = ice_session("ch31-session-catalog", {"hive": "hive"}, **{
    "spark.sql.catalogImplementation": "hive",                     # Spark's own catalog: the HMS
    "spark.hadoop.hive.metastore.uris": "thrift://localhost:19083",
    "spark.sql.warehouse.dir": f"file://{WORK}/hive",
    # replace spark_catalog with Iceberg's wrapper around it, on the same metastore
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type": "hive",
    "spark.sql.catalog.spark_catalog.uri": "thrift://localhost:19083",
})
spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES)) \
     .createOrReplaceTempView("zones_csv")

spark.sql("CREATE NAMESPACE IF NOT EXISTS shared")
spark.sql("USE spark_catalog.shared")
for t in ("zones_parquet", "zones_iceberg", "zones_default"):
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    shutil.rmtree(WORK / "hive" / "shared.db" / t, ignore_errors=True)   # left by an earlier metastore
spark.sql("CREATE TABLE zones_parquet USING parquet AS SELECT * FROM zones_csv")
spark.sql("CREATE TABLE zones_iceberg USING iceberg AS SELECT * FROM zones_csv")
spark.sql("CREATE TABLE zones_default AS SELECT * FROM zones_csv")      # no USING clause

spark.catalog.dropTempView("zones_csv")
print("SHOW TABLES IN spark_catalog.shared:")
for r in [r for r in spark.sql("SHOW TABLES").collect() if not r.isTemporary]:
    d = dict((x[0], x[1]) for x in spark.sql(f"DESCRIBE TABLE EXTENDED {r.tableName}").collect())
    n = spark.sql(f"SELECT count(*) FROM {r.tableName}").first()[0]
    print(f"  {r.tableName:<14} Provider={d.get('Provider'):<8} {n} rows  "
          f"Location={tidy(d.get('Location'))}")

print("\nThe same Iceberg table through the Iceberg catalog `hive`, on the same metastore:")
print("  SELECT count(*) FROM hive.shared.zones_iceberg ->",
      spark.sql("SELECT count(*) FROM hive.shared.zones_iceberg").first()[0])
print("  hive.shared.zones_parquet ->", end=" ")
try:
    spark.sql("SELECT count(*) FROM hive.shared.zones_parquet").collect()
    print("read worked")
except Exception as e:
    print(err(e, 70))

print("\nsystem procedures through spark_catalog:")
spark.sql("INSERT INTO zones_iceberg SELECT * FROM zones_parquet WHERE Borough = 'EWR'")
spark.sql("CALL spark_catalog.system.rewrite_data_files(table => 'shared.zones_iceberg', "
          "options => map('min-input-files', '2'))") \
     .select("rewritten_data_files_count", "added_data_files_count").show()
