"""Chapter 31: count the quarantined rows of the quality pipeline by rule."""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session

work = Path(sys.argv[1])
os.chdir(work)
spark = session("ch31-quarantine", **{"spark.sql.catalogImplementation": "hive",
                                      "spark.sql.warehouse.dir": str(work / "spark-warehouse")})
spark.sql("""SELECT rule, count(*) AS rows FROM quarantine_orders
             LATERAL VIEW explode(failed_rules) t AS rule GROUP BY rule ORDER BY rows DESC, rule""").show(truncate=False)
spark.table("quarantine_orders").drop("_source_file").orderBy("order_id").show(5, truncate=False)
