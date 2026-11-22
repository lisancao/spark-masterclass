"""Chapter 33: print the bronze files, the quarantine and the gold tables of the shop pipeline."""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session

work = Path(sys.argv[1])
os.chdir(work)
spark = session("ch33-peek", **{"spark.sql.catalogImplementation": "hive",
                                "spark.sql.warehouse.dir": str(work / "spark-warehouse")})
spark.sql("""SELECT _source_file, count(*) AS rows,
                    date_format(min(_ingested_at), 'HH:mm:ss') AS ingested_at
             FROM bronze_orders GROUP BY 1 ORDER BY 1""").show(truncate=False)
spark.table("quarantine_orders").select("order_id", "customer_id", "quantity", "unit_price", "_reason") \
    .orderBy("order_id").show(truncate=False)
spark.table("gold_daily_revenue").orderBy("order_date").show(truncate=False)
spark.table("gold_customer_value").orderBy("revenue_rank", "customer_id").show(truncate=False)
