"""Chapter 78, Step 7: a metric view gives an agent the definition of a metric. The by-hand SQL that an
agent writes from the raw table exits 0 with a different number.

Data and YAML: the metric views demo in Lisa Cao's spark-demos (copied to code/ch78/data/metrics/).
Run from code/:  uv run python ch78/metric_view.py
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session  # noqa: E402

DATA = Path(__file__).resolve().parent / "data" / "metrics"
WH = tempfile.mkdtemp(prefix="ch78-mv-")                  # a new warehouse for each run
spark = session("metric-view", spark_master="local[2]", spark_sql_warehouse_dir=WH)
spark.sql(f"CREATE TABLE sessions USING parquet LOCATION '{DATA}/sessions.parquet'")
yaml = (DATA / "delivery_metrics.yaml").read_text()
spark.sql(f"CREATE VIEW delivery_metrics WITH METRICS LANGUAGE YAML AS $${yaml}$$")
print("DESCRIBE delivery_metrics:",
      [(r.col_name, r.data_type) for r in spark.sql("DESCRIBE delivery_metrics").collect()])

by_hand = spark.sql("""
    SELECT ROUND(AVG(rate), 4) FROM (
      SELECT region, AVG(CAST(converted AS DOUBLE)) AS rate FROM sessions GROUP BY region)""").first()[0]
measure = spark.sql("SELECT ROUND(MEASURE(conversion_rate), 4) FROM delivery_metrics").first()[0]
print(f"conversion rate  by hand (average of regional rates) {by_hand}   MEASURE() {measure}")

by_hand = spark.sql("""
    SELECT SUM(n) FROM (
      SELECT session_date, COUNT(DISTINCT user_id) AS n FROM sessions GROUP BY session_date)""").first()[0]
measure = spark.sql("SELECT MEASURE(active_users) FROM delivery_metrics").first()[0]
print(f"active users     by hand (sum of daily distinct users) {by_hand:,}   MEASURE() {measure:,}")

print("MEASURE() at the region grain:")
for r in spark.sql("""SELECT region, ROUND(MEASURE(conversion_rate), 4) AS conversion_rate,
                             MEASURE(active_users) AS active_users
                      FROM delivery_metrics GROUP BY region ORDER BY region""").collect():
    print(f"  {r.region:10} {r.conversion_rate:8} {r.active_users:7,}")
spark.stop()
