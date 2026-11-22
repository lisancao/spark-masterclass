-- Chapter 48: run by SparkSqlOperator through the spark-sql CLI. Airflow renders the Jinja first.
SELECT Borough, trips, CAST(revenue AS BIGINT) AS revenue
FROM parquet.`{{ params.out }}`
ORDER BY revenue DESC
LIMIT 3;
