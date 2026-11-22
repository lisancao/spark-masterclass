-- The same SCD Type 2 flow in SQL (new in Spark 4.3), tracking history on city only.
CREATE STREAMING TABLE customers_city_history
FLOW AUTO CDC
FROM STREAM customer_changes
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, seq, _source_file)
STORED AS SCD TYPE 2
TRACK HISTORY ON (city);
