-- The same kind of dataset in SQL: a materialized view over the temporary view from orders.py.
CREATE MATERIALIZED VIEW placed_by_customer
COMMENT 'Placed orders per customer'
AS SELECT customer_id, count(*) AS placed
FROM placed_orders
GROUP BY customer_id;
