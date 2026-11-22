-- A streaming table with no query of its own, and two SQL flows into it.
CREATE STREAMING TABLE order_events
COMMENT 'One row for each placed or shipped order';

CREATE FLOW placed_events AS INSERT INTO order_events BY NAME
SELECT order_id, 'placed' AS event, order_ts FROM STREAM orders_raw WHERE status = 'placed';

CREATE FLOW shipped_events AS INSERT INTO order_events BY NAME
SELECT order_id, 'shipped' AS event, order_ts FROM STREAM orders_raw WHERE status = 'shipped';
