CREATE MATERIALIZED VIEW borough_daily
COMMENT 'Trips and average fare for each pickup borough and day'
AS SELECT
  c.pickup_date,
  z.Borough                     AS borough,
  count(*)                      AS trips,
  round(avg(c.fare_amount), 2)  AS avg_fare
FROM clean_trips c
JOIN zones z ON c.pickup_zone = z.LocationID
GROUP BY c.pickup_date, z.Borough;
