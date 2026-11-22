CREATE STREAMING TABLE clean_trips
COMMENT 'Trips with a possible fare, distance and duration'
AS SELECT
  tpep_pickup_datetime                 AS pickup_ts,
  CAST(tpep_pickup_datetime AS DATE)   AS pickup_date,
  PULocationID                         AS pickup_zone,
  trip_distance,
  fare_amount,
  tip_amount,
  payment_type
FROM STREAM raw_trips
WHERE fare_amount > 0
  AND trip_distance BETWEEN 0.1 AND 100
  AND tpep_dropoff_datetime > tpep_pickup_datetime;
