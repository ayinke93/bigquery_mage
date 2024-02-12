CREATE OR REPLACE EXTERNAL TABLE `neon-concord-412616.taxi.external_green_taxi` 
OPTIONS(
  format = 'parquet',
  uris = ['gs://yemisi-mage/green_taxi_data.parquet']
);

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `neon-concord-412616.taxi.big_green_taxi`;


SELECT COUNT(*) AS zero_fare_records
FROM `neon-concord-412616.taxi.big_green_taxi`
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `neon-concord-412616.taxi.intermediate_green_taxi` AS
SELECT
  *,
  TIMESTAMP_SECONDS(DIV(lpep_pickup_datetime, 1000000000)) AS pickup_timestamp
FROM
  `neon-concord-412616.taxi.big_green_taxi`;


CREATE OR REPLACE TABLE `neon-concord-412616.taxi.optimized_green_taxi`
PARTITION BY DATE(pickup_timestamp)
AS
SELECT
  *
FROM
  `neon-concord-412616.taxi.intermediate_green_taxi`;


SELECT DISTINCT PULocationID
FROM `neon-concord-412616.taxi.optimized_green_taxi`
WHERE
  TIMESTAMP_SECONDS(DIV(lpep_pickup_datetime, 1000000000))
    BETWEEN TIMESTAMP('2022-06-01 00:00:00')
    AND TIMESTAMP('2022-06-30 23:59:59');

SELECT DISTINCT PULocationID
FROM `neon-concord-412616.taxi.intermediate_green_taxi`
WHERE
  TIMESTAMP_SECONDS(DIV(lpep_pickup_datetime, 1000000000))
    BETWEEN TIMESTAMP('2022-06-01 00:00:00')
    AND TIMESTAMP('2022-06-30 23:59:59');
/Users/yemisi/Downloads/good_nexus

good_nexus