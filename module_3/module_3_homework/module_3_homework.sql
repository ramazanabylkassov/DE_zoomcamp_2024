-- MODULE 3 HOMEWORK

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `fresh-ocean-412204.ny_taxi.green_cab_data_2022_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://module_3_homework_ramazan/green_taxi_data_2022/5bbb8ff2d4f34a11b3f15710294ba48a-0.parquet']
);

-- Create a non-partitioned non-clustered table from external table
CREATE OR REPLACE TABLE fresh-ocean-412204.ny_taxi.green_cab_data_2022_bq AS
SELECT 
  VendorID,
  CAST(TIMESTAMP_SECONDS(CAST(lpep_pickup_datetime / 1000000000 AS INT64)) AS DATETIME) AS lpep_pickup_datetime, 
  CAST(TIMESTAMP_SECONDS(CAST(lpep_dropoff_datetime / 1000000000 AS INT64)) AS DATETIME) AS lpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_external;

-- Question 1
SELECT COUNT(*) FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_external; 
-- 840402 rows

-- Question 2
-- external table
SELECT DISTINCT(PULocationID) FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_external;
-- 0 B

-- bq table 
SELECT DISTINCT(PULocationID) FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_bq;
-- 6.41 MB

-- Question 3
SELECT COUNT(*)
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_external
WHERE fare_amount = 0;
-- 1622

-- Question 4
-- Partition by lpep_pickup_datetime  Cluster on PUlocationID
CREATE OR REPLACE TABLE fresh-ocean-412204.ny_taxi.green_cab_data_2022_part_clust
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS 
SELECT * FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_bq;

-- Question 5
-- non-partitioned table
SELECT DISTINCT(PULocationID) 
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_bq
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
-- 12.82 MB

-- partitioned table
SELECT DISTINCT(PULocationID) 
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_part_clust
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
-- 1.12 MB

-- Question 8
SELECT COUNT(*)
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_bq;
-- 0 B (cached query)