-- All commands are made on the green_cab_data table downloaded via Mage to GCS from 2022 year



-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `fresh-ocean-412204.ny_taxi.green_cab_data_2022`
OPTIONS (
  format = 'parquet',
  uris = ['gs://module_3_homework_ramazan/green_taxi_data_2022/5bbb8ff2d4f34a11b3f15710294ba48a-0.parquet']
);

-- Check green trip data
SELECT * FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022 limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE fresh-ocean-412204.ny_taxi.green_cab_data_2022_non_partitioned AS
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
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022;

-- check the non_partitioned table
SELECT * FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_non_partitioned LIMIT 100;
SELECT DISTINCT(lpep_dropoff_datetime) FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_non_partitioned;
SELECT COUNT(*) FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_non_partitioned;

-- create a partitioned table from external table
CREATE OR REPLACE TABLE fresh-ocean-412204.ny_taxi.green_cab_data_2022_partitioned
PARTITION BY 
  DATE(lpep_pickup_datetime) AS
SELECT * FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_non_partitioned;

-- check parition schema
SELECT * FROM fresh-ocean-412204.ny_taxi.INFORMATION_SCHEMA.PARTITIONS;

-- Impact of partition
-- Scanning 12.82 MB of data
SELECT DISTINCT(VendorID)
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-30';

-- Scanning 7.25 MB of data
SELECT DISTINCT(VendorID)
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_cab_data_2022_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE fresh-ocean-412204.ny_taxi.green_cab_data_2022_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_non_partitioned;

-- Compate partitioned vs paritioned & clustered
-- Query scans 7.28 MB
SELECT count(*) as trips
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31' AND VendorID=1;

-- Query scans 7.07 MB
SELECT count(*) as trips
FROM fresh-ocean-412204.ny_taxi.green_cab_data_2022_partitioned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31' AND VendorID=1;