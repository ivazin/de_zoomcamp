1) Since it was not recommended to use Mage I simply uploaded parquet files to my Google Storage.


2) In BigQuery I created dataset (at same region as storage) and ran this query to create external query:
```
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-412119.my_taxi_dataset.green_data_external_hw03`
OPTIONS (
  format = 'PARQUET',
  -- uris = [
  --   'gs://green-taxi-2022/green_tripdata_2022-*.parquet'
  --   ]
  uris = [
    'gs://green-taxi-2022/green_tripdata_2022-01.parquet', 
    'gs://green-taxi-2022/green_tripdata_2022-02.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-03.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-04.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-05.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-06.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-07.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-08.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-09.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-10.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-11.parquet',
    'gs://green-taxi-2022/green_tripdata_2022-12.parquet'
    ]
);
```

3) Creating BigQuery table from external table:
```
CREATE OR REPLACE TABLE de-zoomcamp-412119.my_taxi_dataset.green_data_internal_hw03 AS 
SELECT * FROM de-zoomcamp-412119.my_taxi_dataset.green_data_external_hw03;
```


Question 1
```
SELECT COUNT(*) FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_external_hw03`;
```
Answer: 840402


Question 2
```
SELECT COUNT(DISTINCT PULocationID) FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_internal_hw03`;
```
-- This query will process 6.41 MB when run
```
SELECT COUNT(DISTINCT PULocationID) FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_external_hw03`;
```
-- This query will process 0 B when run.

Answer: 0 MB for the External Table and 6.41MB for the Materialized Table


Question 3
```
SELECT COUNT(*) FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_internal_hw03` WHERE fare_amount=0;
SELECT COUNT(*) FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_external_hw03` WHERE fare_amount=0;
```
Answer: 1622


Question 4
```
CREATE OR REPLACE TABLE de-zoomcamp-412119.my_taxi_dataset.green_data_internal_hw03_partitoned 
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS
SELECT * FROM de-zoomcamp-412119.my_taxi_dataset.green_data_external_hw03;
```
Answer: Cluster on lpep_pickup_datetime Partition by PUlocationID


Question 5
```
SELECT DISTINCT PUlocationID
FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_internal_hw03`
WHERE
  DATE(lpep_pickup_datetime) >= DATE("2022-06-01")
  AND DATE(lpep_pickup_datetime) <= DATE("2022-06-30");
```
-- This query will process 12.82 MB when run.
```
SELECT DISTINCT PUlocationID
FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_internal_hw03_partitoned`
WHERE
  DATE(lpep_pickup_datetime) >= DATE("2022-06-01")
  AND DATE(lpep_pickup_datetime) <= DATE("2022-06-30");
```
-- This query will process 1.12 MB when run.

Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

Question 6
Answer: GCP Bucket


Question 7
Answer: False

Question 8
```
SELECT count(*) FROM `de-zoomcamp-412119.my_taxi_dataset.green_data_internal_hw03_partitoned`
```

Answer: 