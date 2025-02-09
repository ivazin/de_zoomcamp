## Homework 03
### setup
1) Since it was recommended to use script, a created credentials file for Google Storage and uploaded data with nice load_yellow_taxi_data.py.


2) In BigQuery I created dataset (at same region as storage) and ran this query to create external query:

```
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_external_hw03`

OPTIONS (
format = 'PARQUET',
-- uris = [
-- 'gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-*.parquet'
-- ]

uris = [
'gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-01.parquet',
'gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-02.parquet',
'gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-03.parquet',
'gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-04.parquet',
'gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-05.parquet',
'gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-06.parquet'
]

);

```

  

I was possible to use masks like:

  

```
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_external_hw03`
OPTIONS (
format = 'PARQUET',
uris = ['gs://dezoomcamp_hw03_2025/yellow_tripdata_2024-*.parquet']
);
```

  

3) Then, for later queries comparisons, I created a native BigQuery table from the external table:

```
CREATE OR REPLACE TABLE de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_native_hw03 AS
SELECT * FROM de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_external_hw03;
```

  
   
### Question 1

```

SELECT COUNT(*) FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_external_hw03`;

```

Answer: 20332093

  
  

### Question 2

```

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids

FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_external_hw03`;

```

-- This query will process 0 B when run.

  
  

```

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids

FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_native_hw03`;

```

-- This query will process 155.12 MB when run

  
  

Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

  

  

### Question 3

```
SELECT PULocationID FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_native_hw03`;
```
vs
```
SELECT PULocationID, DOLocationID FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_native_hw03`;
```

Answer:

BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

  
  

### Question 4
```
SELECT COUNT(*) FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_native_hw03` WHERE fare_amount=0;
```
Answer: 8333


### Question 5

```
CREATE OR REPLACE TABLE `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_partclusted_hw03`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_native_hw03`;
```
Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

  

### Question 6
```
SELECT DISTINCT VendorID
FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_native_hw03`
WHERE
DATE(tpep_dropoff_datetime)>=DATE("2024-03-01")
AND
DATE(tpep_dropoff_datetime)<=DATE('2024-03-15');
```
-- This query will process 310.24 MB when run.

  

```
SELECT DISTINCT VendorID
FROM `de-zoomcamp-412119.my_taxi_dataset.yellow_tripdata_partclusted_hw03`
WHERE
DATE(tpep_dropoff_datetime)>=DATE("2024-03-01")
AND
DATE(tpep_dropoff_datetime)<=DATE('2024-03-15');
```
-- This query will process 26.84 MB when run.

 
Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

    
## Question 7
Answer: GCP Bucket
 
  

## Question 8
Answer: False