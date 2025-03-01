## Homework 04
### Cloud setup

Steps to reproduce: 

#### Prepare data

Load source data to GCS bucket called `dezoomcamp_hw04_2025` with script [upload.py](./CloudOption/uploader.py) (or we may use colab for faster speeds).
  
Creat `my_dataset` in BigQuery, load the source data from GCS to BigQuery with:

    CREATE OR REPLACE EXTERNAL TABLE `my_dataset.green_tripdata`
    OPTIONS (
    format = 'CSV',
    compression = 'GZIP',
    uris = ['gs://dezoomcamp_hw04_2025/green_tripdata_2019-*.csv.gz', 'gs://dezoomcamp_hw04_2025/green_tripdata_2020-*.csv.gz']
    );
    
    CREATE OR REPLACE EXTERNAL TABLE `my_dataset.yellow_tripdata`
    OPTIONS (
    format = 'CSV',
    compression = 'GZIP',
    uris = ['gs://dezoomcamp_hw04_2025/yellow_tripdata_2019-*.csv.gz', 'gs://dezoomcamp_hw04_2025/yellow_tripdata_2020-*.csv.gz']
    );
    
    CREATE OR REPLACE EXTERNAL TABLE `my_dataset.fhv_tripdata`
    OPTIONS (
    format = 'CSV',
    compression = 'GZIP',
    uris = ['gs://dezoomcamp_hw04_2025/fhv_tripdata_2019-*.csv.gz']
    );

#### Prepare dbt cloud

Creat project in dbt cloud.

Creat a connection to BigQuery (double check the correct location :) And of course you need credentials.json ready with correct rights for it).

Upload all files from [hw04/CloudOption/taxi_rides_ny](./CloudOption/taxi_rides_ny) to dbt cloud project.

#### Run dbt
    dbt build --vars '{'is_test_run': 'false'}'
    dbt run
  
### Questions

**Question 1:** Understanding dbt model resolution

 **Answer:**

    select * from myproject.raw_nyc_tripdata.ext_green_taxi

Description:
database == DBT_BIGQUERY_PROJECT (defined) == myproject
schema == DBT_BIGQUERY_SOURCE_DATASET undefined, so == raw_nyc_tripdata

  

**Question 2:** dbt Variables & Dynamic Models  

**Answer:**

    pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

Description:
We need to use a combination of var and env_var to ensure that command-line arguments will work over environment variables, which in turn take precedence over default values.

  
**Question 3:** dbt Data Lineage and Execution

**Answer:**

    dbt run --select +models/core/dim_taxi_trips.sql+ --target prod

Description:
It's because fct_taxi_monthly_zone_revenue is not a dependency or child of dim_taxi_trips, and thus would not be materialized by this command.
 

**Question 4:** dbt Macros and Jinja

**Answer:**

 - TRUE — Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is
   mandatory, or it'll fail to compile
 - FALSE — Setting a value for
   DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to
   compile
 - TRUE — When using core, it materializes in the dataset
   defined in DBT_BIGQUERY_TARGET_DATASET 
 - TRUE — When using stg, it
   materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET,
   or defaults to DBT_BIGQUERY_TARGET_DATASET
 - TRUE — When using staging,
   it materializes in the dataset defined in
   DBT_BIGQUERY_STAGING_DATASET, or defaults to
   DBT_BIGQUERY_TARGET_DATASET

  
**Question 5:** Taxi Quarterly Revenue Growth
  
Made [hw04/CloudOption/taxi_rides_ny/models/core/fct_taxi_trips_quarterly_revenue.sql](./CloudOption/taxi_rides_ny/models/core/fct_taxi_trips_quarterly_revenue.sql)

SQL:

    SELECT  *  FROM  `de-zoomcamp-412119.dbt_izinenko.fct_taxi_trips_quarterly_revenue`
    where  year=2020
    ORDER  by  service_type  ASC, year  ASC, quarter  ASC;

**Answer:**
green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
 

**Question 6:** P97/P95/P90 Taxi Monthly Fare

Made [hw04/CloudOption/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql](./CloudOption/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql)

  **Answer:**
  green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
  
 
**Question 7:** Top #Nth longest P90 travel time Location for FHV

 Made [hw04/CloudOption/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql](./CloudOption/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql)
 
 Made [hw04/CloudOption/taxi_rides_ny/models/core/dim_fhv_trips.sql](./CloudOption/taxi_rides_ny/models/core/dim_fhv_trips.sql)

Made [hw04/CloudOption/taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql](./CloudOption/taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql)
 
SQL:

    WITH ranked_zones AS (
      SELECT
        pickup_zone,
        dropoff_zone,
        p90,
        ROW_NUMBER() OVER (PARTITION BY pickup_zone ORDER BY p90 DESC) AS rank
      FROM
        `de-zoomcamp-412119.dbt_izinenko.fct_fhv_monthly_zone_traveltime_p90`
      GROUP BY
        pickup_zone,
        dropoff_zone,
        p90
    )
    SELECT
      pickup_zone,
      dropoff_zone,
      p90
    FROM
      ranked_zones
    WHERE
      rank = 2
    ORDER BY
      pickup_zone ASC;

  **Answer:**
LaGuardia Airport, Chinatown, Garment District

