
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/01-docker-terraform/homework.md

  
Downloads:
curl -O -L https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
curl -O -L https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
gunzip green_tripdata_2019-09.csv.gz

Count lines:
wc -l green_tripdata_2019-09.csv

### Question 1

    docker run --help | grep "Automatically remove the container when it exits"

Result:

    --rm

### Question 2
Go inside docker:

    docker run -it python:3.9 bash
    pip list | grep "wheel"

Result:

    wheel 0.42.0

  
  
### Question 3

    select COUNT(*)
    from green_tripdata gt
    where gt.lpep_pickup_datetime BETWEEN '2019-09-18 00:00:00' AND '2019-09-18 23:59:59'
    and gt.lpep_dropoff_datetime BETWEEN '2019-09-18 00:00:00' AND '2019-09-18 23:59:59'

Result:

    15612

  
  

### Question 4

    select count(*) as cnt, DATE(lpep_pickup_datetime) AS date_only from green_tripdata gt
    group by DATE(lpep_pickup_datetime)
    having DATE(lpep_pickup_datetime) IN ('2019-09-18', '2019-09-16', '2019-09-26', '2019-09-21')
    order by cnt DESC
  
Result:

    16497 2019-09-26
    16428 2019-09-21
    15767 2019-09-18
    14636 2019-09-16

  
  

### Question 5

    select
    COUNT(*),
    SUM(total_amount) as tot_am_sum,
    z2."Borough"
    from
    green_tripdata gt,
    zones z2
    where
    gt."PULocationID" = z2."LocationID"
    AND DATE(gt.lpep_pickup_datetime) in ('2019-09-18')
    AND gt."PULocationID" not in (
    SELECT
    "LocationID"
    FROM
    zones z
    WHERE
    "Borough" ILIKE 'Unknown'
    )
    
    group by
    z2."Borough"
    having
    SUM(total_amount) > 50000
    order by
    SUM(total_amount) DESC

Result:

    4458 96333.24000000356 Brooklyn
    5575 92271.30000000495 Manhattan
    4393 78671.71000000412 Queens


  

### Question 6

    select
    MAX(gt.tip_amount) as tips_max,
    z2."Zone"
    from
    green_tripdata gt,
    zones z1,
    zones z2
    where
    gt."PULocationID" = z1."LocationID"
    and gt."DOLocationID" = z2."LocationID"
    and EXTRACT(YEAR FROM gt.lpep_pickup_datetime) = 2019
    and EXTRACT(MONTH FROM gt.lpep_pickup_datetime) = 9
    and z1."Zone" ilike 'Astoria'
    group by z2."Zone"
    order by tips_max desc
    limit 3

Result:

    62.31 JFK Airport
    30.0 Woodside
    28.0 Kips Bay

### Question 7
see [Terraform](terraform/)