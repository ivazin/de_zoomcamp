## HeadingHomework 01
Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/01-docker-terraform/homework.md

### Question 1. Version of pip
Commands:

    docker run -it --entrypoint bash python:3.12.8
    pip --version

Answer:

    pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
 
### Question 2. Docker networking and docker-compose (1 point)

    postgres:5432

### Question 3. Trip Segmentation Count

##### simple query, Trips up to 1 mile:
    SELECT COUNT(*)
    FROM green_tripdata
    WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
    AND trip_distance <= 1;
  
#### All at once:
    SELECT
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS trips_up_to_1_mile,
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS trips_1_to_3_miles,
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS trips_3_to_7_miles,
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS trips_7_to_10_miles,
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS trips_over_10_miles
    FROM green_tripdata
    WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01';

  ### Question 4. Longest trip for each day

    WITH daily_max_trip AS (
    SELECT
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_trip_distance
    FROM green_tripdata
    GROUP BY DATE(lpep_pickup_datetime)
    )
    SELECT
    pickup_day,
    max_trip_distance
    FROM daily_max_trip
    ORDER BY max_trip_distance DESC
    LIMIT 5;
 

## Question 5. Three biggest pickup zones

    SELECT
    tzl."Zone" AS pickup_zone,
    SUM(gt.total_amount) AS total_amount
    FROM green_tripdata gt
    JOIN taxi_zone_lookup tzl ON gt."PULocationID" = tzl."LocationID"
    WHERE DATE(gt.lpep_pickup_datetime) = '2019-10-18'
    GROUP BY tzl."Zone"
    HAVING SUM(gt.total_amount) > 13000
    ORDER BY total_amount DESC
    LIMIT 3;

## Question 6. Largest tip

    SELECT
    tzl_dropoff."Zone" AS dropoff_zone,
    MAX(gt.tip_amount) AS max_tip
    FROM green_tripdata gt
    JOIN taxi_zone_lookup tzl_pickup ON gt."PULocationID" = tzl_pickup."LocationID"
    JOIN taxi_zone_lookup tzl_dropoff ON gt."DOLocationID" = tzl_dropoff."LocationID"
    WHERE tzl_pickup."Zone" = 'East Harlem North'
    AND gt.lpep_pickup_datetime >= '2019-10-01'
    AND gt.lpep_pickup_datetime < '2019-11-01'
    GROUP BY tzl_dropoff."Zone"
    ORDER BY max_tip DESC
    LIMIT 10;

### Question 7. Terraform Workflow / Terraform
see [Terraform](terraform/)