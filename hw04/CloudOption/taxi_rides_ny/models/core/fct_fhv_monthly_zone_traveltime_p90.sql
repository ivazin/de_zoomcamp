{{ config(materialized="view") }}

with
    temp as (
        select
            record_id,
            dispatching_base_num,
            pickup_location_id,
            dropoff_location_id,
            pickup_datetime,
            dropoff_datetime,
            timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration,
            pickup_zone,
            dropoff_zone,
            year,
            month
        from {{ ref("dim_fhv_trips") }}
        where
            pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
            and year = 2019
            and month = 11
    )
select
    year,
    month,
    pickup_location_id,
    dropoff_location_id,
    pickup_zone,
    dropoff_zone,
    percentile_cont(trip_duration, 0.90) over (
        partition by year, month, pickup_location_id, dropoff_location_id
    ) as p90
from temp
order by pickup_zone, p90 desc
