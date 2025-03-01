{{ config(materialized="table") }}


with
    filtered as (
        select
            service_type,
            extract(year from pickup_datetime) as year,
            extract(month from pickup_datetime) as month,
            fare_amount
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ('Cash', 'Credit card')
    ),

    percentiles as (
        select
            service_type,
            year,
            month,
            percentile_cont(fare_amount, 0.97) over (
                partition by service_type, year, month
            ) as p97_fare,
            percentile_cont(fare_amount, 0.95) over (
                partition by service_type, year, month
            ) as p95_fare,
            percentile_cont(fare_amount, 0.90) over (
                partition by service_type, year, month
            ) as p90_fare
        from filtered
    )

select distinct service_type, year, month, p90_fare, p95_fare, p97_fare
from percentiles
