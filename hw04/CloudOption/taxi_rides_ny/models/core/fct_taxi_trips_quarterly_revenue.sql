{{ config(materialized="table") }}

with
    temp as (
        select
            {{ dbt.date_trunc("year", "pickup_datetime") }} as year,
            {{ dbt.date_trunc("quarter", "pickup_datetime") }} as quarter,
            service_type,
            total_amount
        from {{ ref("fact_trips") }}
    ),

    grouped as (
        select
            service_type,
            extract(year from quarter) as year,
            extract(quarter from quarter) as quarter,
            sum(total_amount) as total_amount
        from temp
        group by service_type, year, quarter
    ),

    yoy_comparison as (
        select
            g1.service_type,
            g1.year,
            g1.quarter,
            g1.total_amount as current_quarter_revenue,
            g2.total_amount as previous_quarter_revenue,
            case
                when g2.total_amount is not null
                then ((g1.total_amount - g2.total_amount) / g2.total_amount) * 100
                else null
            end as yoy_growth
        from grouped g1
        left join
            grouped g2
            on g1.service_type = g2.service_type
            and g1.year = g2.year + 1
            and g1.quarter = g2.quarter
    )

select *
from yoy_comparison
