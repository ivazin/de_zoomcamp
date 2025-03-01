{{ config(materialized="view") }}

with
    fhv_tripdata as (
        select *
        from {{ source("staging", "fhv_tripdata") }}
        where dispatching_base_num is not null
    )
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as record_id,
    dispatching_base_num,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} as dropoff_location_id,
    Affiliated_base_number,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag

from fhv_tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}