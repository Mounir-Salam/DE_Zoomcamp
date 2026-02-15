with
    green_tripdata as (
        select
            *,
            'Green' as service_type
        from {{ ref("stg_bigquery__green_trip") }}
    ),
    yellow_tripdata as (
        select
            *,
            'Yellow' as service_type
        from {{ ref("stg_bigquery__yellow_trip") }}
    ),
    trips_unioned as (
        select * from green_tripdata
        union all
        select * from yellow_tripdata
    )
select *
from trips_unioned