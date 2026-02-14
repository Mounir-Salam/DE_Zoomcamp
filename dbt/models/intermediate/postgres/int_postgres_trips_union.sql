with
    green_tripdata as (
        select *
        from {{ ref("stg_postgres__green_trip") }}
    ),
    yellow_tripdata as (
        select *
        from {{ ref("stg_postgres__yellow_trip") }}
    ),
    trips_unioned as (
        select * from green_tripdata
        union all
        select * from yellow_tripdata
    )
select *
from trips_unioned