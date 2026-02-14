select
    -- identifiers
    "VendorID" as vendor_id,
    "RatecodeID" as rate_code_id,
    "PULocationID" as pickup_location_id,
    "DOLocationID" as dropoff_location_id,

    -- timestamps
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    1 as trip_type, -- yellow taxi can only be ordered through street-hail (type = 1)
    
    -- financials
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    0 as ehail_fee, -- yellow taxi can only be ordered through street-hail
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type
from {{ source('postgres_raw', 'yellow_taxi_trips_2021_1') }}