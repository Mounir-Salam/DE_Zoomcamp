select
    -- identifiers
    vendorid as vendor_id,
    ratecodeid as rate_code_id,
    pulocationid as pickup_location_id,
    dolocationid as dropoff_location_id,

    -- timestamps
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    
    -- financials
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type
from {{ source('postgres_raw', 'green_taxi_trips_2025_11') }}