{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id',
        incremental_strategy = 'merge',
        on_schema_change='append_new_columns'
    )
}}

select
    -- trip identifiers
    trips.trip_id,
    trips.vendor_id,
    vendors.vendor_name,
    trips.service_type,
    trips.rate_code_id,

    -- Location details (enriched)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- timestamps
    trips.pickup_datetime,
    trips.dropoff_datetime,

    -- trip info
    trips.store_and_fwd_flag,
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    {{ get_trip_duration_minutes('trips.pickup_datetime', 'trips.dropoff_datetime') }} as trip_duration_minutes,

    -- financials
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.ehail_fee,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from {{ ref('int_bigquery_trips') }} as trips
left join {{ ref("dim_zones_bq") }} as pz
    on trips.pickup_location_id = pz.location_id
left join {{ ref("dim_zones_bq") }} as dz
    on trips.dropoff_location_id = dz.location_id
left join {{ ref("dim_vendors_bq") }} as vendors
    on trips.vendor_id = vendors.vendor_id

{% if is_incremental() %}
-- Only process new trips based on pickup datetime
where trips.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}