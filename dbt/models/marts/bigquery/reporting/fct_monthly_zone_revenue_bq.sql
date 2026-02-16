select
    -- Grouping dimensions
    pickup_zone,
    cast(date_trunc(pickup_datetime, month) as date) as revenue_month,
    service_type,
    
    -- Revenue Data
    sum(fare_amount) as fare_amount_revenue,
    sum(extra) as extra_revenue,
    sum(mta_tax) as mta_tax_revenue,
    sum(tip_amount) as tip_amount_revenue,
    sum(ehail_fee) as ehail_fee_revenue,
    sum(tolls_amount) as tolls_amount_revenue,
    sum(improvement_surcharge) as improvement_surcharge_revenue,
    sum(total_amount) as total_amount_revenue,

    -- Additional Dimensions
    count(trip_id) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance
from {{ ref("fct_trips_bq") }}
group by pickup_zone, revenue_month, service_type