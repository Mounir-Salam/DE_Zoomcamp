with
    payment_type_lookup as (
        select * from {{ source('bigquery_raw', 'payment_type_lookup') }}
    )
select * from payment_type_lookup