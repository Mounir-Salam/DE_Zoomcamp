with
    payment_type_lookup as (
        select * from {{ ref("payment_type_lookup") }}
    )
select * from renamed