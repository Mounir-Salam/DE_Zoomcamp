Note: dbt still on branch dbt-branch

```SQL
SELECT count(*) FROM `de-zoomcamp-484617.dbt_prod.fct_monthly_zone_revenue_bq`
where Extract(year from revenue_month) < 2023
and Extract(year from revenue_month) > 2018;
```

```SQL
select pickup_zone, service_type
from `de-zoomcamp-484617.dbt_prod.fct_monthly_zone_revenue_bq`
where revenue_month = '2020-01-01';
```

```SQL
-- Zone with highest revenue for Green taxis in 2020
SELECT
  pickup_zone,
  total_amount_revenue
FROM `de-zoomcamp-484617.dbt_prod.fct_monthly_zone_revenue_bq`
WHERE 
  revenue_month = '2020-01-01'
  AND service_type = 'Green'
ORDER BY total_amount_revenue DESC
LIMIT 1;
```

```SQL
-- Total trips for Green taxis in October 2019
SELECT
  sum(total_monthly_trips) as total_trips
from `de-zoomcamp-484617.dbt_prod.fct_monthly_zone_revenue_bq`
where service_type = "Green"
  AND revenue_month = "2019-10-01";
```