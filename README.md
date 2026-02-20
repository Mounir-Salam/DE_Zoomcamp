### dbt

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

### dlt

#### Question 1
```SQL
SELECT 
MIN(trip_pickup_date_time),
MAX(trip_dropoff_date_time)
FROM `de-zoomcamp-484617.dlt_taxi_dataset.taxi_api`
``` 


#### Question 2
```SQL
WITH totals AS (
    -- Calculate the total count of all records
    SELECT
        COUNT(*) AS total_count
    FROM
        `de-zoomcamp-484617.dlt_taxi_dataset.taxi_api`
)
-- Calculate the percentage for a specific category, e.g., 'Electronics'
SELECT
    COUNT(*) * 100.0 / (SELECT total_count FROM totals) AS percentage
FROM
    `de-zoomcamp-484617.dlt_taxi_dataset.taxi_api`
WHERE
    payment_type = 'Credit'
```

#### Question 3
```SQL
SELECT
  SUM(tip_amt)
FROM `de-zoomcamp-484617.dlt_taxi_dataset.taxi_api`
```