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


### Spark

#### Question 2
```Python
df = spark.read \
    .parquet('yellow_tripdata_2025-11.parquet')

df = df.repartition(4)
df.write.parquet('taxi_data', mode='overwrite')
```

#### Question 3
```Python
# count trips that started on 2025-11-01
from pyspark.sql.functions import col
print(df.count())
print(df.filter(col('tpep_pickup_datetime').cast('date') == '2025-11-15').count())
```

#### Question 4
```Python
# longest trip in hours
from pyspark.sql.functions import col
from pyspark.sql import functions as F
df_duration = df \
    .withColumn('duration', F.expr("timestampdiff(HOUR, tpep_pickup_datetime, tpep_dropoff_datetime)")) \
    .withColumn('duration_2', ((F.unix_timestamp(col('tpep_dropoff_datetime')) - F.unix_timestamp(col('tpep_pickup_datetime'))) / 3600).cast("decimal(10,1)"))
df_duration.orderBy(col('duration').desc()).show(5)
```

#### Question 6
```Python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
zone_schema = StructType(
    [
        StructField('LocationID', IntegerType(), True),
        StructField('Borough', StringType(), True), 
        StructField('Zone', StringType(), True), 
        StructField('service_zone', StringType(), True)
    ]
)

zones = spark.read \
    .option("header", "true") \
    .schema(zone_schema) \
    .csv('taxi_zone_lookup.csv')


df_zone_freq = df \
    .select('PULocationID') \
    .join(zones, df.PULocationID == zones.LocationID) \
    .groupBy('Zone') \
    .count() \
    .orderBy('count', ascending=True)

df_zone_freq.limit(5).show()
```