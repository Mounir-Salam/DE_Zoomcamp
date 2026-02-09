```sql
-- Create external table of the yellow taxis for 2024
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-484617.de_dataset.yellow_tripdata_ext`
OPTIONS (
  FORMAT = 'PARQUET',
  uris = ['gs://de-zoomcamp-484617-function_bucket/yellow_tripdata_2024_*.parquet']
);
```

### Question 2
```sql
-- Distinct PULocationID from regular table
SELECT DISTINCT PULocationID
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata`;

-- Distinct PULocationID from external table
SELECT DISTINCT PULocationID
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata_ext`;
```

### Question 3
```sql
-- PULocationID from regular table
SELECT PULocationID
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata`;

-- PULocationID & DOLocationID from regular table
SELECT PULocationID, DOLocationID
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata`;
```

### Question 4
```sql
-- fare_amount is 0
SELECT COUNT(1)
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata`
WHERE fare_amount = 0;
```

### Question 5
```sql
-- create partitioned and clustered table
CREATE OR REPLACE TABLE `de-zoomcamp-484617.de_dataset.yellow_tripdata_par_clus`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
  SELECT *
  FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata`;
```

### Question 6
```sql
-- Distinct VendorID from normal table
SELECT DISTINCT VendorID
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Distinct VendorID from partitioned table
SELECT DISTINCT VendorID
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata_par_clus`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

### Question 9
```sql
-- Select count(*)
SELECT count(*)
FROM `de-zoomcamp-484617.de_dataset.yellow_tripdata`;
```
