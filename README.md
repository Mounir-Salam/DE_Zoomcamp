### Question 1

process:
```bash
docker run -it --rm --entrypoint=bash python:3.13.11
```

```bash
pip -V
```
Answer: 25.3

### Question 3

```postgreSQL
-- Got 8009 actually no 8007
SELECT count(*)
FROM public.green_taxi_trips_2025_11
WHERE trip_distance <= 1
```

### Question 4

```postgreSQL
SELECT lpep_pickup_datetime, trip_distance
FROM public.green_taxi_trips_2025_11
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1
```

### Question 5
```postgreSQL
SELECT
    zpu."Zone",
	SUM(gt.total_amount) as total_amount,
	CAST(gt.lpep_pickup_datetime AS DATE) AS "day"
FROM
	public.green_taxi_trips_2025_11 as gt
JOIN
	public.zones as zpu
		ON gt."PULocationID" = zpu."LocationID"
WHERE
	CAST(gt.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY
	CAST(gt.lpep_pickup_datetime AS DATE),
	zpu."Zone"
ORDER BY
	total_amount DESC
LIMIT 1
```

### Question 6
```postgreSQL
SELECT
	zdo."Zone",
	gt.tip_amount
FROM
	public.green_taxi_trips_2025_11 as gt
JOIN
	public.zones as zpu
		ON gt."PULocationID" = zpu."LocationID"
JOIN 
	public.zones as zdo
		ON gt."DOLocationID" = zdo."LocationID"
WHERE
	zpu."Zone" = 'East Harlem North'
ORDER BY
	gt.tip_amount DESC
LIMIT 1
```