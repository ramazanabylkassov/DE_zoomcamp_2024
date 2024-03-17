**Question 1 & 2:**
```
CREATE MATERIALIZED VIEW homework_1 AS
SELECT
    pl.zone AS pickup_zone_name,
    dl.zone AS dropoff_zone_name,
    AVG(t.tpep_dropoff_datetime - t.tpep_pickup_datetime) AS average,
    MIN(t.tpep_dropoff_datetime - t.tpep_pickup_datetime) AS minimum,
    MAX(t.tpep_dropoff_datetime - t.tpep_pickup_datetime) AS maximum,
    COUNT(*) AS count -- Corrected to count all rows in each group
FROM
    trip_data t
JOIN
    taxi_zone pl ON t.pulocationid = pl.location_id
JOIN
    taxi_zone dl ON t.dolocationid = dl.location_id
GROUP BY pl.zone, dl.zone; -- Corrected to match selected zone names
```
`SELECT * FROM homework_1 ORDER BY average DESC LIMIT 10;`


**Question 3:**
```
CREATE MATERIALIZED VIEW homework_2 AS
SELECT pl.zone, COUNT(pl.zone) AS pickup_zone_count
FROM trip_data t
JOIN taxi_zone pl ON t.pulocationid = pl.location_id
WHERE t.tpep_pickup_datetime BETWEEN
    (SELECT MAX(tpep_pickup_datetime) - INTERVAL '17 hours' FROM trip_data LIMIT 1)
    AND
    (SELECT MAX(tpep_pickup_datetime) FROM trip_data LIMIT 1)
GROUP BY pl.zone;
```
`SELECT * FROM homework_2 ORDER BY pickup_zone_count DESC LIMIT 10;`