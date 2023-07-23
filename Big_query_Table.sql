CREATE OR REPLACE TABLE  data-project-prajwal.Uber_dataset_PG.Analytics AS (
SELECT 
f.trip_distance_id,
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pick.pickup_latitude,
pick.pickup_longitude,
drop.dropoff_latitude,
drop.dropoff_longitude,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM 

data-project-prajwal.Uber_dataset_PG.Fact_table f
JOIN  data-project-prajwal.Uber_dataset_PG.Dim_Datetime d  ON f.datetime_id=d.datetime_id
JOIN  data-project-prajwal.Uber_dataset_PG.Dim_passenger_count p  ON p.passenger_count_id=f.passenger_count_id  
JOIN  data-project-prajwal.Uber_dataset_PG.Dim_trip_distance t  ON t.trip_distance_id=f.trip_distance_id  
JOIN  data-project-prajwal.Uber_dataset_PG.Dim_rate_code  r ON r.rate_code_id=f.rate_code_id  
JOIN  data-project-prajwal.Uber_dataset_PG.Dim_pickup_location  pick ON pick.pickup_location_id=f.pickup_location_id
JOIN  data-project-prajwal.Uber_dataset_PG.Dim_dropoff_location  drop ON drop.dropoff_location_id=f.dropoff_location_id
JOIN  data-project-prajwal.Uber_dataset_PG.Dim_payment_type  pay ON pay.payment_type_id=f.payment_type_id)
;