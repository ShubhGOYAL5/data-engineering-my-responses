-- Question 1: gloud version *
-- Install Google Cloud SDK. What's the version you have? (To get the version, run gcloud --version)

-- Google Cloud SDK 368.0.0


-- Question 3: Count records *
-- How many taxi trips were there on January 15?

select count(1) 
from ( 
	select to_char(tpep_pickup_datetime, 'DD-MM') as dd_mm_trip
	from yellow_taxi_trips
) x
where x.dd_mm_trip = '15-01';

-- Question 4: Largest tip for each day *
-- On which day it was the largest tip in January? (note: it's not a typo, it's "tip", not "trip")

select tpep_pickup_datetime, tpep_dropoff_datetime 
from yellow_taxi_trips 
where tip_amount in ( 
	select max(tip_amount) as max_tip_amt 
	from yellow_taxi_trips
)

-- Question 5: Most popular destination *
-- What was the most popular destination for passengers picked up in central park on January 14? Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

select y."DOLocationID", z."Zone", count(1) from ( select x.dd_mm_trip, *
from ( 
	select *, to_char(tpep_pickup_datetime, 'DD-MM') as dd_mm_trip
	from yellow_taxi_trips
) x
where x.dd_mm_trip = '14-01') y
left join zones z on y."DOLocationID"=z."LocationID"
group by y."DOLocationID", z."Zone"
order by count desc
limit 1

-- Question 6: Most expensive route *
-- What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? Enter two zone names separated by a slashFor example:"Jamaica Bay / Clinton East"If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

select z.col, z_1."Zone" from ( select regexp_split_to_table(col, '/') as col from ( select "PULocationID" || '/' || "DOLocationID" as col from ( select "PULocationID", "DOLocationID", avg(Total_amount)
from yellow_taxi_trips
group by "PULocationID", "DOLocationID"
order by avg desc
limit 1) x) y) z
join zones z_1 on z.col::INTEGER=z_1."LocationID" 