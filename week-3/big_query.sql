
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-338618.trips_data_all.yellow_tripdata_all`
 OPTIONS (
     format = 'PARQUET',
     uris = ['gs://dtc_data_lake_dtc-de-338618/yellow/yellow_tripdata/2019/yellow_tripdata_2019*.parquet', 'gs://dtc_data_lake_dtc-de-338618/yellow/yellow_tripdata/2020/yellow_tripdata_2020*.parquet']
 )

 CREATE OR REPLACE EXTERNAL TABLE `dtc-de-338618.trips_data_all.fhv_tripdata_all`
 OPTIONS (
     format = 'PARQUET',
     uris = ['gs://dtc_data_lake_dtc-de-338618/raw/fhv_tripdata/2019/fhv_tripdata_2019*.parquet', 'gs://dtc_data_lake_dtc-de-338618/raw/fhv_tripdata/2020/fhv_tripdata_2020*.parquet']
 )

create or replace table `dtc-de-338618.trips_data_all.fhv_tripdata_all_partitioned` 
partition by 
    date(pickup_datetime) as
-- select * from `dtc-de-338618.trips_data_all.fhv_tripdata_all`
select 
    dispatching_base_num,
    cast(pickup_datetime as datetime) as pickup_datetime,
    cast(dropoff_datetime as datetime) as dropoff_datetime,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from `dtc-de-338618.trips_data_all.fhv_tripdata_all`

create or replace table `dtc-de-338618.trips_data_all.fhv_tripdata_all_clustered` 
partition by 
    date(pickup_datetime)
cluster by dispatching_base_num as
-- select * from `dtc-de-338618.trips_data_all.fhv_tripdata_all`
select 
    dispatching_base_num,
    cast(pickup_datetime as datetime) as pickup_datetime,
    cast(dropoff_datetime as datetime) as dropoff_datetime,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from `dtc-de-338618.trips_data_all.fhv_tripdata_all`

select count(dispatching_base_num)
from `dtc-de-338618.trips_data_all.fhv_tripdata_all_clustered` 
where 
    extract(year from pickup_datetime)=2019;

select count(distinct(dispatching_base_num))
from `dtc-de-338618.trips_data_all.fhv_tripdata_all_clustered` 
where 
    extract(year from pickup_datetime)=2019;

select count(dispatching_base_num)
from `dtc-de-338618.trips_data_all.fhv_tripdata_all_clustered` 
where 
    dispatching_base_num IN ('B00987', 'B02060', 'B02279') and 
    date(pickup_datetime) between '2019-01-01' and '2019-03-31';
