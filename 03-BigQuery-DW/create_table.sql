-- CREATE OR REPLACE EXTERNAL TABLE `project-de-pnt.nytaxi.external_yellow_tripdata` 
-- OPTIONS(
--   format='PARQUET',
--   uris=['gs://nyc-tl-data-pnt/trip data/ny_taxi_data.parquet']
-- );

-- select * from project-de-pnt.nytaxi.external_yellow_tripdata limit 10;

-- CREATE OR REPLACE TABLE `project-de-pnt.nytaxi.yellow_tripdata_non_partitioned`  AS
-- select * from project-de-pnt.nytaxi.external_yellow_tripdata;

-- CREATE OR REPLACE TABLE `project-de-pnt.nytaxi.yellow_tripdata_partitioned` 
-- PARTITION BY DATE(tpep_pickup_datetime)
-- AS select * from project-de-pnt.nytaxi.external_yellow_tripdata;


-- CREATE OR REPLACE TABLE `project-de-pnt.nytaxi.yellow_tripdata_partitioned_clustered` 
-- PARTITION BY DATE(tpep_pickup_datetime)
-- CLUSTER BY VendorID
-- AS select * from project-de-pnt.nytaxi.external_yellow_tripdata;