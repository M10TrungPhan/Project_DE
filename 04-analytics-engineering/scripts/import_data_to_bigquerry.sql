CREATE TABLE   `project-de-pnt.trip_data_pnt.green_tripdata`  AS
select * from `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`

INSERT INTO   `project-de-pnt.trip_data_pnt.green_tripdata`  
select * from `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`;

CREATE TABLE   `project-de-pnt.trip_data_pnt.yellow_tripdata`  AS
select * from `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`

INSERT INTO   `project-de-pnt.trip_data_pnt.yellow_tripdata`  
select * from `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`;


  -- Fixes yellow table schema
ALTER TABLE `project-de-pnt.trip_data_pnt.yellow_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `project-de-pnt.trip_data_pnt.yellow_tripdata`
  RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE `project-de-pnt.trip_data_pnt.yellow_tripdata`
  RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
ALTER TABLE `project-de-pnt.trip_data_pnt.yellow_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `project-de-pnt.trip_data_pnt.yellow_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `project-de-pnt.trip_data_pnt.yellow_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `project-de-pnt.trip_data_pnt.yellow_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;

  -- Fixes green table schema
ALTER TABLE `project-de-pnt.trip_data_pnt.green_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `project-de-pnt.trip_data_pnt.green_tripdata`
  RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
ALTER TABLE `project-de-pnt.trip_data_pnt.green_tripdata`
  RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
ALTER TABLE `project-de-pnt.trip_data_pnt.green_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `project-de-pnt.trip_data_pnt.green_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `project-de-pnt.trip_data_pnt.green_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `project-de-pnt.trip_data_pnt.green_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;