CREATE OR REPLACE  EXTERNAL TABLE `project-de-pnt.ny_taxi_pnt.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-tl-data-pnt/trip data/ny_taxi_data*.parquet']
);

select count(1) from project-de-pnt.ny_taxi_pnt.external_green_tripdata;

CREATE OR REPLACE TABLE `project-de-pnt.ny_taxi_pnt.green_tripdata_normal` AS
SELECT * FROM `project-de-pnt.ny_taxi_pnt.external_green_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM project-de-pnt.ny_taxi_pnt.external_green_tripdata;
SELECT COUNT(DISTINCT(PULocationID)) FROM project-de-pnt.ny_taxi_pnt.green_tripdata_normal;

SELECT COUNT(1) FROM project-de-pnt.ny_taxi_pnt.external_green_tripdata WHERE fare_amount =0;

CREATE OR REPLACE TABLE `project-de-pnt.ny_taxi_pnt.green_tripdata_partitioned_clusterd`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS SELECT * FROM `project-de-pnt.ny_taxi_pnt.external_green_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM project-de-pnt.ny_taxi_pnt.green_tripdata_normal
WHERE DATE(lpep_pickup_datetime)  BETWEEN '2022-06-01'  AND "2022-06-30";

SELECT COUNT(DISTINCT(PULocationID)) FROM project-de-pnt.ny_taxi_pnt.green_tripdata_partitioned_clusterd
WHERE DATE(lpep_pickup_datetime)  BETWEEN '2022-06-01'  AND "2022-06-30";
SELECT * FROM project-de-pnt.ny_taxi_pnt.green_tripdata_normal
