-- Drop the database if it exists
-- DROP DATABASE IF EXISTS team29_projectdb CASCADE;

-- Create the database at the specified location
-- CREATE DATABASE team29_projectdb LOCATION 'project/hive/warehouse';

-- Use the new database
USE team29_projectdb;

-- Drop tables if they exist
DROP TABLE IF EXISTS stations;
DROP TABLE IF EXISTS records;
DROP TABLE IF EXISTS records_staging;

-- Create stations table (no partitioning needed)
CREATE EXTERNAL TABLE stations (
    id STRING,
    latitude FLOAT,
    longitude FLOAT
)
STORED AS PARQUET
LOCATION 'project/warehouse/stations'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Create a staging table for records (no partitioning)
CREATE EXTERNAL TABLE records_staging (
    record_id INT,
    station_id STRING,
    airnow_ozone FLOAT,
    cmaq_ozone FLOAT,
    cmaq_no2 FLOAT,
    cmaq_co FLOAT,
    cmaq_oc FLOAT,
    pressure FLOAT,
    pbl FLOAT,
    temperature FLOAT,
    wind_speed FLOAT,
    wind_direction FLOAT,
    radiation FLOAT,
    cloud_fraction FLOAT,
    month INT,
    day INT,
    hour INT
)
STORED AS PARQUET
LOCATION 'project/warehouse/records'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Create the optimized records table with partitioning and bucketing
CREATE EXTERNAL TABLE records (
    record_id INT,
    station_id STRING,
    airnow_ozone FLOAT,
    cmaq_ozone FLOAT,
    cmaq_no2 FLOAT,
    cmaq_co FLOAT,
    cmaq_oc FLOAT,
    pressure FLOAT,
    pbl FLOAT,
    temperature FLOAT,
    wind_speed FLOAT,
    wind_direction FLOAT,
    radiation FLOAT,
    cloud_fraction FLOAT,
    hour INT
)
PARTITIONED BY (month INT, day INT)
CLUSTERED BY (station_id) INTO 2 BUCKETS
STORED AS PARQUET
LOCATION 'project/warehouse/records_optimized'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=5000;
SET hive.tez.container.size=4096;
SET parquet.memory.min.allocation.size=2097152;
SET hive.tez.auto.reducer.parallelism=true;
SET parquet.block.size=134217728;

-- Insert data from staging to optimized table
INSERT OVERWRITE TABLE records PARTITION (month, day)
SELECT
    record_id, station_id, airnow_ozone, cmaq_ozone, cmaq_no2, cmaq_co, cmaq_oc,
    pressure, pbl, temperature, wind_speed, wind_direction, radiation, cloud_fraction,
    hour, month, day
FROM records_staging;

-- Check tables
SHOW TABLES;
DESCRIBE stations;
DESCRIBE records;
SELECT * FROM stations LIMIT 10;
SELECT * FROM records LIMIT 10;