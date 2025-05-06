-- Drop the database if it exists
DROP DATABASE IF EXISTS team29_projectdb CASCADE;

-- Create the database at the specified location
CREATE DATABASE team29_projectdb LOCATION 'project/hive/warehouse';

-- Use the new database
USE team29_projectdb;

-- Drop tables if they exist
DROP TABLE IF EXISTS stations;
DROP TABLE IF EXISTS records;
DROP TABLE IF EXISTS records_staging;

-- Create stations table (no partitioning needed)
CREATE EXTERNAL TABLE stations (
    id STRING,
    latitude DECIMAL(7,4),
    longitude DECIMAL(7,4)
)
STORED AS PARQUET
LOCATION 'project/warehouse/stations';

-- Create a staging table for records (no partitioning)
CREATE EXTERNAL TABLE records_staging (
    record_id INT,
    station_id STRING,
    airnow_ozone DECIMAL(4,1),
    cmaq_ozone DECIMAL(4,1),
    cmaq_no2 DECIMAL(4,1),
    cmaq_co DECIMAL(6,1),
    cmaq_oc DECIMAL(5,1),
    pressure DECIMAL(7,1),
    pbl DECIMAL(5,1),
    temperature DECIMAL(4,1),
    wind_speed DECIMAL(4,1),
    wind_direction DECIMAL(4,1),
    radiation DECIMAL(5,1),
    cloud_fraction DECIMAL(2,1),
    month INT,
    day INT,
    hour INT
)
STORED AS PARQUET
LOCATION 'project/warehouse/records';

-- Create the optimized records table with partitioning and bucketing
CREATE EXTERNAL TABLE records (
    record_id INT,
    station_id STRING,
    airnow_ozone DECIMAL(4,1),
    cmaq_ozone DECIMAL(4,1),
    cmaq_no2 DECIMAL(4,1),
    cmaq_co DECIMAL(6,1),
    cmaq_oc DECIMAL(5,1),
    pressure DECIMAL(7,1),
    pbl DECIMAL(5,1),
    temperature DECIMAL(4,1),
    wind_speed DECIMAL(4,1),
    wind_direction DECIMAL(4,1),
    radiation DECIMAL(5,1),
    cloud_fraction DECIMAL(2,1),
    hour INT
)
PARTITIONED BY (month INT, day INT)
CLUSTERED BY (station_id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION 'project/warehouse/records_optimized'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=500;
SET hive.exec.max.dynamic.partitions.pernode=500;
SET parquet.memory.min.chunk.size=524288;

-- Insert data from staging to optimized table
INSERT OVERWRITE TABLE records PARTITION (month, day)
SELECT
    record_id, station_id, airnow_ozone, cmaq_ozone, cmaq_no2, cmaq_co, cmaq_oc,
    pressure, pbl, temperature, wind_speed, wind_direction, radiation, cloud_fraction,
    month, day, hour
FROM records_staging;

DROP TABLE IF EXISTS records_staging;

-- Check tables
SHOW TABLES;
DESCRIBE stations;
DESCRIBE records;
SELECT * FROM stations LIMIT 5;
SELECT * FROM records LIMIT 5;