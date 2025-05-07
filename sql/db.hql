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
    latitude DOUBLE,
    longitude DOUBLE
)
STORED AS PARQUET
LOCATION 'project/warehouse/stations';

-- Create a staging table for records (no partitioning)
CREATE EXTERNAL TABLE records_staging (
    record_id INT,
    station_id STRING,
    airnow_ozone DOUBLE,
    cmaq_ozone DOUBLE,
    cmaq_no2 DOUBLE,
    cmaq_co DOUBLE,
    cmaq_oc DOUBLE,
    pressure DOUBLE,
    pbl DOUBLE,
    temperature DOUBLE,
    wind_speed DOUBLE,
    wind_direction DOUBLE,
    radiation DOUBLE,
    cloud_fraction DOUBLE,
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
    airnow_ozone DOUBLE,
    cmaq_ozone DOUBLE,
    cmaq_no2 DOUBLE,
    cmaq_co DOUBLE,
    cmaq_oc DOUBLE,
    pressure DOUBLE,
    pbl DOUBLE,
    temperature DOUBLE,
    wind_speed DOUBLE,
    wind_direction DOUBLE,
    radiation DOUBLE,
    cloud_fraction DOUBLE,
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
    month, day, hour
FROM records_staging;

DROP TABLE IF EXISTS records_staging;

-- Check tables
SHOW TABLES;
DESCRIBE stations;
DESCRIBE records;
SELECT * FROM stations LIMIT 5;
SELECT * FROM records LIMIT 5;