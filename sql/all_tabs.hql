USE team29_projectdb;

DROP TABLE IF EXISTS stations_results;
CREATE EXTERNAL TABLE stations_results(
    id STRING,
    latitude DECIMAL(7,4),
    longitude DECIMAL(7,4)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/station_results';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO stations_results
SELECT
    id,
    latitude,
    longitude
FROM stations
LIMIT 100;

SELECT * FROM stations_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/station_results'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM stations_results;


DROP TABLE IF EXISTS records_results;
CREATE EXTERNAL TABLE records_results(
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/records_results';

INTERT INTO records_results
SELECT
    record_id,
    station_id,
    airnow_ozone,
    cmaq_ozone,
    cmaq_no2,
    cmaq_co,
    cmaq_oc,
    pressure,
    pbl,
    temperature,
    wind_speed,
    wind_direction,
    radiation,
    cloud_fraction,
    month,
    day,
    hour
FROM records
LIMIT 100;


SELECT * FROM records_results LIMIT 10;

INSERT OVERWRITE DIRECTORY 'project/output/records_results'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM records_results;
