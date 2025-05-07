-- sql/q3.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(
    station_id STRING,
    avg_cmaq_ozone DOUBLE,
    avg_radiation DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q3_results
SELECT
    station_id,
    AVG(cmaq_ozone) AS avg_cmaq_ozone,
    AVG(radiation) AS avg_radiation
FROM records
GROUP BY station_id;

SELECT * FROM q3_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q3_results;