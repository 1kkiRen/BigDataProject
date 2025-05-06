-- sql/q6.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q6_results;
CREATE EXTERNAL TABLE q6_results(
    month INT,
    avg_temperature DECIMAL(4,1),
    avg_cmaq_ozone DECIMAL(4,1),
    avg_wind_speed DECIMAL(4,1) -- Added average wind speed
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q6';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q6_results
SELECT
    month,
    AVG(temperature) AS avg_temperature,
    AVG(cmaq_ozone) AS avg_cmaq_ozone,
    AVG(wind_speed) AS avg_wind_speed
FROM records
GROUP BY month;

SELECT * FROM q6_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q6'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q6_results;