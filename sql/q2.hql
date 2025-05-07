-- sql/q2.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q2_results;
CREATE EXTERNAL TABLE q2_results(
    id STRING,
    latitude DOUBLE,
    longitude DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q2';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q2_results
SELECT id, latitude, longitude
FROM stations;

SELECT * FROM q2_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q2_results;