-- sql/q5.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results(
    station_id STRING,
    avg_pbl DOUBLE,
    avg_radiation DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q5';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q5_results
SELECT
    station_id,
    AVG(pbl) AS avg_pbl,
    AVG(radiation) / 2 AS avg_radiation
FROM records
GROUP BY station_id;

SELECT * FROM q5_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q5'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q5_results;