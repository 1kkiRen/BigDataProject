-- sql/q4.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results(
    month INT,
    avg_temperature FLOAT,
    avg_radiation FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q4';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q4_results
SELECT
    month,
    COALESCE(AVG(temperature), 0) - 274 AS avg_temperature,
    COALESCE(AVG(radiation), 0) / 2 AS avg_radiation
FROM records
GROUP BY month;

SELECT * FROM q4_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q4'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q4_results;