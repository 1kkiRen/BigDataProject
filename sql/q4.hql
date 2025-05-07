-- sql/q4.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results(
    hour INT,
    avg_temperature DECIMAL(4,1),
    avg_radiation DECIMAL(5,1)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q4';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q4_results
SELECT
    hour,
    AVG(temperature) AS avg_temperature,
    AVG(radiation) AS avg_radiation
FROM records
GROUP BY hour
ORDER BY hour;

SELECT * FROM q4_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q4'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q4_results;