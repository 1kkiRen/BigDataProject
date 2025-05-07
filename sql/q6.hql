-- sql/q6.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q6_results;
CREATE EXTERNAL TABLE q6_results(
    day INT,
    avg_pbl FLOAT,
    avg_cmaq_ozone FLOAT,
    avg_radiation FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q6';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q6_results
SELECT
    day,
    AVG(pbl) AS avg_pbl,
    AVG(cmaq_ozone) AS avg_cmaq_ozone,
    AVG(radiation) AS avg_radiation
FROM records
GROUP BY day;

SELECT * FROM q6_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q6'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q6_results;