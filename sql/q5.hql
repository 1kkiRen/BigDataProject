-- sql/q5.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results(
    station_id STRING,
    avg_pbl DECIMAL(5,1), -- Changed from avg_cmaq_co
    avg_radiation DECIMAL(5,1)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q5';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q5_results
SELECT
    station_id,
    AVG(pbl) AS avg_pbl, -- Changed from cmaq_co
    AVG(radiation) AS avg_radiation
FROM records
GROUP BY station_id
ORDER BY avg_pbl DESC; -- Order by the new primary insight

SELECT * FROM q5_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q5'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q5_results;