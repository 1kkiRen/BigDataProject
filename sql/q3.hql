-- sql/q3.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(
    latitude_bucket INT,
    avg_radiation FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q3_results
SELECT 
  FLOOR(s.latitude / 5.0) * 5 AS latitude_bucket, 
  AVG(r.radiation) / 2 AS avg_radiation
FROM records r
JOIN stations s ON r.station_id = s.id
GROUP BY FLOOR(s.latitude / 5.0) * 5;


SELECT * FROM q3_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q3_results;