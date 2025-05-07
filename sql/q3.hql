-- sql/q3.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(
    latitude DOUBLE,
    radiation DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q3_results
SELECT AVG(latitude) as latitude, AVG(radiation) as avg_radiation
FROM records
JOIN stations ON records.station_id = stations.id
GROUP BY stations.id
ORDER BY latitude;


SELECT * FROM q3_results LIMIT 10;

-- Export the results to HDFS directory as CSV
INSERT OVERWRITE DIRECTORY 'project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q3_results;