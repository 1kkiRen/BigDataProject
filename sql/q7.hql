-- sql/q7.hql
USE team29_projectdb;

DROP TABLE IF EXISTS q7_results;
CREATE EXTERNAL TABLE q7_results(
    latitude FLOAT,
    longitude FLOAT,
    corr_temp_radiation DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
LOCATION 'project/hive/warehouse/q7';

SET hive.resultset.use.unique.column.names=false;

INSERT OVERWRITE TABLE q7_results
SELECT
    s.latitude,
    s.longitude,
    corr(temperature, radiation) AS corr_temp_radiation
FROM records r
JOIN stations s ON r.station_id = s.id
GROUP BY s.latitude, s.longitude
ORDER BY ABS(corr_temp_radiation) DESC;

SELECT * FROM q7_results LIMIT 10;

INSERT OVERWRITE DIRECTORY 'project/output/q7'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q7_results;