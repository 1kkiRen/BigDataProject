USE team29_projectdb;

DROP TABLE IF EXISTS q9_results;

CREATE EXTERNAL TABLE q9_results(
    latitude FLOAT,
    longitude FLOAT,
    corr_pbl_radiation FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 'project/hive/warehouse/q9';
SET hive.resultset.use.unique.column.names=false;

INSERT INTO q9_results
SELECT
    s.latitude,
    s.longitude,
    corr(cmaq_ozone, radiation) AS corr_cmaq_ozone_radiation
FROM records r
JOIN stations s ON r.station_id = s.id
GROUP BY s.latitude, s.longitude
ORDER BY ABS(corr_cmaq_ozone_radiation) DESC;

SELECT * FROM q9_results LIMIT 10;

INSERT OVERWRITE DIRECTORY 'project/output/q9'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q9_results;