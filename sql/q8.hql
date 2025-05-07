USE team29_projectdb;

DROP TABLE IF EXISTS q8_results;

CREATE EXTERNAL TABLE q8_results(
    latitude FLOAT,
    longitude FLOAT,
    corr_pbl_radiation FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 'project/hive/warehouse/q8';
SET hive.resultset.use.unique.column.names=false;

INSERT INTO q8_results
SELECT
    s.latitude,
    s.longitude,
    corr(pbl, radiation) AS corr_pbl_radiation
FROM records r
JOIN stations s ON r.station_id = s.id
GROUP BY s.latitude, s.longitude
ORDER BY ABS(corr_pbl_radiation) DESC;

SELECT * FROM q8_results LIMIT 10;

INSERT OVERWRITE DIRECTORY 'project/output/q8'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q8_results;