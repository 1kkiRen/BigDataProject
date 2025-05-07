USE team29_projectdb;

DROP TABLE IF EXISTS q8_results;

CREATE EXTERNAL TABLE q8_results(
    radiation_level STRING,
    count BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 'project/hive/warehouse/q8';
SET hive.resultset.use.unique.column.names=false;

INSERT INTO q8_results
SELECT
    CASE
        WHEN r.radiation / 2 <= 100 THEN 'low'
        WHEN r.radiation / 2  > 100 AND r.radiation / 2 <= 500 THEN 'normal'
        ELSE 'high'
    END AS radiation_level,
    COUNT(*) AS count
FROM records r
GROUP BY
    CASE
        WHEN r.radiation / 2 <= 100 THEN 'low'
        WHEN r.radiation / 2 > 100 AND r.radiation / 2 <= 500 THEN 'normal'
        ELSE 'high'
    END
ORDER BY
    CASE
        WHEN radiation_level = 'low' THEN 1
        WHEN radiation_level = 'normal' THEN 2
        ELSE 3
    END;

SELECT * FROM q8_results LIMIT 10;

INSERT OVERWRITE DIRECTORY 'project/output/q8'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q8_results;