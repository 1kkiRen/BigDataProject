COPY stations FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';

COPY records
    (station_id, airnow_ozone, cmaq_ozone, cmaq_no2, cmaq_co,
     cmaq_oc, pressure, pbl, temperature, wind_speed,
     wind_direction, radiation, cloud_fraction, month, day, hour)
FROM STDIN WITH CSV HEADER
DELIMITER ','
NULL AS 'null';