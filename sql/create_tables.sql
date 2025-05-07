START TRANSACTION;

DROP TABLE IF EXISTS stations CASCADE;
DROP TABLE IF EXISTS records CASCADE;


CREATE TABLE IF NOT EXISTS stations
(
    id        VARCHAR(12) PRIMARY KEY,
    latitude  FLOAT,
    longitude FLOAT
);

CREATE TABLE IF NOT EXISTS records
(
    record_id           SERIAL PRIMARY KEY,
    station_id          VARCHAR(12) CONSTRAINT station_id_null NOT NULL,

    airnow_ozone        FLOAT CONSTRAINT pos_airnow_ozone CHECK (airnow_ozone >= 0),
    cmaq_ozone          FLOAT CONSTRAINT pos_cmaq_ozone CHECK (cmaq_ozone >= 0),
    cmaq_no2            FLOAT CONSTRAINT positive_no2 CHECK (cmaq_no2 >= 0),
    cmaq_co             FLOAT CONSTRAINT positive_co CHECK (cmaq_co >= 0),
    cmaq_oc             FLOAT CONSTRAINT positive_oc CHECK (cmaq_oc >= 0),
    pressure            FLOAT CONSTRAINT positive_pressure CHECK (pressure > 0),
    pbl                 FLOAT CONSTRAINT positive_pbl CHECK (pbl >= 0),
    temperature         FLOAT CONSTRAINT temp_check CHECK (temperature > 0),
    wind_speed          FLOAT CONSTRAINT positive_speed CHECK (wind_speed >= 0),
    wind_direction      FLOAT CONSTRAINT dir_check CHECK (wind_direction BETWEEN 0 AND 360),
    radiation           FLOAT CONSTRAINT positive_radiation CHECK (radiation >= 0),
    cloud_fraction      FLOAT CONSTRAINT positive_cloud_fraction CHECK (cloud_fraction BETWEEN 0 AND 1),

    month               INTEGER CONSTRAINT month_null NOT NULL CHECK (month BETWEEN 0 AND 12),
    day                 INTEGER CONSTRAINT day_null NOT NULL,
    hour                INTEGER CONSTRAINT hours_null NOT NULL CHECK (hour BETWEEN 0 AND 23),

    FOREIGN KEY (station_id) REFERENCES stations (id),

    CONSTRAINT valid_day_month CHECK (
        (month IN (1, 3, 5, 7, 8, 10, 12) AND day <= 31) OR
        (month IN (4, 6, 9, 11) AND day <= 30) OR
        (month = 2 AND day <= 29)
        ),

    CONSTRAINT unique_record UNIQUE (station_id, month, day, hour)
);
COMMIT;