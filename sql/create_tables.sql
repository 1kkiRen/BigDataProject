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

    airnow_ozone        FLOAT,
    cmaq_ozone          FLOAT,
    cmaq_no2            FLOAT,
    cmaq_co             FLOAT,
    cmaq_oc             FLOAT,
    pressure            FLOAT,
    pbl                 FLOAT,
    temperature         FLOAT CONSTRAINT temp_check CHECK (temperature > 0),
    wind_speed          FLOAT,
    wind_direction      FLOAT CONSTRAINT dir_check CHECK (wind_direction >= 0 and wind_direction <= 360),
    radiation           FLOAT,
    cloud_fraction      FLOAT,

    month               INTEGER CONSTRAINT month_null NOT NULL CHECK (month >= 1 AND month <= 12),
    day                 INTEGER CONSTRAINT day_null NOT NULL,
    hour                INTEGER CONSTRAINT hours_null NOT NULL CHECK (hour >= 0 AND hour <= 23),

    FOREIGN KEY (station_id) REFERENCES stations (id),

    CONSTRAINT valid_day_month CHECK (
        (month IN (1, 3, 5, 7, 8, 10, 12) AND day <= 31) OR
        (month IN (4, 6, 9, 11) AND day <= 30) OR
        (month = 2 AND day <= 29)
        ),

    CONSTRAINT unique_record UNIQUE (station_id, month, day, hour)
);
COMMIT;