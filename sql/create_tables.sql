START TRANSACTION;

DROP TABLE IF EXISTS stations CASCADE;
DROP TABLE IF EXISTS records CASCADE;


CREATE TABLE IF NOT EXISTS stations
(
    id        VARCHAR(12) PRIMARY KEY,
    latitude  NUMERIC(7, 4),
    longitude NUMERIC(7, 4)
);

CREATE TABLE IF NOT EXISTS records
(
    record_id           SERIAL PRIMARY KEY,
    station_id          VARCHAR(12) CONSTRAINT station_id_null NOT NULL,

    airnow_ozone        NUMERIC(4,1),
    cmaq_ozone          NUMERIC(4,1),
    cmaq_no2            NUMERIC(4,1),
    cmaq_co             NUMERIC(6,1),
    cmaq_oc             NUMERIC(5,1),
    pressure            NUMERIC(7,1),
    pbl                 NUMERIC(5,1),
    temperature         NUMERIC(4,1) CONSTRAINT temp_check CHECK (temperature > 0),
    wind_speed          NUMERIC(4,1),
    wind_direction      NUMERIC(4,1) CONSTRAINT dir_check CHECK (wind_direction >= 0 and wind_direction <= 360),
    radiation           NUMERIC(5,1),
    cloud_fraction      NUMERIC(2,1), -- Ranges from 0 to 1

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