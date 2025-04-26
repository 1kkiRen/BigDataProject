START TRANSACTION;

DROP TABLE stations CASCADE;
DROP TABLE records CASCADE;


CREATE TABLE IF NOT EXISTS stations
(
    id        BIGINT PRIMARY KEY,
    latitude  NUMERIC(7, 4),
    longitude NUMERIC(7, 4)
);

CREATE TABLE IF NOT EXISTS records
(
    record_id      SERIAL PRIMARY KEY,
    station_id     BIGINT CONSTRAINT NOT NULL,
    split          BOOLEAN, -- Train (1) or Test(0)

    month          INTEGER CONSTRAINT NOT NULL CHECK(month >= 1 AND month <=12),
    day            INT CONSTRAINT NOT NULL,
    hour           INT CONSTRAINT NOT NULL CHECK (hour >= 0 AND hour <= 23),

    airnow_ozon    SMALLINT,
    cmaq_ozon      SMALLINT,
    cmaq_no2       SMALLINT,
    cmaq_co        SMALLINT,
    pressure       INTEGER,
    pbl            SMALLINT,
    temperature    SMALLINT CONSTRAINT temp_check CHECK (temperature > 0),
    wind_speed     SMALLINT,
    wind_direction SMALLINT CONSTRAINT dir_check CHECK (wind_direction >= 0 and wind_direction < 360),
    radiation      SMALLINT,
    cloud_fraction NUMERIC(3, 2), -- Ranges from 0 to 1

    FOREIGN KEY (station_id) REFERENCES stations (id),

    CONSTRAINT valid_day_month CHECK (
        (month IN (1, 3, 5, 7, 8, 10, 12) AND day <= 31) OR
        (month IN (4, 6, 9, 11) AND day <= 30) OR
        (month = 2 AND day <= 29)
        )
);

COMMIT;