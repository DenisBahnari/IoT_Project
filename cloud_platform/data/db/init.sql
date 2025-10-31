CREATE TABLE IF NOT EXISTS ev_station (
    station_id TEXT PRIMARY KEY,
    distrito TEXT,
    concelho TEXT,
    freguesia TEXT,
    latitude NUMERIC(12,8),
    longitude NUMERIC(12,8),
    potencia_max_kw NUMERIC(8,2),
    num_pontos_ligacao INT,
    cod_distrito INT,
    cod_distrito_concelho INT,
    cod_distrito_concelho_freguesia INT
);

CREATE TABLE IF NOT EXISTS ev_session (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    vehicle_model TEXT,
    battery_capacity_kwh INTEGER,
    station_id TEXT REFERENCES ev_station(station_id),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    energy_consumed_kwh NUMERIC(12,8),
    duration_h NUMERIC(12,8),
    charging_rate_kw NUMERIC(12,8),
    charging_cost_eur NUMERIC(12,8),
    time_of_day TEXT,
    day_of_week TEXT,
    soc_start NUMERIC(12,8),
    soc_end NUMERIC(12,8),
    distance_driven_km NUMERIC(12,8),
    temperature_c NUMERIC(12,8),
    vehicle_age_years INTEGER
);

ALTER TABLE ev_session
ADD CONSTRAINT unique_session UNIQUE (user_id, station_id, start_time);


