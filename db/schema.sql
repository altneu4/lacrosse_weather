-- Schema for storing La Crosse View weather station data in PostgreSQL.
-- Idempotent: safe to run on every collector startup.

CREATE TABLE IF NOT EXISTS locations (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    id          TEXT PRIMARY KEY,               -- La Crosse device_id
    location_id TEXT NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    sensor_id   TEXT,
    sensor_type TEXT
);

CREATE TABLE IF NOT EXISTS readings (
    id        BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    field     TEXT NOT NULL,                    -- e.g. "Temperature", "Humidity"
    raw_value TEXT,                             -- original value as reported by the API
    value     DOUBLE PRECISION,                 -- parsed numeric value, NULL if not numeric
    unit      TEXT,
    ts        TIMESTAMPTZ NOT NULL,
    UNIQUE (device_id, field, ts)                -- keeps re-collection idempotent
);

CREATE INDEX IF NOT EXISTS idx_readings_device_ts ON readings (device_id, ts DESC);

-- Keeps global MIN(ts)/MAX(ts) (used for forward polling and backfill
-- progress) cheap regardless of table size.
CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings (ts);
