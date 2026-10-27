CREATE SCHEMA IF NOT EXISTS week4;
CREATE TABLE IF NOT EXISTS week4.edits (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL CHECK (event_type = 'wikipedia.edit'),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    wiki TEXT NOT NULL,
    title TEXT NOT NULL,
    bot BOOLEAN NOT NULL,
    old_length BIGINT,
    new_length BIGINT,
    bytes_delta BIGINT,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS week4.rejected (
    source_id TEXT PRIMARY KEY,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    raw_value TEXT,
    error_reason TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS edits_time_wiki ON week4.edits (event_time, wiki);
CREATE OR REPLACE VIEW week4.hourly_wiki AS
SELECT date_trunc('hour', event_time AT TIME ZONE 'UTC') AS hour_utc,
       wiki, count(*) AS edit_count,
       count(*) FILTER (WHERE bot) AS bot_edit_count,
       count(bytes_delta) AS size_known_count,
       sum(bytes_delta) AS net_bytes_delta
FROM week4.edits
GROUP BY 1, 2;
