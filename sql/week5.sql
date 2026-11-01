CREATE SCHEMA IF NOT EXISTS week5;
CREATE TABLE IF NOT EXISTS week5.daily_stage (
    load_id TEXT NOT NULL, stage_partition INTEGER,
    target_date DATE NOT NULL, wiki TEXT NOT NULL,
    edit_count BIGINT NOT NULL, bot_edit_count BIGINT NOT NULL,
    size_known_count BIGINT NOT NULL, net_bytes_delta BIGINT
);
CREATE INDEX IF NOT EXISTS daily_stage_load ON week5.daily_stage(load_id);
CREATE TABLE IF NOT EXISTS week5.daily_wiki (
    target_date DATE NOT NULL, wiki TEXT NOT NULL,
    edit_count BIGINT NOT NULL, bot_edit_count BIGINT NOT NULL,
    size_known_count BIGINT NOT NULL, net_bytes_delta BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (target_date,wiki)
);
CREATE TABLE IF NOT EXISTS week5.publications (
    target_date DATE PRIMARY KEY,
    fingerprint TEXT NOT NULL, manifest JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
