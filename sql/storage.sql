-- Executor JDBC writes are retryable; staging deliberately allows duplicate rows.
CREATE TABLE IF NOT EXISTS week4.audit_stage (
    load_id TEXT NOT NULL,
    stage_partition INTEGER,
    event_id TEXT, event_type TEXT, schema_version INTEGER,
    event_time TIMESTAMPTZ, ingest_time TIMESTAMPTZ, source TEXT,
    wiki TEXT, title TEXT, bot BOOLEAN,
    old_length BIGINT, new_length BIGINT, bytes_delta BIGINT,
    source_topic TEXT, source_partition INTEGER, source_offset BIGINT,
    source_id TEXT, raw_value TEXT, error_reason TEXT,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_stage_load ON week4.audit_stage(load_id);
CREATE TABLE IF NOT EXISTS week4.aggregate_stage (
    load_id TEXT NOT NULL, stage_partition INTEGER,
    window_start TIMESTAMPTZ, window_end TIMESTAMPTZ, wiki TEXT,
    edit_count BIGINT, bot_edit_count BIGINT, size_known_count BIGINT, net_bytes_delta BIGINT,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS aggregate_stage_load ON week4.aggregate_stage(load_id);

-- One migration seed, then one revision bump per changed date per SQL statement.
-- Lock makes seed + trigger installation atomic against writers.
LOCK TABLE week4.edits IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE IF NOT EXISTS week4.storage_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS week4.export_dates (event_date DATE PRIMARY KEY, revision BIGINT NOT NULL);
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM week4.storage_meta WHERE key='date_tracking') THEN
        INSERT INTO week4.export_dates
        SELECT DISTINCT (event_time AT TIME ZONE 'UTC')::date, 1 FROM week4.edits
        ON CONFLICT DO NOTHING;
        INSERT INTO week4.storage_meta VALUES ('date_tracking', gen_random_uuid()::text);
    END IF;
END $$;
CREATE OR REPLACE FUNCTION week4.track_export_dates() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE input_sql TEXT;
BEGIN
    IF TG_OP='INSERT' THEN input_sql := 'SELECT event_time FROM new_rows';
    ELSIF TG_OP='DELETE' THEN input_sql := 'SELECT event_time FROM old_rows';
    ELSE input_sql := 'SELECT event_time FROM new_rows UNION ALL SELECT event_time FROM old_rows';
    END IF;
    EXECUTE 'INSERT INTO week4.export_dates(event_date,revision)
        SELECT DISTINCT (event_time AT TIME ZONE ''UTC'')::date,1 FROM (' || input_sql || ') changed
        ORDER BY 1 ON CONFLICT (event_date) DO UPDATE SET revision=week4.export_dates.revision+1';
    RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER track_edits_insert AFTER INSERT ON week4.edits
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION week4.track_export_dates();
CREATE OR REPLACE TRIGGER track_edits_update AFTER UPDATE ON week4.edits
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION week4.track_export_dates();
CREATE OR REPLACE TRIGGER track_edits_delete AFTER DELETE ON week4.edits
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION week4.track_export_dates();
