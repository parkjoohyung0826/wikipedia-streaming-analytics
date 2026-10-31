-- Separate from the existing canonical events and hourly view.
CREATE TABLE IF NOT EXISTS week4.stream_pipelines (
    pipeline_id TEXT PRIMARY KEY,
    contract JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS week4.stream_batches (
    pipeline_id TEXT NOT NULL REFERENCES week4.stream_pipelines,
    batch_id BIGINT NOT NULL,
    PRIMARY KEY (pipeline_id, batch_id)
);
CREATE TABLE IF NOT EXISTS week4.windowed_wiki (
    pipeline_id TEXT NOT NULL REFERENCES week4.stream_pipelines,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    wiki TEXT NOT NULL,
    edit_count BIGINT NOT NULL,
    bot_edit_count BIGINT NOT NULL,
    size_known_count BIGINT NOT NULL,
    net_bytes_delta BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, window_start, wiki)
);
