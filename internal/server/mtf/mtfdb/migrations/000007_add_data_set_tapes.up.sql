CREATE TABLE IF NOT EXISTS data_set_tapes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    data_set_id INTEGER NOT NULL REFERENCES data_sets(id) ON DELETE CASCADE,
    media_seq   INTEGER NOT NULL,
    sset_pba    INTEGER NOT NULL DEFAULT 0,
    UNIQUE(data_set_id, media_seq)
);

CREATE INDEX IF NOT EXISTS idx_dst_set ON data_set_tapes(data_set_id);