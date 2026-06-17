CREATE TABLE IF NOT EXISTS mtf_changers (
    name        TEXT PRIMARY KEY,
    device      TEXT NOT NULL,
    comment     TEXT DEFAULT '',
    created_at  INTEGER DEFAULT (strftime('%s', 'now'))
);

CREATE TABLE IF NOT EXISTS mtf_drives (
    name        TEXT PRIMARY KEY,
    device      TEXT NOT NULL,
    changer     TEXT DEFAULT '',
    drive_index INTEGER DEFAULT 0,
    comment     TEXT DEFAULT '',
    created_at  INTEGER DEFAULT (strftime('%s', 'now')),
    FOREIGN KEY (changer) REFERENCES mtf_changers(name) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS mtf_cartridges (
    barcode         TEXT PRIMARY KEY,
    label           TEXT DEFAULT '',
    media_family_id INTEGER NOT NULL,
    sequence        INTEGER DEFAULT 1,
    role            TEXT DEFAULT 'unknown',
    catalog_type    INTEGER DEFAULT 0,
    is_bkf_file     INTEGER DEFAULT 0,
    source_path     TEXT DEFAULT '',

    volumes         INTEGER DEFAULT 0,
    directories     INTEGER DEFAULT 0,
    files           INTEGER DEFAULT 0,
    empty_files     INTEGER DEFAULT 0,
    file_bytes      INTEGER DEFAULT 0,
    sparse_files    INTEGER DEFAULT 0,
    compressed_files INTEGER DEFAULT 0,
    encrypted_files INTEGER DEFAULT 0,
    has_catalog     INTEGER DEFAULT 0,
    catalog_bytes   INTEGER DEFAULT 0,
    sets_closed     INTEGER DEFAULT 0,

    status          TEXT DEFAULT 'online',
    last_scanned    INTEGER DEFAULT 0,
    created_at      INTEGER DEFAULT (strftime('%s', 'now')),
    FOREIGN KEY (media_family_id) REFERENCES media_families(id)
);

CREATE TABLE IF NOT EXISTS media_families (
    id           INTEGER PRIMARY KEY,
    name         TEXT DEFAULT '',
    total_tapes  INTEGER DEFAULT 0,
    has_catalog  INTEGER DEFAULT 0,
    last_scanned INTEGER DEFAULT 0,
    created_at   INTEGER DEFAULT (strftime('%s', 'now'))
);

CREATE TABLE IF NOT EXISTS data_sets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    media_family_id INTEGER NOT NULL,
    set_number      INTEGER DEFAULT 0,
    name            TEXT DEFAULT '',
    description     TEXT DEFAULT '',
    owner           TEXT DEFAULT '',
    machine_name    TEXT DEFAULT '',
    write_time      INTEGER DEFAULT 0,
    num_directories INTEGER DEFAULT 0,
    num_files       INTEGER DEFAULT 0,
    num_corrupt     INTEGER DEFAULT 0,
    size            INTEGER DEFAULT 0,
    first_media_seq INTEGER DEFAULT 0,
    FOREIGN KEY (media_family_id) REFERENCES media_families(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS data_set_volumes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    data_set_id     INTEGER NOT NULL,
    device          TEXT DEFAULT '',
    volume_label    TEXT DEFAULT '',
    machine_name    TEXT DEFAULT '',
    mapped_namespace TEXT DEFAULT '',
    FOREIGN KEY (data_set_id) REFERENCES data_sets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dsv_set ON data_set_volumes(data_set_id);
CREATE INDEX IF NOT EXISTS idx_data_sets_family ON data_sets(media_family_id);
CREATE INDEX IF NOT EXISTS idx_cartridges_family ON mtf_cartridges(media_family_id);

CREATE TABLE IF NOT EXISTS namespace_mappings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT DEFAULT '',
    priority    INTEGER DEFAULT 0,
    match_regex TEXT DEFAULT '',
    template    TEXT NOT NULL DEFAULT '',
    is_default  INTEGER DEFAULT 0,
    enabled     INTEGER DEFAULT 1,
    comment     TEXT DEFAULT '',
    created_at  INTEGER DEFAULT (strftime('%s', 'now'))
);

CREATE TABLE IF NOT EXISTS mtf_jobs (
    id               TEXT PRIMARY KEY,
    source_kind      TEXT NOT NULL DEFAULT 'family',
    source_ref       TEXT NOT NULL,
    datastore        TEXT NOT NULL,
    namespace        TEXT DEFAULT '',
    schedule         TEXT DEFAULT '',
    comment          TEXT DEFAULT '',
    notification_mode TEXT DEFAULT '',
    spanning         INTEGER DEFAULT 1,
    overwrite_mappings INTEGER DEFAULT 0,
    changer          TEXT DEFAULT '',
    drive            TEXT DEFAULT '',

    current_pid         TEXT DEFAULT '',
    last_run_upid       TEXT DEFAULT '',
    last_successful_upid TEXT DEFAULT '',
    last_run_status     INTEGER DEFAULT 0,
    retry_count         INTEGER DEFAULT 0,
    retry               INTEGER DEFAULT 0,
    retry_interval      INTEGER DEFAULT 1,
    last_run_starttime  INTEGER DEFAULT 0,
    last_run_endtime    INTEGER DEFAULT 0,
    last_successful_endtime INTEGER DEFAULT 0,
    duration            INTEGER DEFAULT 0,

    created_at          INTEGER DEFAULT (strftime('%s', 'now'))
);

CREATE TABLE IF NOT EXISTS mtf_inventory_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    changer     TEXT DEFAULT '',
    started_at  INTEGER DEFAULT 0,
    completed_at INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'pending',
    cartridges  INTEGER DEFAULT 0,
    message     TEXT DEFAULT ''
);
