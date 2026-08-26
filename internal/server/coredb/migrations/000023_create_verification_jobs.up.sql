-- Create verification_jobs table for data verification feature
CREATE TABLE IF NOT EXISTS verification_jobs (
    id TEXT PRIMARY KEY,
    backup_job_id TEXT NOT NULL,
    store TEXT NOT NULL,
    namespace TEXT DEFAULT '',
    mode TEXT NOT NULL DEFAULT 'random_spot',
    schedule TEXT DEFAULT '',
    comment TEXT DEFAULT '',

    -- Random spot check config (JSON)
    spot_config TEXT DEFAULT '{}',

    -- History tracking
    last_run_upid TEXT DEFAULT '',
    last_successful_upid TEXT DEFAULT '',
    last_run_status INTEGER DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    retry INTEGER DEFAULT 0,
    retry_interval INTEGER DEFAULT 1,

    created_at INTEGER DEFAULT (strftime('%s', 'now'))
);

-- Create verification_results table
CREATE TABLE IF NOT EXISTS verification_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    verification_job_id TEXT NOT NULL,
    upid TEXT DEFAULT '',
    snapshot TEXT NOT NULL DEFAULT '',
    snapshot_time INTEGER NOT NULL DEFAULT 0,
    total_files INTEGER DEFAULT 0,
    verified_files INTEGER DEFAULT 0,
    failed_files INTEGER DEFAULT 0,
    skipped_files INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    started_at INTEGER DEFAULT 0,
    completed_at INTEGER DEFAULT 0,
    details TEXT DEFAULT '[]'
);
