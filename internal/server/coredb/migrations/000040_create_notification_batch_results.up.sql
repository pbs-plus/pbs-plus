CREATE TABLE IF NOT EXISTS notification_batch_results (
    batch_name TEXT NOT NULL REFERENCES notification_batches(name) ON DELETE CASCADE,
    job_type TEXT NOT NULL,
    job_id TEXT NOT NULL,
    datastore TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    severity TEXT NOT NULL DEFAULT 'info',
    recorded_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    PRIMARY KEY (batch_name, job_type, job_id)
);
