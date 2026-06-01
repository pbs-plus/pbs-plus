-- Create notification_batches table for grouping job notifications.
-- Instead of one notification per job, a batch collects all jobs in the group
-- and sends a single consolidated notification when all jobs have completed
-- (or after a timeout).
CREATE TABLE IF NOT EXISTS notification_batches (
    name TEXT PRIMARY KEY,
    comment TEXT DEFAULT '',
    notification_mode TEXT DEFAULT 'notification-system',
    -- How long to wait after the first job completes before flushing.
    -- If 0, flush immediately when all jobs in the batch are done.
    wait_timeout_secs INTEGER DEFAULT 300,
    -- Whether to send a notification even if some jobs are still running
    -- when the timeout fires.
    send_on_timeout INTEGER DEFAULT 1,
    created_at INTEGER DEFAULT (strftime('%s', 'now'))
);

-- Join table: which jobs belong to which batch.
-- A job can belong to at most one batch (or none, meaning immediate notification).
-- job_type discriminates between backups, restores, and verification jobs.
CREATE TABLE IF NOT EXISTS notification_batch_jobs (
    batch_name TEXT NOT NULL REFERENCES notification_batches(name) ON DELETE CASCADE,
    job_type TEXT NOT NULL CHECK (job_type IN ('backup', 'restore', 'verification')),
    job_id TEXT NOT NULL,
    PRIMARY KEY (batch_name, job_type, job_id)
);
