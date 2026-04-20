-- Add typed status and retry count columns to backups table
ALTER TABLE backups ADD COLUMN last_run_status INTEGER DEFAULT 0;
ALTER TABLE backups ADD COLUMN retry_count INTEGER DEFAULT 0;