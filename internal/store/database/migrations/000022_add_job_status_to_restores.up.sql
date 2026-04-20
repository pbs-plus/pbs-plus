-- Add typed status and retry count columns to restores table
ALTER TABLE restores ADD COLUMN last_run_status INTEGER DEFAULT 0;
ALTER TABLE restores ADD COLUMN retry_count INTEGER DEFAULT 0;