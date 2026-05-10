-- Remove typed status and retry count columns from backups table
ALTER TABLE backups DROP COLUMN last_run_status;
ALTER TABLE backups DROP COLUMN retry_count;