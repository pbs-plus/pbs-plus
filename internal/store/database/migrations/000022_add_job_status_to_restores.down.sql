-- Remove typed status and retry count columns from restores table
ALTER TABLE restores DROP COLUMN last_run_status;
ALTER TABLE restores DROP COLUMN retry_count;