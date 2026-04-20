-- Add typed status and retry count columns to backups table
ALTER TABLE backups ADD COLUMN last_run_status INTEGER DEFAULT 0;
ALTER TABLE backups ADD COLUMN retry_count INTEGER DEFAULT 0;

-- Initialize existing data with best-effort status mapping
UPDATE backups SET last_run_status = CASE
    WHEN last_run_state = 'OK' THEN 1
    WHEN last_run_state LIKE 'WARNINGS:%' THEN 2
    WHEN last_run_state = 'operation canceled' THEN 4
    WHEN last_run_state IS NOT NULL AND last_run_state != '' THEN 3
    ELSE 0
END;