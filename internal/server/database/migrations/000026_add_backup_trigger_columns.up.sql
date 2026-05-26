-- Add run-on-backup-complete scheduling option
ALTER TABLE verification_jobs ADD COLUMN run_on_backup_complete INTEGER DEFAULT 0;
ALTER TABLE verification_jobs ADD COLUMN pending_since INTEGER DEFAULT 0;
