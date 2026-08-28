-- Add namespace targeting columns to verification_jobs
ALTER TABLE verification_jobs ADD COLUMN target_mode TEXT DEFAULT 'backup_job';
ALTER TABLE verification_jobs ADD COLUMN recursive INTEGER DEFAULT 0;
