-- Add history tracking columns to verification_jobs
ALTER TABLE verification_jobs ADD COLUMN last_run_starttime INTEGER DEFAULT 0;
ALTER TABLE verification_jobs ADD COLUMN last_run_endtime INTEGER DEFAULT 0;
ALTER TABLE verification_jobs ADD COLUMN last_successful_endtime INTEGER DEFAULT 0;
