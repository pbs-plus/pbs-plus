ALTER TABLE mtf_jobs ADD COLUMN keep_loaded INTEGER NOT NULL DEFAULT 1;
UPDATE mtf_jobs SET keep_loaded = 1;
