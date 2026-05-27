ALTER TABLE verification_results ADD COLUMN sampled_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE verification_results ADD COLUMN failed_bytes INTEGER NOT NULL DEFAULT 0;
