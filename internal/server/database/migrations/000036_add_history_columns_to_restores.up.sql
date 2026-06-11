-- Store history fields in DB so list queries don't need per-job filesystem lookups
ALTER TABLE restores ADD COLUMN last_run_state TEXT DEFAULT '';
ALTER TABLE restores ADD COLUMN last_run_starttime INTEGER DEFAULT 0;
ALTER TABLE restores ADD COLUMN last_run_endtime INTEGER DEFAULT 0;
ALTER TABLE restores ADD COLUMN last_successful_endtime INTEGER DEFAULT 0;
ALTER TABLE restores ADD COLUMN duration INTEGER DEFAULT 0;
