CREATE TABLE IF NOT EXISTS exclusions (
  section_id TEXT PRIMARY KEY,
  job_id TEXT,
  path TEXT NOT NULL,
  comment TEXT
);
