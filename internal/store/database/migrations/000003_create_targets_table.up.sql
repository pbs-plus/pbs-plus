CREATE TABLE IF NOT EXISTS targets (
  name TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  auth TEXT,
  token_used TEXT,
  drive_type TEXT,
  drive_name TEXT,
  drive_fs TEXT,
  drive_total_bytes INTEGER,
  drive_used_bytes INTEGER,
  drive_free_bytes INTEGER,
  drive_total TEXT,
  drive_used TEXT,
  drive_free TEXT
);
