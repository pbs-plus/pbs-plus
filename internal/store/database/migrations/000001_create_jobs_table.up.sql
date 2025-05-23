CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  store TEXT NOT NULL ,
  mode TEXT DEFAULT "metadata",
  source_mode TEXT DEFAULT "snapshot",
  target TEXT NOT NULL,
  subpath TEXT,
  schedule TEXT,
  comment TEXT,
  notification_mode TEXT,
  namespace TEXT,
  current_pid TEXT,
  last_run_upid TEXT,
  last_successful_upid TEXT,
  retry INTEGER,
  retry_interval INTEGER,
  raw_exclusions TEXT
);
