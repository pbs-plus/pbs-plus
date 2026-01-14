CREATE TABLE IF NOT EXISTS restores (
  id TEXT PRIMARY KEY,
  store TEXT NOT NULL ,
  namespace TEXT,
  snapshot TEXT NOT NULL,
  src_path TEXT NOT NULL,
  dest_target TEXT NOT NULL,
  dest_target_path TEXT,
  dest_path TEXT,
  comment TEXT,
  current_pid TEXT,
  last_run_upid TEXT,
  last_successful_upid TEXT,
  retry INTEGER,
  retry_interval INTEGER
);
