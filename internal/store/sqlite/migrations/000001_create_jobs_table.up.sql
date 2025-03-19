CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  store TEXT,
  mode TEXT,
  source_mode TEXT,
  target TEXT,
  subpath TEXT,
  schedule TEXT,
  comment TEXT,
  notification_mode TEXT,
  namespace TEXT,
  current_pid TEXT,
  last_run_upid TEXT,
  retry INTEGER,
  retry_interval INTEGER,
  raw_exclusions TEXT,
  last_run_endtime INTEGER,
  last_run_state TEXT,
  duration INTEGER,
  last_successful_upid TEXT,
  last_successful_endtime INTEGER,
  next_run INTEGER
);
