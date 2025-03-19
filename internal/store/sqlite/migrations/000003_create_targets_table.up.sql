CREATE TABLE IF NOT EXISTS targets (
  name TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  drive_used_bytes INTEGER,
  is_agent BOOLEAN,
  connection_status BOOLEAN
);
