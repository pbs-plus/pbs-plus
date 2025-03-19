CREATE TABLE IF NOT EXISTS tokens (
  token TEXT PRIMARY KEY,
  comment TEXT,
  created_at INTEGER,
  revoked BOOLEAN
);
