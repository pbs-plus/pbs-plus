ALTER TABLE targets RENAME TO targets_base;

CREATE TABLE targets (
  name TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  agent_host TEXT,
  volume_id TEXT,
  volume_type TEXT,
  volume_name TEXT,
  volume_fs TEXT,
  volume_total_bytes INTEGER,
  volume_used_bytes INTEGER,
  volume_free_bytes INTEGER,
  volume_total TEXT,
  volume_used TEXT,
  volume_free TEXT,
  mount_script TEXT NOT NULL DEFAULT '',
  secret_s3 TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (agent_host) REFERENCES agent_hosts(name) ON DELETE CASCADE
);

INSERT INTO targets (
  name, path, agent_host, volume_id, volume_type, volume_name, volume_fs,
  volume_total_bytes, volume_used_bytes, volume_free_bytes,
  volume_total, volume_used, volume_free, mount_script, secret_s3
)
SELECT
  t.name,
  COALESCE(f.path, s.url, ''),
  f.agent_host,
  f.volume_id,
  f.volume_type,
  f.volume_name,
  f.volume_fs,
  f.volume_total_bytes,
  f.volume_used_bytes,
  f.volume_free_bytes,
  f.volume_total,
  f.volume_used,
  f.volume_free,
  t.mount_script,
  COALESCE(s.secret, '')
FROM targets_base t
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name;

DROP TABLE target_filesystems;
DROP TABLE target_s3;
DROP TABLE targets_base;
