ALTER TABLE targets RENAME TO targets_legacy;

CREATE TABLE targets (
  name TEXT PRIMARY KEY,
  target_type TEXT NOT NULL,
  mount_script TEXT NOT NULL DEFAULT ''
);

INSERT INTO targets (name, target_type, mount_script)
SELECT
  name,
  CASE
    WHEN agent_host IS NOT NULL THEN 'filesystem'
    WHEN LOWER(path) LIKE 'http://%' OR LOWER(path) LIKE 'https://%' THEN 's3'
    ELSE 'filesystem'
  END,
  mount_script
FROM targets_legacy;

CREATE TABLE target_filesystems (
  target_name TEXT PRIMARY KEY,
  access TEXT NOT NULL CHECK (access IN ('local', 'agent')),
  path TEXT NOT NULL DEFAULT '',
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
  FOREIGN KEY (target_name) REFERENCES targets(name) ON DELETE CASCADE,
  FOREIGN KEY (agent_host) REFERENCES agent_hosts(name) ON DELETE CASCADE,
  CHECK (
    (access = 'local' AND path <> '' AND agent_host IS NULL) OR
    (access = 'agent' AND agent_host IS NOT NULL)
  )
);

INSERT INTO target_filesystems (
  target_name, access, path, agent_host, volume_id, volume_type, volume_name,
  volume_fs, volume_total_bytes, volume_used_bytes, volume_free_bytes,
  volume_total, volume_used, volume_free
)
SELECT
  name,
  CASE WHEN agent_host IS NOT NULL THEN 'agent' ELSE 'local' END,
  path,
  agent_host,
  volume_id,
  volume_type,
  volume_name,
  volume_fs,
  volume_total_bytes,
  volume_used_bytes,
  volume_free_bytes,
  volume_total,
  volume_used,
  volume_free
FROM targets_legacy
WHERE agent_host IS NOT NULL
   OR (LOWER(path) NOT LIKE 'http://%' AND LOWER(path) NOT LIKE 'https://%');

CREATE TABLE target_s3 (
  target_name TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  secret TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (target_name) REFERENCES targets(name) ON DELETE CASCADE
);

INSERT INTO target_s3 (target_name, url, secret)
SELECT name, path, secret_s3
FROM targets_legacy
WHERE agent_host IS NULL
  AND (LOWER(path) LIKE 'http://%' OR LOWER(path) LIKE 'https://%');

DROP TABLE targets_legacy;
