CREATE TABLE IF NOT EXISTS agent_hosts (
  name TEXT PRIMARY KEY,
  ip TEXT NOT NULL,
  auth TEXT,
  token_used TEXT,
  os TEXT NOT NULL DEFAULT ''
);

INSERT OR IGNORE INTO agent_hosts (name, ip, auth, token_used, os)
SELECT 
    TRIM(SUBSTR(name, 1, INSTR(name, ' - ') - 1)) as host_name,
    SUBSTR(path, 9, INSTR(SUBSTR(path, 9), '/') - 1) as host_ip,
    MAX(auth),
    MAX(token_used),
    MAX(os)
FROM targets
WHERE (auth IS NOT NULL OR token_used IS NOT NULL)
  AND name LIKE '% - %'
  AND path LIKE 'agent://%/%'
GROUP BY host_name;

CREATE TABLE targets_new (
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
  FOREIGN KEY (agent_host) REFERENCES agent_hosts(name)
    ON DELETE CASCADE
);

INSERT INTO targets_new 
SELECT 
    name,
    CASE 
        WHEN (auth IS NOT NULL OR token_used IS NOT NULL) AND name LIKE '% - %'
        THEN '' 
        ELSE path 
    END as path,
    CASE 
        WHEN (auth IS NOT NULL OR token_used IS NOT NULL) AND name LIKE '% - %'
        THEN TRIM(SUBSTR(name, 1, INSTR(name, ' - ') - 1)) 
        ELSE NULL 
    END as agent_host,
    CASE 
        WHEN (auth IS NOT NULL OR token_used IS NOT NULL) AND name LIKE '% - %'
        THEN TRIM(SUBSTR(name, INSTR(name, ' - ') + 3)) 
        ELSE NULL 
    END as volume_id,
    drive_type,
    drive_name,
    drive_fs,
    drive_total_bytes,
    drive_used_bytes,
    drive_free_bytes,
    drive_total,
    drive_used,
    drive_free,
    mount_script,
    secret_s3
FROM targets;

DROP TABLE targets;
ALTER TABLE targets_new RENAME TO targets;

ALTER TABLE restores RENAME COLUMN dest_path TO dest_subpath;
