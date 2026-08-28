ALTER TABLE restores RENAME COLUMN dest_subpath TO dest_path;

CREATE TABLE targets_rollback (
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
  drive_free TEXT,
  mount_script TEXT NOT NULL DEFAULT '', 
  os TEXT NOT NULL DEFAULT '', 
  secret_s3 TEXT NOT NULL DEFAULT ''
);

INSERT INTO targets_rollback
SELECT 
    t.name,
    CASE 
        WHEN t.agent_host IS NOT NULL 
        THEN ('agent://' || a.ip || '/' || t.volume_id) 
        ELSE t.path 
    END,
    a.auth,
    a.token_used,
    t.volume_type,
    t.volume_name,
    t.volume_fs,
    t.volume_total_bytes,
    t.volume_used_bytes,
    t.volume_free_bytes,
    t.volume_total,
    t.volume_used,
    t.volume_free,
    t.mount_script,
    COALESCE(a.os, ''),
    t.secret_s3
FROM targets t
LEFT JOIN agent_hosts a ON t.agent_host = a.name;

DROP TABLE targets;
ALTER TABLE targets_rollback RENAME TO targets;
