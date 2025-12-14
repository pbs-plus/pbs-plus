CREATE TABLE IF NOT EXISTS targets_new (
  name TEXT PRIMARY KEY,                         -- original targets.name
  target_type TEXT NOT NULL,                     -- 'agent' | 's3' | 'local'
  -- Agent specific
  agent_host TEXT,                               -- host or IP from agent://<host>/<volume>
  -- S3 specific
  s3_access_id TEXT,
  s3_host TEXT,
  s3_bucket TEXT,
  s3_region TEXT,
  s3_ssl INTEGER,
  s3_path_style INTEGER,
  s3_secret TEXT NOT NULL DEFAULT '',            -- from secret_s3
  -- Local specific
  local_path TEXT,                               -- for local targets
  -- Shared host-level properties
  auth TEXT,
  token_used TEXT,
  os TEXT NOT NULL DEFAULT '',
  mount_script TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS volumes_new (
  target_name TEXT NOT NULL,                     -- FK to targets_new.name
  volume_name TEXT NOT NULL,                     -- per-target unique name (e.g., 'C', 'Root', bucket, etc.)
  meta_type TEXT,
  meta_name TEXT,
  meta_fs TEXT,
  meta_total_bytes INTEGER,
  meta_used_bytes INTEGER,
  meta_free_bytes INTEGER,
  meta_total TEXT,
  meta_used TEXT,
  meta_free TEXT,
  PRIMARY KEY (target_name, volume_name),
  FOREIGN KEY (target_name) REFERENCES targets_new(name) ON DELETE CASCADE
);

INSERT INTO targets_new (
  name, target_type,
  agent_host,
  s3_access_id, s3_host, s3_bucket, s3_secret,
  local_path,
  auth, token_used, os, mount_script
)
SELECT
  t.name,
  CASE
    WHEN instr(t.path, 'agent://') = 1 THEN 'agent'
    WHEN instr(t.path, 'https://') = 1 AND instr(substr(t.path, 9), '@') > 0 THEN 's3'
    ELSE 'local'
  END AS target_type,
  -- agent_host
  CASE
    WHEN instr(t.path, 'agent://') = 1 THEN
      -- Extract between 'agent://' and next '/'
      substr(t.path, 9, CASE
        WHEN instr(substr(t.path, 9), '/') - 1 < 0 THEN length(t.path) - 8
        ELSE instr(substr(t.path, 9), '/') - 1
      END)
    ELSE NULL
  END AS agent_host,
  -- s3_access_id
  CASE
    WHEN instr(t.path, 'https://') = 1 AND instr(substr(t.path, 9), '@') > 0 THEN
      -- After https:// up to '@'
      substr(t.path, 9, instr(substr(t.path, 9), '@') - 1)
    ELSE NULL
  END AS s3_access_id,
  -- s3_host
  CASE
    WHEN instr(t.path, 'https://') = 1 AND instr(substr(t.path, 9), '@') > 0 THEN
      -- After '@' up to next '/'
      substr(
        substr(t.path, 9 + instr(substr(t.path, 9), '@')),
        1,
        CASE
          WHEN instr(substr(t.path, 9 + instr(substr(t.path, 9), '@')), '/') - 1 < 0
          THEN length(substr(t.path, 9 + instr(substr(t.path, 9), '@')))
          ELSE instr(substr(t.path, 9 + instr(substr(t.path, 9), '@')), '/') - 1
        END
      )
    ELSE NULL
  END AS s3_host,
  -- s3_bucket
  CASE
    WHEN instr(t.path, 'https://') = 1 AND instr(substr(t.path, 9), '@') > 0 THEN
      -- After the first '/' following '@'
      CASE
        WHEN instr(substr(t.path, 9 + instr(substr(t.path, 9), '@')), '/') > 0 THEN
          substr(
            substr(t.path, 9 + instr(substr(t.path, 9), '@')),
            instr(substr(t.path, 9 + instr(substr(t.path, 9), '@')), '/') + 1
          )
        ELSE NULL
      END
    ELSE NULL
  END AS s3_bucket,
  -- s3_secret
  COALESCE(t.secret_s3, '') AS s3_secret,
  -- local_path
  CASE
    WHEN instr(t.path, 'agent://') = 1 THEN NULL
    WHEN instr(t.path, 'https://') = 1 AND instr(substr(t.path, 9), '@') > 0 THEN NULL
    ELSE t.path
  END AS local_path,
  t.auth,
  t.token_used,
  t.os,
  t.mount_script
FROM targets t;

INSERT INTO volumes_new (
  target_name, volume_name,
  meta_type, meta_name, meta_fs,
  meta_total_bytes, meta_used_bytes, meta_free_bytes,
  meta_total, meta_used, meta_free
)
SELECT
  t.name AS target_name,
  CASE
    WHEN instr(t.path, 'agent://') = 1 THEN
      -- volume is after the first '/' following the host
      CASE
        WHEN instr(substr(t.path, 9), '/') > 0 THEN
          -- Get substring after first '/' in substr(t.path,9)
          substr(
            substr(t.path, 9),
            instr(substr(t.path, 9), '/') + 1
          )
        ELSE 'Root'
      END
    WHEN instr(t.path, 'https://') = 1 AND instr(substr(t.path, 9), '@') > 0 THEN
      -- s3 bucket (same logic as above for s3_bucket)
      CASE
        WHEN instr(substr(t.path, 9 + instr(substr(t.path, 9), '@')), '/') > 0 THEN
          substr(
            substr(t.path, 9 + instr(substr(t.path, 9), '@')),
            instr(substr(t.path, 9 + instr(substr(t.path, 9), '@')), '/') + 1
          )
        ELSE 'Root'
      END
    ELSE
      -- local: parse name pattern "<hostname> - <volume>"
      CASE
        WHEN instr(t.name, ' - ') > 0 THEN
          substr(t.name, instr(t.name, ' - ') + 3)
        ELSE t.name
      END
  END AS volume_name,
  t.drive_type AS meta_type,
  t.drive_name AS meta_name,
  t.drive_fs AS meta_fs,
  t.drive_total_bytes AS meta_total_bytes,
  t.drive_used_bytes AS meta_used_bytes,
  t.drive_free_bytes AS meta_free_bytes,
  t.drive_total AS meta_total,
  t.drive_used AS meta_used,
  t.drive_free AS meta_free
FROM targets t;

ALTER TABLE targets RENAME TO targets_backup_legacy;

ALTER TABLE targets_new RENAME TO targets;
ALTER TABLE volumes_new RENAME TO volumes;

CREATE INDEX IF NOT EXISTS idx_volumes_target ON volumes (target_name);
CREATE INDEX IF NOT EXISTS idx_targets_type ON targets (target_type);

DROP TABLE IF EXISTS targets_backup_legacy;

-- JOBS Migration

CREATE TABLE IF NOT EXISTS jobs_new (
  id TEXT PRIMARY KEY,
  store TEXT NOT NULL,
  mode TEXT DEFAULT 'metadata',
  source_mode TEXT DEFAULT 'snapshot',
  target TEXT NOT NULL,            -- references targets.name
  volume_name TEXT,                -- references volumes.volume_name within target
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
  max_dir_entries INTEGER DEFAULT 1048576,
  read_mode TEXT DEFAULT 'standard',
  pre_script TEXT NOT NULL DEFAULT '',
  post_script TEXT NOT NULL DEFAULT ''
);

-- Strategy:
--  - If legacy target name contains " - " and the left part matches a targets.agent_host,
--    split into agent_host and volume; else keep full legacy as target, null volume_name.
--  - Prefer exact match to targets.name; if exists, keep target=t.name and volume_name=NULL.
--  - If no exact name match but a (target_name, volume_name) can be inferred by
--    matching "X - Y" where targets.agent_host=X and volumes.volume_name=Y for that target,
--    use that target and volume_name.
-- Notes:
--  - This is best-effort and keeps data even if no mapping is possible.

WITH legacy AS (
  SELECT
    j.*,
    CASE
      WHEN instr(j.target, ' - ') > 0 THEN substr(j.target, 1, instr(j.target, ' - ') - 1)
      ELSE NULL
    END AS legacy_host,
    CASE
      WHEN instr(j.target, ' - ') > 0 THEN substr(j.target, instr(j.target, ' - ') + 3)
      ELSE NULL
    END AS legacy_volume
  FROM jobs j
),
exact_match AS (
  SELECT
    l.id AS job_id,
    t.name AS target_name_exact
  FROM legacy l
  JOIN targets t ON t.name = l.target
),
agent_match AS (
  SELECT
    l.id AS job_id,
    t.name AS target_name_agent,
    v.volume_name AS volume_name_agent
  FROM legacy l
  JOIN targets t ON t.target_type = 'agent' AND l.legacy_host IS NOT NULL AND t.agent_host = l.legacy_host
  JOIN volumes v ON v.target_name = t.name AND l.legacy_volume IS NOT NULL AND v.volume_name = l.legacy_volume
),
resolved AS (
  SELECT
    l.id,
    COALESCE(em.target_name_exact, am.target_name_agent) AS resolved_target,
    am.volume_name_agent AS resolved_volume
  FROM legacy l
  LEFT JOIN exact_match em ON em.job_id = l.id
  LEFT JOIN agent_match am ON am.job_id = l.id
)
INSERT INTO jobs_new (
  id, store, mode, source_mode, target, volume_name, subpath, schedule, comment,
  notification_mode, namespace, current_pid, last_run_upid, last_successful_upid,
  retry, retry_interval, max_dir_entries, read_mode, pre_script, post_script
)
SELECT
  l.id, l.store, l.mode, l.source_mode,
  COALESCE(r.resolved_target, l.target) AS target,
  r.resolved_volume AS volume_name,
  l.subpath, l.schedule, l.comment, l.notification_mode, l.namespace, l.current_pid,
  l.last_run_upid, l.last_successful_upid, l.retry, l.retry_interval,
  COALESCE(l.max_dir_entries, 1048576) AS max_dir_entries,
  COALESCE(l.read_mode, 'standard') AS read_mode,
  COALESCE(l.pre_script, '') AS pre_script,
  COALESCE(l.post_script, '') AS post_script
FROM legacy l
LEFT JOIN resolved r ON r.id = l.id;

ALTER TABLE jobs RENAME TO jobs_backup_legacy;
ALTER TABLE jobs_new RENAME TO jobs;

CREATE INDEX IF NOT EXISTS idx_jobs_target ON jobs (target);
CREATE INDEX IF NOT EXISTS idx_jobs_target_volume ON jobs (target, volume_name);
