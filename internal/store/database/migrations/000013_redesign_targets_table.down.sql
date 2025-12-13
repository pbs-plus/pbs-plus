CREATE TABLE IF NOT EXISTS targets_legacy_rebuild (
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
  drive_free TEXT
);
ALTER TABLE targets_legacy_rebuild ADD COLUMN mount_script TEXT NOT NULL DEFAULT '';
ALTER TABLE targets_legacy_rebuild ADD COLUMN os TEXT NOT NULL DEFAULT '';
ALTER TABLE targets_legacy_rebuild ADD COLUMN secret_s3 TEXT NOT NULL DEFAULT '';

-- Recompose rows by joining targets + volumes.
-- Path:
--  - agent: agent://{agent_host}/{volume_name}
--  - s3: https://{s3_access_id}@{s3_host}/{s3_bucket}  (volume_name equals bucket usually)
--  - local: use targets.local_path; if NULL, fallback to volumes.original_path
-- Name:
--  - agent: "<agent_host> - <volume_name>"
--  - s3: keep targets.name
--  - local: keep targets.name (legacy retained local names)
INSERT INTO targets_legacy_rebuild (
  name, path, auth, token_used,
  drive_type, drive_name, drive_fs,
  drive_total_bytes, drive_used_bytes, drive_free_bytes,
  drive_total, drive_used, drive_free,
  mount_script, os, secret_s3
)
SELECT
  CASE
    WHEN tgt.target_type = 'agent' THEN
      COALESCE(tgt.agent_host, tgt.name) || ' - ' || COALESCE(vol.volume_name, 'Root')
    ELSE
      tgt.name
  END AS name,
  CASE
    WHEN tgt.target_type = 'agent' THEN
      'agent://' || COALESCE(tgt.agent_host, 'unknown') || '/' || COALESCE(vol.volume_name, 'Root')
    WHEN tgt.target_type = 's3' THEN
      'https://' || COALESCE(tgt.s3_access_id, '') || '@' ||
      COALESCE(tgt.s3_host, 'unknown') || '/' ||
      COALESCE(tgt.s3_bucket, vol.volume_name)
    ELSE
      COALESCE(tgt.local_path, vol.original_path)
  END AS path,
  tgt.auth,
  tgt.token_used,
  vol.meta_type,
  vol.meta_name,
  vol.meta_fs,
  vol.meta_total_bytes,
  vol.meta_used_bytes,
  vol.meta_free_bytes,
  vol.meta_total,
  vol.meta_used,
  vol.meta_free,
  tgt.mount_script,
  tgt.os,
  COALESCE(tgt.s3_secret, '')
FROM targets tgt
JOIN volumes vol
  ON vol.target_name = tgt.name;

DROP TABLE IF EXISTS volumes;
DROP TABLE IF EXISTS targets;

ALTER TABLE targets_legacy_rebuild RENAME TO targets;

DROP TABLE IF EXISTS targets_backup_legacy;

-- JOBS Migration

CREATE TABLE IF NOT EXISTS jobs_legacy_rebuild (
  id TEXT PRIMARY KEY,
  store TEXT NOT NULL,
  mode TEXT DEFAULT 'metadata',
  source_mode TEXT DEFAULT 'snapshot',
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
  retry_interval INTEGER
);
ALTER TABLE jobs_legacy_rebuild ADD COLUMN raw_exclusions TEXT;
ALTER TABLE jobs_legacy_rebuild ADD COLUMN max_dir_entries INTEGER DEFAULT 1048576;
ALTER TABLE jobs_legacy_rebuild ADD COLUMN read_mode TEXT DEFAULT 'standard';
ALTER TABLE jobs_legacy_rebuild ADD COLUMN pre_script TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs_legacy_rebuild ADD COLUMN post_script TEXT NOT NULL DEFAULT '';

--  - If the job references an agent target and has a volume_name, emit "<agent_host> - <volume_name>"
--  - Else keep jobs.target as-is
INSERT INTO jobs_legacy_rebuild (
  id, store, mode, source_mode, target, subpath, schedule, comment,
  notification_mode, namespace, current_pid, last_run_upid, last_successful_upid,
  retry, retry_interval, raw_exclusions, max_dir_entries, read_mode, pre_script, post_script
)
SELECT
  j.id, j.store, j.mode, j.source_mode,
  CASE
    WHEN t.target_type = 'agent' AND j.volume_name IS NOT NULL AND j.volume_name != '' THEN
      COALESCE(t.agent_host, t.name) || ' - ' || j.volume_name
    ELSE
      j.target
  END AS target,
  j.subpath, j.schedule, j.comment, j.notification_mode, j.namespace, j.current_pid,
  j.last_run_upid, j.last_successful_upid, j.retry, j.retry_interval,
  NULL AS raw_exclusions,
  COALESCE(j.max_dir_entries, 1048576),
  COALESCE(j.read_mode, 'standard'),
  COALESCE(j.pre_script, ''),
  COALESCE(j.post_script, '')
FROM jobs j
LEFT JOIN targets t ON t.name = j.target;

DROP TABLE IF EXISTS jobs;
ALTER TABLE jobs_legacy_rebuild RENAME TO jobs;

DROP TABLE IF EXISTS jobs_backup_legacy;
