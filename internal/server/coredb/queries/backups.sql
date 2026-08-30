-- name: CreateBackup :exec
INSERT INTO backups (
    id, store, mode, source_mode, read_mode, target, subpath, schedule, comment,
    notification_mode, namespace, current_pid, last_run_upid, last_successful_upid,
    retry, retry_interval, max_dir_entries, pre_script, post_script,
    include_xattr, legacy_xattr, last_run_status, retry_count,
    last_run_state, last_run_starttime, last_run_endtime, last_successful_endtime, duration
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetBackup :one
SELECT
    j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath,
    j.schedule, j.comment, j.notification_mode, j.namespace, j.current_pid,
    j.last_run_upid, j.last_successful_upid, j.retry, j.retry_interval,
    j.max_dir_entries, j.pre_script, j.post_script, j.include_xattr, j.legacy_xattr,
    j.last_run_status, j.retry_count,
    j.last_run_state, j.last_run_starttime, j.last_run_endtime,
    j.last_successful_endtime, j.duration,
    COALESCE(dbo.scope, '') AS database_scope,
    COALESCE(dbo.database_name, '') AS database_name,
    COALESCE(dbo.client_family, '') AS database_client_family,
    COALESCE(dbo.client_dir, '') AS database_client_dir,
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    COALESCE(p.host, m.host, '') AS database_host,
    COALESCE(p.port, m.port, 0) AS database_port,
    COALESCE(p.username, m.username, '') AS database_username,
    COALESCE(p.ssl_mode, m.tls_mode, '') AS database_tls_mode,
    COALESCE(p.ca_certificate, m.ca_certificate, '') AS database_ca_certificate,
    COALESCE(p.default_client_dir, m.default_client_dir, '') AS database_default_client_dir,
    COALESCE(m.variant, '') AS database_variant,
    COALESCE(m.default_client_family, '') AS database_default_client_family,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth,
    ah.token_used as agent_token_used, ah.os as agent_os
FROM backups j
LEFT JOIN backup_database_options dbo ON dbo.backup_id = j.id
LEFT JOIN targets t ON j.target = t.name
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN target_postgresql p ON p.target_name = t.name
LEFT JOIN target_mysql m ON m.target_name = t.name
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
WHERE j.id = ?
LIMIT 1;

-- name: ListAllBackups :many
SELECT
    j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath,
    j.schedule, j.comment, j.notification_mode, j.namespace, j.current_pid,
    j.last_run_upid, j.last_successful_upid, j.retry, j.retry_interval,
    j.max_dir_entries, j.pre_script, j.post_script, j.include_xattr, j.legacy_xattr,
    j.last_run_status, j.retry_count,
    j.last_run_state, j.last_run_starttime, j.last_run_endtime,
    j.last_successful_endtime, j.duration,
    COALESCE(dbo.scope, '') AS database_scope,
    COALESCE(dbo.database_name, '') AS database_name,
    COALESCE(dbo.client_family, '') AS database_client_family,
    COALESCE(dbo.client_dir, '') AS database_client_dir,
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    COALESCE(p.host, m.host, '') AS database_host,
    COALESCE(p.port, m.port, 0) AS database_port,
    COALESCE(p.username, m.username, '') AS database_username,
    COALESCE(p.ssl_mode, m.tls_mode, '') AS database_tls_mode,
    COALESCE(p.ca_certificate, m.ca_certificate, '') AS database_ca_certificate,
    COALESCE(p.default_client_dir, m.default_client_dir, '') AS database_default_client_dir,
    COALESCE(m.variant, '') AS database_variant,
    COALESCE(m.default_client_family, '') AS database_default_client_family,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth,
    ah.token_used as agent_token_used, ah.os as agent_os
FROM backups j
LEFT JOIN backup_database_options dbo ON dbo.backup_id = j.id
LEFT JOIN targets t ON j.target = t.name
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN target_postgresql p ON p.target_name = t.name
LEFT JOIN target_mysql m ON m.target_name = t.name
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
ORDER BY j.id;

-- name: UpdateBackup :exec
UPDATE backups
SET store = ?, mode = ?, source_mode = ?, read_mode = ?, target = ?,
    subpath = ?, schedule = ?, comment = ?, notification_mode = ?,
    namespace = ?, current_pid = ?, last_run_upid = ?, retry = ?,
    retry_interval = ?, last_successful_upid = ?, pre_script = ?,
    post_script = ?, max_dir_entries = ?, include_xattr = ?, legacy_xattr = ?,
    last_run_status = ?, retry_count = ?,
    last_run_state = ?, last_run_starttime = ?, last_run_endtime = ?,
    last_successful_endtime = ?, duration = ?
WHERE id = ?;

-- name: UpsertBackupDatabaseOptions :exec
INSERT INTO backup_database_options (
    backup_id, scope, database_name, client_family, client_dir
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(backup_id) DO UPDATE SET
    scope = excluded.scope,
    database_name = excluded.database_name,
    client_family = excluded.client_family,
    client_dir = excluded.client_dir;

-- name: DeleteBackupDatabaseOptions :exec
DELETE FROM backup_database_options WHERE backup_id = ?;

-- name: DeleteBackup :execrows
DELETE FROM backups WHERE id = ?;

-- name: BackupExists :one
SELECT 1 FROM backups WHERE id = ? LIMIT 1;

-- name: CountBackups :one
SELECT COUNT(*) FROM backups;
