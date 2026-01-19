-- name: CreateBackup :exec
INSERT INTO backups (
    id, store, mode, source_mode, read_mode, target, subpath, schedule, comment,
    notification_mode, namespace, current_pid, last_run_upid, last_successful_upid, 
    retry, retry_interval, max_dir_entries, pre_script, post_script, 
    include_xattr, legacy_xattr
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetBackup :one
SELECT
    j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, 
    j.schedule, j.comment, j.notification_mode, j.namespace, j.current_pid, 
    j.last_run_upid, j.last_successful_upid, j.retry, j.retry_interval, 
    j.max_dir_entries, j.pre_script, j.post_script, j.include_xattr, j.legacy_xattr,
    t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name,
    t.volume_fs, t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
    t.volume_total, t.volume_used, t.volume_free, t.mount_script,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth, 
    ah.token_used as agent_token_used, ah.os as agent_os
FROM backups j
LEFT JOIN targets t ON j.target = t.name
LEFT JOIN agent_hosts ah ON t.agent_host = ah.name
WHERE j.id = ?
LIMIT 1;

-- name: ListAllBackups :many
SELECT
    j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, 
    j.schedule, j.comment, j.notification_mode, j.namespace, j.current_pid, 
    j.last_run_upid, j.last_successful_upid, j.retry, j.retry_interval, 
    j.max_dir_entries, j.pre_script, j.post_script, j.include_xattr, j.legacy_xattr,
    t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name,
    t.volume_fs, t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
    t.volume_total, t.volume_used, t.volume_free, t.mount_script,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth, 
    ah.token_used as agent_token_used, ah.os as agent_os
FROM backups j
LEFT JOIN targets t ON j.target = t.name
LEFT JOIN agent_hosts ah ON t.agent_host = ah.name
ORDER BY j.id;

-- name: ListQueuedBackups :many
SELECT
    j.id, j.store, j.mode, j.source_mode, j.read_mode, j.target, j.subpath, 
    j.schedule, j.comment, j.notification_mode, j.namespace, j.current_pid, 
    j.last_run_upid, j.last_successful_upid, j.retry, j.retry_interval, 
    j.max_dir_entries, j.pre_script, j.post_script, j.include_xattr, j.legacy_xattr,
    t.volume_used_bytes, t.mount_script
FROM backups j
LEFT JOIN targets t ON j.target = t.name
WHERE j.last_run_upid LIKE '%pbsplusgen-queue%'
ORDER BY j.id;

-- name: UpdateBackup :exec
UPDATE backups 
SET store = ?, mode = ?, source_mode = ?, read_mode = ?, target = ?,
    subpath = ?, schedule = ?, comment = ?, notification_mode = ?,
    namespace = ?, current_pid = ?, last_run_upid = ?, retry = ?,
    retry_interval = ?, last_successful_upid = ?, pre_script = ?, 
    post_script = ?, max_dir_entries = ?, include_xattr = ?, legacy_xattr = ?
WHERE id = ?;

-- name: DeleteBackup :execrows
DELETE FROM backups WHERE id = ?;

-- name: BackupExists :one
SELECT 1 FROM backups WHERE id = ? LIMIT 1;

