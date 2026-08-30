-- name: CreateRestore :exec
INSERT INTO restores (
    id, store, namespace, snapshot, src_path, dest_target, dest_subpath,
    comment, current_pid, last_run_upid, last_successful_upid, retry,
    retry_interval, pre_script, post_script, restore_mode, last_run_status, retry_count,
    notification_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetRestore :one
SELECT
    j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target,
    j.dest_subpath, j.comment, j.current_pid, j.last_run_upid,
    j.last_successful_upid, j.retry, j.retry_interval, j.pre_script, j.post_script,
    j.restore_mode, j.last_run_status, j.retry_count, j.notification_mode,
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth,
    ah.token_used as agent_token_used, ah.os as agent_os
FROM restores j
LEFT JOIN targets t ON j.dest_target = t.name
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
WHERE j.id = ?
LIMIT 1;

-- name: ListAllRestores :many
SELECT
    j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target,
    j.dest_subpath, j.comment, j.current_pid, j.last_run_upid,
    j.last_successful_upid, j.retry, j.retry_interval, j.pre_script, j.post_script,
    j.restore_mode, j.last_run_status, j.retry_count, j.notification_mode,
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth,
    ah.token_used as agent_token_used, ah.os as agent_os
FROM restores j
LEFT JOIN targets t ON j.dest_target = t.name
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
ORDER BY j.id;

-- name: UpdateRestore :exec
UPDATE restores
SET store = ?, namespace = ?, snapshot = ?, src_path = ?, dest_target = ?,
    dest_subpath = ?, comment = ?, current_pid = ?, last_run_upid = ?,
    retry = ?, retry_interval = ?, last_successful_upid = ?,
    pre_script = ?, post_script = ?, restore_mode = ?, last_run_status = ?, retry_count = ?,
    notification_mode = ?
WHERE id = ?;

-- name: DeleteRestore :execrows
DELETE FROM restores WHERE id = ?;

-- name: RestoreExists :one
SELECT 1 FROM restores WHERE id = ? LIMIT 1;

-- name: CountRestores :one
SELECT COUNT(*) FROM restores;
