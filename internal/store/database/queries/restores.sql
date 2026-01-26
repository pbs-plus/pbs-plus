-- name: CreateRestore :exec
INSERT INTO restores (
    id, store, namespace, snapshot, src_path, dest_target, dest_subpath, 
    comment, current_pid, last_run_upid, last_successful_upid, retry,
    retry_interval, pre_script, post_script, restore_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetRestore :one
SELECT
    j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target, 
    j.dest_subpath, j.comment, j.current_pid, j.last_run_upid, 
    j.last_successful_upid, j.retry, j.retry_interval, j.pre_script, j.post_script,
    j.restore_mode,
    t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name,
    t.volume_fs, t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
    t.volume_total, t.volume_used, t.volume_free, t.mount_script,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth, 
    ah.token_used as agent_token_used, ah.os as agent_os
FROM restores j
LEFT JOIN targets t ON j.dest_target = t.name
LEFT JOIN agent_hosts ah ON t.agent_host = ah.name
WHERE j.id = ?
LIMIT 1;

-- name: ListAllRestores :many
SELECT
    j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target, 
    j.dest_subpath, j.comment, j.current_pid, j.last_run_upid, 
    j.last_successful_upid, j.retry, j.retry_interval, j.pre_script, j.post_script,
    j.restore_mode,
    t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name,
    t.volume_fs, t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
    t.volume_total, t.volume_used, t.volume_free, t.mount_script,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth, 
    ah.token_used as agent_token_used, ah.os as agent_os
FROM restores j
LEFT JOIN targets t ON j.dest_target = t.name
LEFT JOIN agent_hosts ah ON t.agent_host = ah.name
ORDER BY j.id;

-- name: ListQueuedRestores :many
SELECT
    j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target, 
    j.dest_subpath, j.comment, j.current_pid, j.last_run_upid, 
    j.last_successful_upid, j.retry, j.retry_interval, j.pre_script, j.post_script,
    j.restore_mode
FROM restores j
LEFT JOIN targets t ON j.dest_target = t.name
WHERE j.last_run_upid LIKE '%pbsplusgen-queue%'
ORDER BY j.id;

-- name: UpdateRestore :exec
UPDATE restores 
SET store = ?, namespace = ?, snapshot = ?, src_path = ?, dest_target = ?, 
    dest_subpath = ?, comment = ?, current_pid = ?, last_run_upid = ?, 
    retry = ?, retry_interval = ?, last_successful_upid = ?, 
    pre_script = ?, post_script = ?, restore_mode = ?
WHERE id = ?;

-- name: DeleteRestore :execrows
DELETE FROM restores WHERE id = ?;

-- name: RestoreExists :one
SELECT 1 FROM restores WHERE id = ? LIMIT 1;
