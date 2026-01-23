-- name: CreateTarget :exec
INSERT INTO targets (
    name, path, agent_host, volume_id, volume_type, volume_name, volume_fs,
    volume_total_bytes, volume_used_bytes, volume_free_bytes,
    volume_total, volume_used, volume_free, mount_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: UpdateTarget :exec
UPDATE targets SET
    path = ?, agent_host = ?, volume_id = ?, volume_type = ?,
    volume_name = ?, volume_fs = ?, volume_total_bytes = ?,
    volume_used_bytes = ?, volume_free_bytes = ?, volume_total = ?,
    volume_used = ?, volume_free = ?, mount_script = ?
WHERE name = ?;

-- name: UpsertTarget :exec
INSERT INTO targets (
    name, path, agent_host, volume_id, volume_type, volume_name, volume_fs,
    volume_total_bytes, volume_used_bytes, volume_free_bytes,
    volume_total, volume_used, volume_free, mount_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    path = excluded.path,
    agent_host = excluded.agent_host,
    volume_id = excluded.volume_id,
    volume_type = excluded.volume_type,
    volume_name = excluded.volume_name,
    volume_fs = excluded.volume_fs,
    volume_total_bytes = excluded.volume_total_bytes,
    volume_used_bytes = excluded.volume_used_bytes,
    volume_free_bytes = excluded.volume_free_bytes,
    volume_total = excluded.volume_total,
    volume_used = excluded.volume_used,
    volume_free = excluded.volume_free,
    mount_script = excluded.mount_script;

-- name: UpdateTargetS3Secret :exec
UPDATE targets SET secret_s3 = ? WHERE name = ?;

-- name: DeleteTarget :execrows
DELETE FROM targets WHERE name = ?;

-- name: GetTarget :one
SELECT
    t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name,
    t.volume_fs, t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
    t.volume_total, t.volume_used, t.volume_free, t.mount_script,
    COUNT(j.id) as job_count,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth, 
    ah.token_used as agent_token_used, ah.os as agent_os
FROM targets t
LEFT JOIN backups j ON t.name = j.target
LEFT JOIN agent_hosts ah ON t.agent_host = ah.name
WHERE t.name = ?
GROUP BY t.name;

-- name: GetTargetS3Secret :one
SELECT secret_s3 FROM targets WHERE name = ?;

-- name: ListAllTargets :many
SELECT
    t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name,
    t.volume_fs, t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
    t.volume_total, t.volume_used, t.volume_free, t.mount_script,
    COUNT(j.id) as job_count,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth, 
    ah.token_used as agent_token_used, ah.os as agent_os
FROM targets t
LEFT JOIN backups j ON t.name = j.target
LEFT JOIN agent_hosts ah ON t.agent_host = ah.name
GROUP BY t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name,
         t.volume_fs, t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
         t.volume_total, t.volume_used, t.volume_free, t.mount_script,
         ah.name, ah.ip, ah.auth, ah.token_used, ah.os
ORDER BY t.name;

-- name: ListTargetsByAgentHost :many
SELECT 
    t.name, t.path, t.agent_host, t.volume_id, t.volume_type, t.volume_name, t.volume_fs,
    t.volume_total_bytes, t.volume_used_bytes, t.volume_free_bytes,
    t.volume_total, t.volume_used, t.volume_free, t.mount_script,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth, 
    ah.token_used as agent_token_used, ah.os as agent_os
FROM targets t
LEFT JOIN agent_hosts ah ON t.agent_host = ah.name
WHERE t.agent_host = ?
ORDER BY t.name;

-- name: TargetExists :one
SELECT 1 FROM targets WHERE name = ? LIMIT 1;
