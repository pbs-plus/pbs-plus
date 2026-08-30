-- name: CreateTarget :exec
INSERT INTO targets (name, target_type, mount_script)
VALUES (?, ?, ?);

-- name: UpdateTarget :exec
UPDATE targets
SET target_type = ?, mount_script = ?
WHERE name = ?;

-- name: UpsertTarget :exec
INSERT INTO targets (name, target_type, mount_script)
VALUES (?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    target_type = excluded.target_type,
    mount_script = excluded.mount_script;

-- name: UpsertTargetFilesystem :exec
INSERT INTO target_filesystems (
    target_name, access, path, agent_host, volume_id, volume_type, volume_name,
    volume_fs, volume_total_bytes, volume_used_bytes, volume_free_bytes,
    volume_total, volume_used, volume_free
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(target_name) DO UPDATE SET
    access = excluded.access,
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
    volume_free = excluded.volume_free;

-- name: UpsertTargetS3 :exec
INSERT INTO target_s3 (target_name, url)
VALUES (?, ?)
ON CONFLICT(target_name) DO UPDATE SET url = excluded.url;

-- name: DeleteTargetFilesystem :exec
DELETE FROM target_filesystems WHERE target_name = ?;

-- name: DeleteTargetS3 :exec
DELETE FROM target_s3 WHERE target_name = ?;

-- name: UpdateTargetS3Secret :execrows
UPDATE target_s3 SET secret = ? WHERE target_name = ?;

-- name: DeleteTarget :execrows
DELETE FROM targets WHERE name = ?;

-- name: GetTarget :one
SELECT
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    COUNT(j.id) AS job_count,
    ah.name AS agent_name, ah.ip AS agent_ip, ah.auth AS agent_auth,
    ah.token_used AS agent_token_used, ah.os AS agent_os
FROM targets t
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN backups j ON t.name = j.target
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
WHERE t.name = ?
GROUP BY t.name;

-- name: GetTargetS3Secret :one
SELECT secret FROM target_s3 WHERE target_name = ?;

-- name: ListAllTargets :many
SELECT
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    COUNT(j.id) AS job_count,
    ah.name AS agent_name, ah.ip AS agent_ip, ah.auth AS agent_auth,
    ah.token_used AS agent_token_used, ah.os AS agent_os
FROM targets t
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN backups j ON t.name = j.target
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
GROUP BY t.name, t.target_type, t.mount_script, f.access, f.path, s.url,
         f.agent_host, f.volume_id, f.volume_type, f.volume_name, f.volume_fs,
         f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
         f.volume_total, f.volume_used, f.volume_free,
         ah.name, ah.ip, ah.auth, ah.token_used, ah.os
ORDER BY t.name;

-- name: ListTargetsByAgentHost :many
SELECT
    t.name, t.target_type, t.mount_script,
    f.access AS filesystem_access, f.path, f.agent_host, f.volume_id,
    f.volume_type, f.volume_name, f.volume_fs, f.volume_total_bytes,
    f.volume_used_bytes, f.volume_free_bytes, f.volume_total, f.volume_used,
    f.volume_free,
    ah.name AS agent_name, ah.ip AS agent_ip, ah.auth AS agent_auth,
    ah.token_used AS agent_token_used, ah.os AS agent_os
FROM targets t
JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
WHERE f.agent_host = ?
ORDER BY t.name;

-- name: TargetExists :one
SELECT 1 FROM targets WHERE name = ? LIMIT 1;
