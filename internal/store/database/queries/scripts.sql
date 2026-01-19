-- name: CreateScript :exec
INSERT INTO scripts (path, description)
VALUES (?, ?);

-- name: UpdateScript :exec
UPDATE scripts 
SET description = ?
WHERE path = ?;

-- name: DeleteScript :execrows
DELETE FROM scripts WHERE path = ?;

-- name: GetScript :one
SELECT
    s.path, s.description,
    COUNT(DISTINCT CASE WHEN j.pre_script = s.path OR j.post_script = s.path THEN j.id END) as job_count,
    COUNT(DISTINCT CASE WHEN t.mount_script = s.path THEN t.name END) as target_count
FROM scripts s
LEFT JOIN backups j ON s.path = j.pre_script OR s.path = j.post_script
LEFT JOIN targets t ON s.path = t.mount_script
WHERE s.path = ?
GROUP BY s.path, s.description;

-- name: ListAllScripts :many
SELECT
    s.path, s.description,
    COUNT(DISTINCT CASE WHEN j.pre_script = s.path OR j.post_script = s.path THEN j.id END) as job_count,
    COUNT(DISTINCT CASE WHEN t.mount_script = s.path THEN t.name END) as target_count
FROM scripts s
LEFT JOIN backups j ON s.path = j.pre_script OR s.path = j.post_script
LEFT JOIN targets t ON s.path = t.mount_script
GROUP BY s.path, s.description
ORDER BY s.path;

-- name: ScriptExists :one
SELECT 1 FROM scripts WHERE path = ? LIMIT 1;
