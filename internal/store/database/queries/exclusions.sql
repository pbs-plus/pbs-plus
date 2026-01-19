-- name: CreateExclusion :exec
INSERT INTO exclusions (job_id, path, comment) 
VALUES (?, ?, ?);

-- name: GetExclusion :one
SELECT job_id, path, comment 
FROM exclusions 
WHERE job_id = ? AND path = ?
LIMIT 1;

-- name: GetBackupExclusions :many
SELECT job_id, path, comment
FROM exclusions
WHERE job_id = ?
ORDER BY path;

-- name: DeleteExclusion :exec
DELETE FROM exclusions 
WHERE job_id = ? AND path = ?;

-- name: DeleteBackupExclusions :exec
DELETE FROM exclusions WHERE job_id = ?;

-- name: ListGlobalExclusions :many
SELECT path, comment 
FROM exclusions 
WHERE job_id = '';

-- name: ListAllBackupExclusions :many
SELECT job_id, path
FROM exclusions
ORDER BY job_id, path;

-- name: UpdateExclusion :execrows
UPDATE exclusions 
SET job_id = ?, comment = ? 
WHERE path = ?;
