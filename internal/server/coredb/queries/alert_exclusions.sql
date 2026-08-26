-- name: CreateAlertExclusion :exec
INSERT INTO alert_exclusions (alert_type, exclude_type, exclude_value, comment)
VALUES (?, ?, ?, ?)
ON CONFLICT (alert_type, exclude_type, exclude_value) DO UPDATE SET
    comment = excluded.comment;

-- name: DeleteAlertExclusion :exec
DELETE FROM alert_exclusions WHERE id = ?;

-- name: ListAlertExclusions :many
SELECT id, alert_type, exclude_type, exclude_value, comment
FROM alert_exclusions
WHERE alert_type = ?
ORDER BY exclude_type, exclude_value;

-- name: ListAllAlertExclusions :many
SELECT id, alert_type, exclude_type, exclude_value, comment
FROM alert_exclusions
ORDER BY alert_type, exclude_type, exclude_value;

-- name: GetAlertExclusion :one
SELECT id, alert_type, exclude_type, exclude_value, comment
FROM alert_exclusions
WHERE id = ?;

-- name: GetAlertExclusionsByType :many
SELECT id, alert_type, exclude_type, exclude_value, comment
FROM alert_exclusions
WHERE alert_type = ? AND exclude_type = ?;

-- name: DeleteAlertExclusionsByType :exec
DELETE FROM alert_exclusions WHERE alert_type = ?;
