-- name: GetAlertSetting :one
SELECT name, enabled, threshold, severity, comment, last_sent
FROM alert_settings
WHERE name = ?;

-- name: ListAlertSettings :many
SELECT name, enabled, threshold, severity, comment, last_sent
FROM alert_settings
ORDER BY name;

-- name: UpsertAlertSetting :exec
INSERT INTO alert_settings (name, enabled, threshold, severity, comment, last_sent)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (name) DO UPDATE SET
    enabled   = excluded.enabled,
    threshold = excluded.threshold,
    severity  = excluded.severity,
    comment   = excluded.comment,
    last_sent = excluded.last_sent;

-- name: UpdateAlertLastSent :exec
UPDATE alert_settings SET last_sent = ? WHERE name = ?;

-- name: DeleteAlertSetting :exec
DELETE FROM alert_settings WHERE name = ?;
