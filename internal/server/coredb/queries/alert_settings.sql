-- name: GetAlertSetting :one
SELECT name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days, skip_unscheduled, schedule_time, schedule_window_minutes
FROM alert_settings
WHERE name = ?;

-- name: ListAlertSettings :many
SELECT name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days, skip_unscheduled, schedule_time, schedule_window_minutes
FROM alert_settings
ORDER BY name;

-- name: UpsertAlertSetting :exec
INSERT INTO alert_settings (name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days, skip_unscheduled, schedule_time, schedule_window_minutes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (name) DO UPDATE SET
    enabled                  = excluded.enabled,
    threshold                = excluded.threshold,
    severity                 = excluded.severity,
    comment                  = excluded.comment,
    last_sent                = excluded.last_sent,
    cooldown_minutes         = excluded.cooldown_minutes,
    quiet_days               = excluded.quiet_days,
    skip_unscheduled         = excluded.skip_unscheduled,
    schedule_time            = excluded.schedule_time,
    schedule_window_minutes  = excluded.schedule_window_minutes;

-- name: UpdateAlertLastSent :exec
UPDATE alert_settings SET last_sent = ? WHERE name = ?;

-- name: DeleteAlertSetting :exec
DELETE FROM alert_settings WHERE name = ?;
