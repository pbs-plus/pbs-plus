-- Alert exclusions for D2D monitoring notifications.
-- Allows excluding specific jobs or targets from individual alert types.
-- Also adds skip_unscheduled flag to alert_settings for stale-backup.
ALTER TABLE alert_settings ADD COLUMN skip_unscheduled INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS alert_exclusions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type    TEXT    NOT NULL,                    -- 'stale-backup', 'unconfigured-target', 'target-offline'
    exclude_type  TEXT    NOT NULL,                    -- 'job', 'target'
    exclude_value TEXT    NOT NULL,                    -- job ID or target name
    comment       TEXT    NOT NULL DEFAULT '',
    UNIQUE(alert_type, exclude_type, exclude_value)
);
