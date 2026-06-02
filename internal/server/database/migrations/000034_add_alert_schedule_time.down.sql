-- Down migration: remove schedule_time and schedule_window_minutes columns.
-- SQLite DROP COLUMN requires 3.35.0+, so recreate table.
ALTER TABLE alert_settings RENAME TO _alert_settings_old;

CREATE TABLE alert_settings (
    name             TEXT PRIMARY KEY,
    enabled          INTEGER NOT NULL DEFAULT 1,
    threshold        INTEGER NOT NULL DEFAULT 0,
    severity         TEXT    NOT NULL DEFAULT 'warning',
    comment          TEXT    NOT NULL DEFAULT '',
    last_sent        INTEGER NOT NULL DEFAULT 0,
    cooldown_minutes INTEGER NOT NULL DEFAULT 1440,
    quiet_days       TEXT    NOT NULL DEFAULT '[]',
    skip_unscheduled INTEGER NOT NULL DEFAULT 0
);

INSERT INTO alert_settings (name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days, skip_unscheduled)
    SELECT name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days, skip_unscheduled
    FROM _alert_settings_old;

DROP TABLE _alert_settings_old;
