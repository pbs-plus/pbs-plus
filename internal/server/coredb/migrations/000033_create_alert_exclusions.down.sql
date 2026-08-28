-- Down migration: remove alert exclusions table and skip_unscheduled column.
-- SQLite DROP COLUMN requires 3.35.0+, so recreate table.
ALTER TABLE alert_exclusions RENAME TO _alert_exclusions_old;

DROP TABLE IF EXISTS _alert_exclusions_old;

CREATE TABLE alert_settings_backup AS
    SELECT name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days
    FROM alert_settings;

DROP TABLE alert_settings;

CREATE TABLE alert_settings (
    name            TEXT PRIMARY KEY,
    enabled         INTEGER NOT NULL DEFAULT 1,
    threshold       INTEGER NOT NULL DEFAULT 0,
    severity        TEXT    NOT NULL DEFAULT 'warning',
    comment         TEXT    NOT NULL DEFAULT '',
    last_sent       INTEGER NOT NULL DEFAULT 0,
    cooldown_minutes INTEGER NOT NULL DEFAULT 1440,
    quiet_days      TEXT    NOT NULL DEFAULT '[]'
);

INSERT INTO alert_settings (name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days)
    SELECT name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days
    FROM alert_settings_backup;

DROP TABLE alert_settings_backup;
