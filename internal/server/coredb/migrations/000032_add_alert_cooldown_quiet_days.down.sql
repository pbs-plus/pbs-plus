-- SQLite does not support DROP COLUMN before 3.35.0, so recreate the table.
ALTER TABLE alert_settings RENAME TO alert_settings_old;

CREATE TABLE alert_settings (
    name             TEXT PRIMARY KEY,
    enabled          INTEGER NOT NULL DEFAULT 1,
    threshold        INTEGER NOT NULL DEFAULT 0,
    severity         TEXT    NOT NULL DEFAULT 'warning',
    comment          TEXT    NOT NULL DEFAULT '',
    last_sent        INTEGER NOT NULL DEFAULT 0,
    cooldown_minutes INTEGER NOT NULL DEFAULT 1440,
    quiet_days       TEXT    NOT NULL DEFAULT '[]'
);

INSERT INTO alert_settings (name, enabled, threshold, severity, comment, last_sent, cooldown_minutes, quiet_days)
SELECT name, enabled, threshold, severity, comment, last_sent, 1440, '[]'
FROM alert_settings_old;

DROP TABLE alert_settings_old;
