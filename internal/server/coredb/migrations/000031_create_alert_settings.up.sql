-- Alert settings for D2D monitoring notifications.
-- Stores per-alert-type configuration (enable/disable, thresholds).
-- Each alert type gets one row, keyed by its alert name.
CREATE TABLE IF NOT EXISTS alert_settings (
    name         TEXT PRIMARY KEY,          -- e.g. "unconfigured-target", "stale-backup"
    enabled      INTEGER NOT NULL DEFAULT 1, -- 0 = disabled, 1 = enabled
    threshold    INTEGER NOT NULL DEFAULT 0, -- alert-specific threshold (e.g. days for stale)
    severity     TEXT    NOT NULL DEFAULT 'warning', -- info | warning | error
    comment      TEXT    NOT NULL DEFAULT '',
    last_sent    INTEGER NOT NULL DEFAULT 0  -- unix timestamp of last sent alert (for dedup/cooldown)
);
