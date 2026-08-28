-- Add configurable cooldown and quiet days to alert settings.
-- cooldown_minutes: minimum time between repeated alerts (0 = use default 1440 = 24h)
-- quiet_days: JSON array of weekday names when alerts should be suppressed,
--             e.g. '["Saturday","Sunday"]'
ALTER TABLE alert_settings ADD COLUMN cooldown_minutes INTEGER NOT NULL DEFAULT 1440;
ALTER TABLE alert_settings ADD COLUMN quiet_days TEXT NOT NULL DEFAULT '[]';
