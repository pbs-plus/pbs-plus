-- Add scheduled alert time window to alert settings.
-- schedule_time: HH:MM format (24h), e.g. "09:00". When set, alerts only fire
--                during the check that runs closest to this time.
-- schedule_window_minutes: how many minutes around schedule_time the alert is
--                          allowed to fire (default 60 = ±30 min).
ALTER TABLE alert_settings ADD COLUMN schedule_time TEXT NOT NULL DEFAULT '';
ALTER TABLE alert_settings ADD COLUMN schedule_window_minutes INTEGER NOT NULL DEFAULT 60;
