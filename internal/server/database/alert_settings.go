package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database/sqlc"
)

type AlertSetting struct {
	Name            string   `json:"name"`
	Enabled         bool     `json:"enabled"`
	Threshold       int      `json:"threshold"`
	Severity        string   `json:"severity"`
	Comment         string   `json:"comment"`
	LastSent        int64    `json:"last-sent"`
	CooldownMinutes int      `json:"cooldown-minutes"`
	QuietDays       []string `json:"quiet-days"`
}

func sqlcToAlertSetting(row sqlc.AlertSetting) AlertSetting {
	var quietDays []string
	if row.QuietDays != "" {
		_ = json.Unmarshal([]byte(row.QuietDays), &quietDays)
	}
	if quietDays == nil {
		quietDays = []string{}
	}
	return AlertSetting{
		Name:            row.Name,
		Enabled:         row.Enabled == 1,
		Threshold:       int(row.Threshold),
		Severity:        row.Severity,
		Comment:         row.Comment,
		LastSent:        row.LastSent,
		CooldownMinutes: int(row.CooldownMinutes),
		QuietDays:       quietDays,
	}
}

func alertSettingQuietDaysJSON(days []string) string {
	if days == nil {
		days = []string{}
	}
	b, _ := json.Marshal(days)
	return string(b)
}

func (database *Database) GetAlertSetting(name string) (AlertSetting, error) {
	row, err := database.readQueries.GetAlertSetting(context.Background(), name)
	if err != nil {
		return AlertSetting{}, err
	}
	return sqlcToAlertSetting(row), nil
}

func (database *Database) ListAlertSettings() ([]AlertSetting, error) {
	rows, err := database.readQueries.ListAlertSettings(context.Background())
	if err != nil {
		return nil, err
	}
	result := make([]AlertSetting, len(rows))
	for i, row := range rows {
		result[i] = sqlcToAlertSetting(row)
	}
	return result, nil
}

func (database *Database) UpsertAlertSetting(setting AlertSetting) error {
	enabled := int64(0)
	if setting.Enabled {
		enabled = 1
	}
	return database.queries.UpsertAlertSetting(context.Background(), sqlc.UpsertAlertSettingParams{
		Name:            setting.Name,
		Enabled:         enabled,
		Threshold:       int64(setting.Threshold),
		Severity:        setting.Severity,
		Comment:         setting.Comment,
		LastSent:        setting.LastSent,
		CooldownMinutes: int64(setting.CooldownMinutes),
		QuietDays:       alertSettingQuietDaysJSON(setting.QuietDays),
	})
}

func (database *Database) UpdateAlertLastSent(name string, ts int64) error {
	return database.queries.UpdateAlertLastSent(context.Background(), sqlc.UpdateAlertLastSentParams{
		LastSent: ts,
		Name:     name,
	})
}

func (database *Database) DeleteAlertSetting(name string) error {
	return database.queries.DeleteAlertSetting(context.Background(), name)
}

// EnsureAlertSetting creates a setting with defaults if it doesn't exist.
func (database *Database) EnsureAlertSetting(name string, defaultThreshold int, defaultSeverity string) (AlertSetting, error) {
	setting, err := database.GetAlertSetting(name)
	if err == nil {
		return setting, nil
	}
	if err != sql.ErrNoRows {
		return AlertSetting{}, err
	}
	setting = AlertSetting{
		Name:            name,
		Enabled:         true,
		Threshold:       defaultThreshold,
		Severity:        defaultSeverity,
		CooldownMinutes: 1440, // 24h default
		QuietDays:       []string{},
	}
	if err := database.UpsertAlertSetting(setting); err != nil {
		return AlertSetting{}, err
	}
	return setting, nil
}

// IsCoolingDown returns true if the alert was sent within the configured cooldown duration.
func (s AlertSetting) IsCoolingDown() bool {
	if s.LastSent == 0 {
		return false
	}
	cooldown := time.Duration(s.CooldownMinutes) * time.Minute
	if cooldown <= 0 {
		cooldown = 24 * time.Hour
	}
	return time.Since(time.Unix(s.LastSent, 0)) < cooldown
}

// IsQuietDay returns true if today (in the alert's local weekday name) is in the quiet-days list.
func (s AlertSetting) IsQuietDay() bool {
	if len(s.QuietDays) == 0 {
		return false
	}
	today := time.Now().Weekday().String()
	return slices.Contains(s.QuietDays, today)
}
