package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database/sqlc"
)

type AlertSetting struct {
	Name      string `json:"name"`
	Enabled   bool   `json:"enabled"`
	Threshold int    `json:"threshold"`
	Severity  string `json:"severity"`
	Comment   string `json:"comment"`
	LastSent  int64  `json:"last-sent"`
}

func sqlcToAlertSetting(row sqlc.AlertSetting) AlertSetting {
	return AlertSetting{
		Name:      row.Name,
		Enabled:   row.Enabled == 1,
		Threshold: int(row.Threshold),
		Severity:  row.Severity,
		Comment:   row.Comment,
		LastSent:  row.LastSent,
	}
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
		Name:      setting.Name,
		Enabled:   enabled,
		Threshold: int64(setting.Threshold),
		Severity:  setting.Severity,
		Comment:   setting.Comment,
		LastSent:  setting.LastSent,
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
		Name:      name,
		Enabled:   true,
		Threshold: defaultThreshold,
		Severity:  defaultSeverity,
	}
	if err := database.UpsertAlertSetting(setting); err != nil {
		return AlertSetting{}, err
	}
	return setting, nil
}

// IsCoolingDown returns true if the alert was sent within the given cooldown duration.
func (s AlertSetting) IsCoolingDown(cooldown time.Duration) bool {
	if s.LastSent == 0 {
		return false
	}
	return time.Since(time.Unix(s.LastSent, 0)) < cooldown
}
