package coredb

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

type AlertSetting struct {
	Name                  string   `json:"name"`
	Enabled               bool     `json:"enabled"`
	Threshold             int      `json:"threshold"`
	Severity              string   `json:"severity"`
	Comment               string   `json:"comment"`
	LastSent              int64    `json:"last-sent"`
	CooldownMinutes       int      `json:"cooldown-minutes"`
	QuietDays             []string `json:"quiet-days"`
	SkipUnscheduled       bool     `json:"skip-unscheduled"`
	ScheduleTime          string   `json:"schedule-time"`
	ScheduleWindowMinutes int      `json:"schedule-window-minutes"`
}

type AlertExclusion struct {
	ID           int64  `json:"id"`
	AlertType    string `json:"alert-type"`
	ExcludeType  string `json:"exclude-type"`
	ExcludeValue string `json:"exclude-value"`
	Comment      string `json:"comment"`
}

func sqlcToAlertSetting(row corequery.AlertSetting) AlertSetting {
	var quietDays []string
	if row.QuietDays != "" {
		if err := json.Unmarshal([]byte(row.QuietDays), &quietDays); err != nil {
			log.Error(err, "")
		}
	}
	if quietDays == nil {
		quietDays = []string{}
	}
	return AlertSetting{
		Name:                  row.Name,
		Enabled:               row.Enabled == 1,
		Threshold:             int(row.Threshold),
		Severity:              row.Severity,
		Comment:               row.Comment,
		LastSent:              row.LastSent,
		CooldownMinutes:       int(row.CooldownMinutes),
		QuietDays:             quietDays,
		SkipUnscheduled:       row.SkipUnscheduled == 1,
		ScheduleTime:          row.ScheduleTime,
		ScheduleWindowMinutes: int(row.ScheduleWindowMinutes),
	}
}

func sqlcToAlertExclusion(row corequery.AlertExclusion) AlertExclusion {
	return AlertExclusion{
		ID:           row.ID,
		AlertType:    row.AlertType,
		ExcludeType:  row.ExcludeType,
		ExcludeValue: row.ExcludeValue,
		Comment:      row.Comment,
	}
}

func alertSettingQuietDaysJSON(days []string) string {
	if days == nil {
		days = []string{}
	}
	b, err := json.Marshal(days)
	if err != nil {
		log.Error(err, "")
	}
	return string(b)
}

func (db *Store) GetAlertSetting(name string) (AlertSetting, error) {
	row, err := db.readQueries.GetAlertSetting(context.Background(), name)
	if err != nil {
		return AlertSetting{}, err
	}
	return sqlcToAlertSetting(row), nil
}

func (db *Store) ListAlertSettings() ([]AlertSetting, error) {
	rows, err := db.readQueries.ListAlertSettings(context.Background())
	if err != nil {
		return nil, err
	}
	result := make([]AlertSetting, len(rows))
	for i, row := range rows {
		result[i] = sqlcToAlertSetting(row)
	}
	return result, nil
}

func (db *Store) UpsertAlertSetting(setting AlertSetting) error {
	enabled := int64(0)
	if setting.Enabled {
		enabled = 1
	}
	skipUnscheduled := int64(0)
	if setting.SkipUnscheduled {
		skipUnscheduled = 1
	}
	return db.queries.UpsertAlertSetting(context.Background(), corequery.UpsertAlertSettingParams{
		Name:                  setting.Name,
		Enabled:               enabled,
		Threshold:             int64(setting.Threshold),
		Severity:              setting.Severity,
		Comment:               setting.Comment,
		LastSent:              setting.LastSent,
		CooldownMinutes:       int64(setting.CooldownMinutes),
		QuietDays:             alertSettingQuietDaysJSON(setting.QuietDays),
		SkipUnscheduled:       skipUnscheduled,
		ScheduleTime:          setting.ScheduleTime,
		ScheduleWindowMinutes: int64(setting.ScheduleWindowMinutes),
	})
}

func (db *Store) UpdateAlertLastSent(name string, ts int64) error {
	return db.queries.UpdateAlertLastSent(context.Background(), corequery.UpdateAlertLastSentParams{
		LastSent: ts,
		Name:     name,
	})
}

func (db *Store) DeleteAlertSetting(name string) error {
	return db.queries.DeleteAlertSetting(context.Background(), name)
}

func (db *Store) EnsureAlertSetting(name string, defaultThreshold int, defaultSeverity string) (AlertSetting, error) {
	setting, err := db.GetAlertSetting(name)
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
		CooldownMinutes: 1440,
		QuietDays:       []string{},
	}
	if err := db.UpsertAlertSetting(setting); err != nil {
		return AlertSetting{}, err
	}
	return setting, nil
}

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

func (s AlertSetting) IsQuietDay() bool {
	if len(s.QuietDays) == 0 {
		return false
	}
	today := time.Now().Weekday().String()
	return slices.Contains(s.QuietDays, today)
}

// schedule window. If no schedule_time is set, it always returns true (any time).
func (s AlertSetting) IsInScheduleWindow() bool {
	if s.ScheduleTime == "" {
		return true
	}

	parts := strings.SplitN(s.ScheduleTime, ":", 2)
	if len(parts) != 2 {
		return true
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return true
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return true
	}

	window := s.ScheduleWindowMinutes
	if window <= 0 {
		window = 60
	}

	now := time.Now()
	scheduledMinutes := hour*60 + minute
	nowMinutes := now.Hour()*60 + now.Minute()

	halfWindow := window / 2
	diff := nowMinutes - scheduledMinutes
	if diff < 0 {
		diff = -diff
	}

	if diff > 720 {
		diff = 1440 - diff
	}

	return diff <= halfWindow
}

func (db *Store) CreateAlertExclusion(alertType, excludeType, excludeValue, comment string) error {
	return db.queries.CreateAlertExclusion(context.Background(), corequery.CreateAlertExclusionParams{
		AlertType:    alertType,
		ExcludeType:  excludeType,
		ExcludeValue: excludeValue,
		Comment:      comment,
	})
}

func (db *Store) DeleteAlertExclusion(id int64) error {
	return db.queries.DeleteAlertExclusion(context.Background(), id)
}

func (db *Store) ListAlertExclusions(alertType string) ([]AlertExclusion, error) {
	rows, err := db.readQueries.ListAlertExclusions(context.Background(), alertType)
	if err != nil {
		return nil, err
	}
	result := make([]AlertExclusion, len(rows))
	for i, row := range rows {
		result[i] = sqlcToAlertExclusion(row)
	}
	return result, nil
}

func (db *Store) ListAllAlertExclusions() ([]AlertExclusion, error) {
	rows, err := db.readQueries.ListAllAlertExclusions(context.Background())
	if err != nil {
		return nil, err
	}
	result := make([]AlertExclusion, len(rows))
	for i, row := range rows {
		result[i] = sqlcToAlertExclusion(row)
	}
	return result, nil
}

func (db *Store) GetAlertExclusion(id int64) (AlertExclusion, error) {
	row, err := db.readQueries.GetAlertExclusion(context.Background(), id)
	if err != nil {
		return AlertExclusion{}, err
	}
	return sqlcToAlertExclusion(row), nil
}

// GetExcludedValues returns a set of excluded values for a given alert type and exclude type.
func (db *Store) GetExcludedValues(alertType, excludeType string) (map[string]bool, error) {
	rows, err := db.readQueries.GetAlertExclusionsByType(context.Background(), corequery.GetAlertExclusionsByTypeParams{
		AlertType:   alertType,
		ExcludeType: excludeType,
	})
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool, len(rows))
	for _, row := range rows {
		result[row.ExcludeValue] = true
	}
	return result, nil
}
