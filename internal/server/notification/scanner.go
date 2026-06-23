package notification

import (
	"context"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	DefaultStaleDays       = 7
	DefaultCooldownMinutes = 1440
)

type AlertScanner struct {
	db *database.Database
}

func NewAlertScanner(db *database.Database) *AlertScanner {
	return &AlertScanner{db: db}
}

func (s *AlertScanner) Start(ctx context.Context, interval time.Duration) {
	syslog.L.Info().WithMessage("alert scanner started").Write()
	s.ensureDefaults()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	time.AfterFunc(30*time.Second, func() {
		s.RunChecks(ctx)
	})

	for {
		select {
		case <-ctx.Done():
			syslog.L.Info().WithMessage("alert scanner stopped").Write()
			return
		case <-ticker.C:
			s.RunChecks(ctx)
		}
	}
}

func (s *AlertScanner) RunChecks(ctx context.Context) {
	s.checkUnconfiguredTargets(ctx)
	s.checkStaleBackups(ctx)
}

func (s *AlertScanner) ensureDefaults() {
	defaults := []struct {
		name      string
		threshold int
		severity  string
	}{
		{string(AlertUnconfiguredTarget), 0, "warning"},
		{string(AlertStaleBackup), DefaultStaleDays, "warning"},
		{string(AlertTargetOffline), 0, "warning"},
	}
	for _, d := range defaults {
		s.db.EnsureAlertSetting(d.name, d.threshold, d.severity)
	}
}

func shouldSkip(setting database.AlertSetting) bool {
	if !setting.Enabled {
		return true
	}
	if setting.IsQuietDay() {
		return true
	}
	if setting.IsCoolingDown() {
		return true
	}
	if !setting.IsInScheduleWindow() {
		return true
	}
	return false
}

func isExcluded(exclusions map[string]bool, value string) bool {
	return exclusions != nil && exclusions[value]
}

func (s *AlertScanner) checkUnconfiguredTargets(ctx context.Context) {
	setting, err := s.db.GetAlertSetting(string(AlertUnconfiguredTarget))
	if err != nil || shouldSkip(setting) {
		return
	}

	excludedTargets, _ := s.db.GetExcludedValues(string(AlertUnconfiguredTarget), "target")

	targets, err := s.db.GetAllTargets()
	if err != nil {
		return
	}

	backups, err := s.db.GetAllBackups()
	if err != nil {
		return
	}

	coveredTargets := make(map[string]bool)
	for _, b := range backups {
		coveredTargets[b.Target.Name] = true
	}

	var unconfigured []database.Target
	for _, t := range targets {
		if coveredTargets[t.Name] {
			continue
		}
		if isExcluded(excludedTargets, t.Name) {
			continue
		}
		unconfigured = append(unconfigured, t)
	}

	if len(unconfigured) == 0 {
		return
	}

	details := map[string]string{
		"count": fmt.Sprintf("%d", len(unconfigured)),
	}
	var names []string
	for i, t := range unconfigured {
		if i >= 20 {
			names = append(names, fmt.Sprintf("... and %d more", len(unconfigured)-20))
			break
		}
		names = append(names, t.Name)
	}
	for i, n := range names {
		details[fmt.Sprintf("target-name-%d", i)] = n
	}
	details["target-count"] = fmt.Sprintf("%d", len(names))

	SendAlertWithData(AlertUnconfiguredTarget, setting.Severity, details, map[string]any{
		"targets": names,
	})
	s.db.UpdateAlertLastSent(string(AlertUnconfiguredTarget), time.Now().Unix())
}

func (s *AlertScanner) checkStaleBackups(ctx context.Context) {
	setting, err := s.db.GetAlertSetting(string(AlertStaleBackup))
	if err != nil || shouldSkip(setting) {
		return
	}

	threshold := setting.Threshold
	if threshold <= 0 {
		threshold = DefaultStaleDays
	}

	excludedJobs, _ := s.db.GetExcludedValues(string(AlertStaleBackup), "job")

	backups, err := s.db.GetAllBackups()
	if err != nil {
		return
	}

	now := time.Now()
	thresholdDuration := time.Duration(threshold) * 24 * time.Hour

	var staleJobs []database.Backup
	for _, b := range backups {
		if isExcluded(excludedJobs, b.ID) {
			continue
		}

		if setting.SkipUnscheduled && b.Schedule == "" {
			continue
		}

		lastSuccessful := b.History.LastSuccessfulEndtime
		if lastSuccessful == 0 {
			// Never ran  -  only report as stale if job has a schedule
			if !setting.SkipUnscheduled || b.Schedule != "" {
				staleJobs = append(staleJobs, b)
			}
			continue
		}

		if now.Sub(time.Unix(lastSuccessful, 0)) > thresholdDuration {
			staleJobs = append(staleJobs, b)
		}
	}

	if len(staleJobs) == 0 {
		return
	}

	type staleEntry struct {
		JobID     string `json:"job-id"`
		Datastore string `json:"datastore"`
		Target    string `json:"target"`
		DaysStale string `json:"days-stale"`
		LastRun   string `json:"last-run"`
	}

	var entries []staleEntry
	for _, b := range staleJobs {
		var daysStale string
		if b.History.LastSuccessfulEndtime == 0 {
			daysStale = "never"
		} else {
			days := int(now.Sub(time.Unix(b.History.LastSuccessfulEndtime, 0)).Hours() / 24)
			daysStale = fmt.Sprintf("%d", days)
		}
		entries = append(entries, staleEntry{
			JobID:     b.ID,
			Datastore: b.Store,
			Target:    b.Target.Name,
			DaysStale: daysStale,
			LastRun:   formatTimestamp(b.History.LastSuccessfulEndtime),
		})
	}

	SendAlertWithData(AlertStaleBackup, setting.Severity, map[string]string{
		"count":     fmt.Sprintf("%d", len(staleJobs)),
		"threshold": fmt.Sprintf("%d", threshold),
	}, map[string]any{
		"jobs": entries,
	})

	s.db.UpdateAlertLastSent(string(AlertStaleBackup), time.Now().Unix())
}

func formatTimestamp(unix int64) string {
	if unix == 0 {
		return "never"
	}
	return time.Unix(unix, 0).Format(time.RFC3339)
}
