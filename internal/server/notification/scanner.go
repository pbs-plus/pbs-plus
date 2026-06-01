package notification

import (
	"context"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	// DefaultStaleDays is the default threshold for stale-backup alerts.
	DefaultStaleDays = 7

	// DefaultCooldownMinutes is the default cooldown between repeated alerts.
	DefaultCooldownMinutes = 1440 // 24 hours
)

// AlertScanner periodically checks for D2D alert conditions and sends notifications.
type AlertScanner struct {
	db *database.Database
}

// NewAlertScanner creates a new alert scanner.
func NewAlertScanner(db *database.Database) *AlertScanner {
	return &AlertScanner{db: db}
}

// Start runs the alert scanner loop. It checks every interval and exits when ctx is done.
func (s *AlertScanner) Start(ctx context.Context, interval time.Duration) {
	slog := syslog.L.Info().WithMessage("alert scanner started")
	slog.Write()

	// Ensure all default alert settings exist
	s.ensureDefaults()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run once immediately on startup (after a short delay to let services initialize)
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

// RunChecks executes all alert checks once.
func (s *AlertScanner) RunChecks(ctx context.Context) {
	s.checkUnconfiguredTargets(ctx)
	s.checkStaleBackups(ctx)
}

// ensureDefaults creates default alert settings if they don't exist.
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

// shouldSkip returns true if the alert is disabled, in cooldown, or on a quiet day.
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
	return false
}

// checkUnconfiguredTargets finds targets that have no backup job assigned.
func (s *AlertScanner) checkUnconfiguredTargets(ctx context.Context) {
	setting, err := s.db.GetAlertSetting(string(AlertUnconfiguredTarget))
	if err != nil || shouldSkip(setting) {
		return
	}

	targets, err := s.db.GetAllTargets()
	if err != nil {
		return
	}

	backups, err := s.db.GetAllBackups()
	if err != nil {
		return
	}

	// Build a set of targets that have at least one backup job
	coveredTargets := make(map[string]bool)
	for _, b := range backups {
		coveredTargets[b.Target.Name] = true
	}

	// Find unconfigured targets
	var unconfigured []database.Target
	for _, t := range targets {
		if !coveredTargets[t.Name] {
			unconfigured = append(unconfigured, t)
		}
	}

	if len(unconfigured) == 0 {
		return
	}

	// Send a single alert listing all unconfigured targets
	details := map[string]string{
		"count": fmt.Sprintf("%d", len(unconfigured)),
	}

	// Include target names in details (up to a reasonable limit)
	for i, t := range unconfigured {
		if i >= 10 {
			details[fmt.Sprintf("target-name-%d", i)] = fmt.Sprintf("... and %d more", len(unconfigured)-10)
			break
		}
		details[fmt.Sprintf("target-name-%d", i)] = t.Name
	}

	SendAlert(AlertUnconfiguredTarget, setting.Severity, details)

	// Update last-sent timestamp for cooldown
	s.db.UpdateAlertLastSent(string(AlertUnconfiguredTarget), time.Now().Unix())
}

// checkStaleBackups finds backup jobs that haven't run successfully in a configurable number of days.
func (s *AlertScanner) checkStaleBackups(ctx context.Context) {
	setting, err := s.db.GetAlertSetting(string(AlertStaleBackup))
	if err != nil || shouldSkip(setting) {
		return
	}

	threshold := setting.Threshold
	if threshold <= 0 {
		threshold = DefaultStaleDays
	}

	backups, err := s.db.GetAllBackups()
	if err != nil {
		return
	}

	now := time.Now()
	thresholdDuration := time.Duration(threshold) * 24 * time.Hour

	var staleJobs []database.Backup
	for _, b := range backups {
		lastSuccessful := b.History.LastSuccessfulEndtime
		if lastSuccessful == 0 {
			// Never ran successfully — always report as stale
			staleJobs = append(staleJobs, b)
			continue
		}

		lastRun := time.Unix(lastSuccessful, 0)
		if now.Sub(lastRun) > thresholdDuration {
			staleJobs = append(staleJobs, b)
		}
	}

	if len(staleJobs) == 0 {
		return
	}

	// Send a single alert per stale job
	for _, b := range staleJobs {
		var daysStale string
		if b.History.LastSuccessfulEndtime == 0 {
			daysStale = "never"
		} else {
			days := int(now.Sub(time.Unix(b.History.LastSuccessfulEndtime, 0)).Hours() / 24)
			daysStale = fmt.Sprintf("%d", days)
		}

		SendAlert(AlertStaleBackup, setting.Severity, map[string]string{
			"job-id":     b.ID,
			"datastore":  b.Store,
			"target":     b.Target.Name,
			"days-stale": daysStale,
			"schedule":   b.Schedule,
			"last-run":   formatTimestamp(b.History.LastSuccessfulEndtime),
			"job-status": b.History.LastRunStatus.String(),
		})
	}

	// Update last-sent timestamp for cooldown
	s.db.UpdateAlertLastSent(string(AlertStaleBackup), time.Now().Unix())
}

func formatTimestamp(unix int64) string {
	if unix == 0 {
		return "never"
	}
	return time.Unix(unix, 0).Format(time.RFC3339)
}
