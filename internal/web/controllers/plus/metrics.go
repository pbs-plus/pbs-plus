//go:build linux

package plus

import (
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	globalRegistry *prometheus.Registry
	globalMetrics  *metrics
)

func init() {
	globalRegistry = prometheus.NewRegistry()
	globalMetrics = newMetrics(globalRegistry)
}

type metrics struct {
	backupsTotal                        prometheus.Gauge
	backupLastRunSuccess                *prometheus.GaugeVec
	backupLastRunTimestamp              *prometheus.GaugeVec
	backupLastSuccessfulTimestamp       *prometheus.GaugeVec
	backupNextRunTimestamp              *prometheus.GaugeVec
	backupDuration                      *prometheus.GaugeVec
	backupRunning                       *prometheus.GaugeVec
	backupQueued                        *prometheus.GaugeVec
	backupExpectedSize                  *prometheus.GaugeVec
	backupCurrentBytesTotal             *prometheus.GaugeVec
	backupCurrentBytesSpeed             *prometheus.GaugeVec
	backupCurrentFilesSpeed             *prometheus.GaugeVec
	backupCurrentFileCount              *prometheus.GaugeVec
	backupCurrentFolderCount            *prometheus.GaugeVec
	backupTimeSinceLastSuccess          *prometheus.GaugeVec
	backupLatestSnapshotSize            *prometheus.GaugeVec
	backupsRunningTotal                 prometheus.Gauge
	backupsQueuedTotal                  prometheus.Gauge
	backupsLastRunFailedTotal           prometheus.Gauge
	backupsLastRunSuccessTotal          prometheus.Gauge
	targetsTotal                        prometheus.Gauge
	targetVolumeTotalBytes              *prometheus.GaugeVec
	targetVolumeUsedBytes               *prometheus.GaugeVec
	targetVolumeFreeBytes               *prometheus.GaugeVec
	targetVolumeUsagePercent            *prometheus.GaugeVec
	targetBackupCount                   *prometheus.GaugeVec
	targetInfo                          *prometheus.GaugeVec
	targetsAgentTotal                   prometheus.Gauge
	targetsS3Total                      prometheus.Gauge
	targetsLocalTotal                   prometheus.Gauge
	targetLastSuccessfulBackupTimestamp *prometheus.GaugeVec
	targetTimeSinceLastSuccessfulBackup *prometheus.GaugeVec
	targetHasFailedBackups              *prometheus.GaugeVec
	targetFailedBackupCount             *prometheus.GaugeVec
	targetSuccessfulBackupCount         *prometheus.GaugeVec
	previousBackupLabels                map[string]prometheus.Labels
	previousTargetLabels                map[string]prometheus.Labels
	previousTargetInfoLabels            map[string]prometheus.Labels
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		backupsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_backups_total",
			Help: "Total number of backup backups",
		}),
		backupLastRunSuccess: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_last_run_success",
				Help: "Last run success status (1=success, 0=failure, -1=unknown)",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupLastRunTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_last_run_timestamp_seconds",
				Help: "Timestamp of the last backup run",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupLastSuccessfulTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_last_successful_timestamp_seconds",
				Help: "Timestamp of the last successful backup run",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupNextRunTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_next_run_timestamp_seconds",
				Help: "Timestamp of the next scheduled backup run",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_duration_seconds",
				Help: "Duration of the last backup run in seconds",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_running",
				Help: "Backup currently running (1=running, 0=not running)",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupQueued: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_queued",
				Help: "Backup is in queue (1=queued, 0=not queued)",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupExpectedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_expected_size_bytes",
				Help: "Expected size of the backup in bytes",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupCurrentBytesTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_current_bytes_total",
				Help: "Current bytes transferred during active backup",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupCurrentBytesSpeed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_current_bytes_speed",
				Help: "Current transfer speed in bytes per second",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupCurrentFilesSpeed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_current_files_speed",
				Help: "Current file processing speed in files per second",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupCurrentFileCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_current_file_count",
				Help: "Current number of files processed",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupCurrentFolderCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_current_folder_count",
				Help: "Current number of folders processed",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupTimeSinceLastSuccess: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_time_since_last_success_seconds",
				Help: "Time since last successful run in seconds",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupLatestSnapshotSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_backup_latest_snapshot_size_bytes",
				Help: "Size of the latest snapshot in bytes",
			},
			[]string{"backup_id", "target", "store", "mode", "schedule"},
		),
		backupsRunningTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_backups_running_total",
			Help: "Number of currently running backups",
		}),
		backupsQueuedTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_backups_queued_total",
			Help: "Number of queued backups",
		}),
		backupsLastRunFailedTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_backups_last_run_failed_total",
			Help: "Number of backups that failed on last run",
		}),
		backupsLastRunSuccessTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_backups_last_run_success_total",
			Help: "Number of backups that succeeded on last run",
		}),
		targetsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_targets_total",
			Help: "Total number of backup targets",
		}),
		targetVolumeTotalBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_volume_total_bytes",
				Help: "Total capacity of target drive in bytes",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
		),
		targetVolumeUsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_volume_used_bytes",
				Help: "Used capacity of target drive in bytes",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
		),
		targetVolumeFreeBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_volume_free_bytes",
				Help: "Free capacity of target drive in bytes",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
		),
		targetVolumeUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_volume_usage_percent",
				Help: "Volume usage percentage (0-100)",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
		),
		targetBackupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_backup_count",
				Help: "Number of backups associated with this target",
			},
			[]string{"target_name"},
		),
		targetInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_info",
				Help: "Target metadata (always 1, use labels for info)",
			},
			[]string{"target_name", "path", "volume_type", "volume_name", "volume_fs", "os", "is_agent", "is_s3"},
		),
		targetsAgentTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_targets_agent_total",
			Help: "Number of agent-based targets",
		}),
		targetsS3Total: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_targets_s3_total",
			Help: "Number of S3 targets",
		}),
		targetsLocalTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_targets_local_total",
			Help: "Number of local/other targets",
		}),
		targetLastSuccessfulBackupTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_last_successful_backup_timestamp_seconds",
				Help: "Timestamp of the most recent successful backup for this target",
			},
			[]string{"target_name"},
		),
		targetTimeSinceLastSuccessfulBackup: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_time_since_last_successful_backup_seconds",
				Help: "Time since the most recent successful backup for this target in seconds",
			},
			[]string{"target_name"},
		),
		targetHasFailedBackups: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_has_failed_backups",
				Help: "Target has at least one failed backup (1=yes, 0=no)",
			},
			[]string{"target_name"},
		),
		targetFailedBackupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_failed_backup_count",
				Help: "Number of backups that failed on last run for this target",
			},
			[]string{"target_name"},
		),
		targetSuccessfulBackupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_successful_backup_count",
				Help: "Number of backups that succeeded on last run for this target",
			},
			[]string{"target_name"},
		),
		previousBackupLabels:     make(map[string]prometheus.Labels),
		previousTargetLabels:     make(map[string]prometheus.Labels),
		previousTargetInfoLabels: make(map[string]prometheus.Labels),
	}

	reg.MustRegister(
		m.backupsTotal,
		m.backupLastRunSuccess,
		m.backupLastRunTimestamp,
		m.backupLastSuccessfulTimestamp,
		m.backupNextRunTimestamp,
		m.backupDuration,
		m.backupRunning,
		m.backupQueued,
		m.backupExpectedSize,
		m.backupCurrentBytesTotal,
		m.backupCurrentBytesSpeed,
		m.backupCurrentFilesSpeed,
		m.backupCurrentFileCount,
		m.backupCurrentFolderCount,
		m.backupTimeSinceLastSuccess,
		m.backupLatestSnapshotSize,
		m.backupsRunningTotal,
		m.backupsQueuedTotal,
		m.backupsLastRunFailedTotal,
		m.backupsLastRunSuccessTotal,
		m.targetsTotal,
		m.targetVolumeTotalBytes,
		m.targetVolumeUsedBytes,
		m.targetVolumeFreeBytes,
		m.targetVolumeUsagePercent,
		m.targetBackupCount,
		m.targetInfo,
		m.targetsAgentTotal,
		m.targetsS3Total,
		m.targetsLocalTotal,
		m.targetLastSuccessfulBackupTimestamp,
		m.targetTimeSinceLastSuccessfulBackup,
		m.targetHasFailedBackups,
		m.targetFailedBackupCount,
		m.targetSuccessfulBackupCount,
	)

	return m
}

// PrometheusMetricsHandler returns an HTTP handler that exposes Prometheus metrics
func PrometheusMetricsHandler(storeInstance *store.Store) http.HandlerFunc {
	handler := promhttp.HandlerFor(globalRegistry, promhttp.HandlerOpts{
		Registry: globalRegistry,
	})

	// Wrap with collector that updates metrics on each scrape
	return func(w http.ResponseWriter, r *http.Request) {
		updateMetrics(globalMetrics, storeInstance, time.Now().Unix())
		handler.ServeHTTP(w, r)
	}
}

func updateMetrics(m *metrics, storeInstance *store.Store, now int64) {
	currentBackupLabels := make(map[string]prometheus.Labels)
	currentTargetLabels := make(map[string]prometheus.Labels)
	currentTargetInfoLabels := make(map[string]prometheus.Labels)

	// Collect backup metrics
	backups, err := storeInstance.Database.GetAllBackups()
	if err != nil {
		syslog.L.Error(err).
			WithField("handler", "prometheus_metrics").
			WithMessage("failed to get backups").Write()
		return
	}

	m.backupsTotal.Set(float64(len(backups)))

	var runningCount, queuedCount, failedCount, successCount int

	// Track per-target statistics
	targetStats := make(map[string]struct {
		lastSuccessfulTimestamp int64
		failedCount             int
		successfulCount         int
	})

	for _, backup := range backups {
		labels := prometheus.Labels{
			"backup_id": backup.ID,
			"target":    backup.Target.Name,
			"store":     backup.Store,
			"mode":      backup.Mode,
			"schedule":  backup.Schedule,
		}
		currentBackupLabels[backup.ID] = labels

		// Last run success status
		// Consider successful if LastRunEndtime == LastSuccessfulEndtime OR if LastRunState == "OK"
		successValue := float64(-1)
		isSuccess := false
		if backup.History.LastRunEndtime > 0 && backup.History.LastSuccessfulEndtime > 0 && backup.History.LastRunEndtime == backup.History.LastSuccessfulEndtime {
			successValue = 1
			successCount++
			isSuccess = true
		} else if backup.History.LastRunState == "OK" {
			successValue = 1
			successCount++
			isSuccess = true
		} else if backup.History.LastRunEndtime > 0 || backup.History.LastRunState != "" {
			// If we have a last run but it's not successful, mark as failed
			successValue = 0
			failedCount++
		}
		m.backupLastRunSuccess.With(labels).Set(successValue)

		// Update target statistics
		stats := targetStats[backup.Target.Name]
		if isSuccess {
			stats.successfulCount++
			// Track the most recent successful timestamp for this target
			if backup.History.LastSuccessfulEndtime > stats.lastSuccessfulTimestamp {
				stats.lastSuccessfulTimestamp = backup.History.LastSuccessfulEndtime
			}
		} else if successValue == 0 {
			stats.failedCount++
		}
		targetStats[backup.Target.Name] = stats

		// Timestamps
		if backup.History.LastRunEndtime > 0 {
			m.backupLastRunTimestamp.With(labels).Set(float64(backup.History.LastRunEndtime))
		}
		if backup.History.LastSuccessfulEndtime > 0 {
			m.backupLastSuccessfulTimestamp.With(labels).Set(float64(backup.History.LastSuccessfulEndtime))
			m.backupTimeSinceLastSuccess.With(labels).Set(float64(now - backup.History.LastSuccessfulEndtime))
		}
		if backup.NextRun > 0 {
			m.backupNextRunTimestamp.With(labels).Set(float64(backup.NextRun))
		}

		// Duration
		if backup.History.Duration > 0 {
			m.backupDuration.With(labels).Set(float64(backup.History.Duration))
		}

		// Running status
		isRunning := float64(0)
		if backup.CurrentPID > 0 {
			isRunning = 1
			runningCount++
		}
		m.backupRunning.With(labels).Set(isRunning)

		// Queued status
		isQueued := float64(0)
		if strings.Contains(backup.History.LastRunUpid, "pbsplusgen-queue") {
			isQueued = 1
			queuedCount++
		}
		m.backupQueued.With(labels).Set(isQueued)

		// Size metrics
		if backup.Target.VolumeUsedBytes > 0 {
			m.backupExpectedSize.With(labels).Set(float64(backup.Target.VolumeUsedBytes))
		}
		if backup.CurrentStats.CurrentBytesTotal > 0 {
			m.backupCurrentBytesTotal.With(labels).Set(float64(backup.CurrentStats.CurrentBytesTotal))
		}
		if backup.CurrentStats.CurrentBytesSpeed > 0 {
			m.backupCurrentBytesSpeed.With(labels).Set(float64(backup.CurrentStats.CurrentBytesSpeed))
		}
		if backup.CurrentStats.CurrentFilesSpeed > 0 {
			m.backupCurrentFilesSpeed.With(labels).Set(float64(backup.CurrentStats.CurrentFilesSpeed))
		}
		if backup.CurrentStats.CurrentFileCount > 0 {
			m.backupCurrentFileCount.With(labels).Set(float64(backup.CurrentStats.CurrentFileCount))
		}
		if backup.CurrentStats.CurrentFolderCount > 0 {
			m.backupCurrentFolderCount.With(labels).Set(float64(backup.CurrentStats.CurrentFolderCount))
		}
		if backup.History.LatestSnapshotSize > 0 {
			m.backupLatestSnapshotSize.With(labels).Set(float64(backup.History.LatestSnapshotSize))
		}
	}

	// Delete metrics for backups that no longer exist
	for backupID, labels := range m.previousBackupLabels {
		if _, exists := currentBackupLabels[backupID]; !exists {
			m.backupLastRunSuccess.Delete(labels)
			m.backupLastRunTimestamp.Delete(labels)
			m.backupLastSuccessfulTimestamp.Delete(labels)
			m.backupNextRunTimestamp.Delete(labels)
			m.backupDuration.Delete(labels)
			m.backupRunning.Delete(labels)
			m.backupQueued.Delete(labels)
			m.backupExpectedSize.Delete(labels)
			m.backupCurrentBytesTotal.Delete(labels)
			m.backupCurrentBytesSpeed.Delete(labels)
			m.backupCurrentFilesSpeed.Delete(labels)
			m.backupCurrentFileCount.Delete(labels)
			m.backupCurrentFolderCount.Delete(labels)
			m.backupTimeSinceLastSuccess.Delete(labels)
			m.backupLatestSnapshotSize.Delete(labels)
		}
	}
	m.previousBackupLabels = currentBackupLabels // Update for next run

	// Aggregate backup metrics
	m.backupsRunningTotal.Set(float64(runningCount))
	m.backupsQueuedTotal.Set(float64(queuedCount))
	m.backupsLastRunFailedTotal.Set(float64(failedCount))
	m.backupsLastRunSuccessTotal.Set(float64(successCount))

	// Collect target metrics
	targets, err := storeInstance.Database.GetAllTargets()
	if err != nil {
		syslog.L.Error(err).
			WithField("handler", "prometheus_metrics").
			WithMessage("failed to get targets").Write()
		return
	}

	m.targetsTotal.Set(float64(len(targets)))

	var agentCount, s3Count, localCount int

	for _, target := range targets {
		driveLabels := prometheus.Labels{
			"target_name": target.Name,
			"volume_name": target.VolumeName,
			"volume_type": target.VolumeType,
			"volume_fs":   target.VolumeFS,
			"os":          target.AgentHost.OperatingSystem,
		}

		currentTargetLabels[target.Name+"-"+target.VolumeName] = driveLabels

		// Volume capacity metrics
		if target.VolumeTotalBytes > 0 {
			m.targetVolumeTotalBytes.With(driveLabels).Set(float64(target.VolumeTotalBytes))
		}
		if target.VolumeUsedBytes > 0 {
			m.targetVolumeUsedBytes.With(driveLabels).Set(float64(target.VolumeUsedBytes))
		}
		if target.VolumeFreeBytes > 0 {
			m.targetVolumeFreeBytes.With(driveLabels).Set(float64(target.VolumeFreeBytes))
		}

		// Calculate usage percentage
		if target.VolumeTotalBytes > 0 {
			usagePercent := float64(target.VolumeUsedBytes) / float64(target.VolumeTotalBytes) * 100
			m.targetVolumeUsagePercent.With(driveLabels).Set(usagePercent)
		}

		// Backup count
		backupCountLabels := prometheus.Labels{
			"target_name": target.Name,
		}
		m.targetBackupCount.With(backupCountLabels).Set(float64(target.JobCount))

		// Target-specific backup statistics
		if stats, exists := targetStats[target.Name]; exists {
			if stats.lastSuccessfulTimestamp > 0 {
				m.targetLastSuccessfulBackupTimestamp.With(backupCountLabels).Set(float64(stats.lastSuccessfulTimestamp))
				m.targetTimeSinceLastSuccessfulBackup.With(backupCountLabels).Set(float64(now - stats.lastSuccessfulTimestamp))
			}

			m.targetFailedBackupCount.With(backupCountLabels).Set(float64(stats.failedCount))
			m.targetSuccessfulBackupCount.With(backupCountLabels).Set(float64(stats.successfulCount))

			hasFailedBackups := float64(0)
			if stats.failedCount > 0 {
				hasFailedBackups = 1
			}
			m.targetHasFailedBackups.With(backupCountLabels).Set(hasFailedBackups)
		} else {
			// Target has no backups, set defaults
			m.targetFailedBackupCount.With(backupCountLabels).Set(0)
			m.targetSuccessfulBackupCount.With(backupCountLabels).Set(0)
			m.targetHasFailedBackups.With(backupCountLabels).Set(0)
		}

		// Target info (metadata)
		isAgent := "false"
		if target.IsAgent() {
			isAgent = "true"
			agentCount++
		}
		isS3 := "false"
		if target.IsS3() {
			isS3 = "true"
			s3Count++
		}
		if target.IsLocal() {
			localCount++
		}

		infoLabels := prometheus.Labels{
			"target_name": target.Name,
			"path":        target.Path,
			"volume_type": target.VolumeType,
			"volume_name": target.VolumeName,
			"volume_fs":   target.VolumeFS,
			"os":          target.AgentHost.OperatingSystem,
			"is_agent":    isAgent,
			"is_s3":       isS3,
		}

		currentTargetInfoLabels[target.Name+"-"+target.VolumeName] = infoLabels
		m.targetInfo.With(infoLabels).Set(1)
	}

	// Delete metrics for targets that no longer exist
	for targetKey, labels := range m.previousTargetLabels {
		if _, exists := currentTargetLabels[targetKey]; !exists {
			m.targetVolumeTotalBytes.Delete(labels)
			m.targetVolumeUsedBytes.Delete(labels)
			m.targetVolumeFreeBytes.Delete(labels)
			m.targetVolumeUsagePercent.Delete(labels)

			if infoLabels, exists := m.previousTargetInfoLabels[targetKey]; exists {
				m.targetInfo.Delete(infoLabels)
			}

			targetOnlyLabels := prometheus.Labels{"target_name": labels["target_name"]}

			m.targetBackupCount.Delete(targetOnlyLabels)
			m.targetLastSuccessfulBackupTimestamp.Delete(targetOnlyLabels)
			m.targetTimeSinceLastSuccessfulBackup.Delete(targetOnlyLabels)
			m.targetHasFailedBackups.Delete(targetOnlyLabels)
			m.targetFailedBackupCount.Delete(targetOnlyLabels)
			m.targetSuccessfulBackupCount.Delete(targetOnlyLabels)
		}
	}

	m.previousTargetLabels = currentTargetLabels // Update for next run
	m.previousTargetInfoLabels = currentTargetInfoLabels

	// Aggregate target type metrics
	m.targetsAgentTotal.Set(float64(agentCount))
	m.targetsS3Total.Set(float64(s3Count))
	m.targetsLocalTotal.Set(float64(localCount))
}
