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

type metrics struct {
	jobsTotal                        prometheus.Gauge
	jobLastRunSuccess                *prometheus.GaugeVec
	jobLastRunTimestamp              *prometheus.GaugeVec
	jobLastSuccessfulTimestamp       *prometheus.GaugeVec
	jobNextRunTimestamp              *prometheus.GaugeVec
	jobDuration                      *prometheus.GaugeVec
	jobRunning                       *prometheus.GaugeVec
	jobQueued                        *prometheus.GaugeVec
	jobExpectedSize                  *prometheus.GaugeVec
	jobCurrentBytesTotal             *prometheus.GaugeVec
	jobCurrentBytesSpeed             *prometheus.GaugeVec
	jobCurrentFilesSpeed             *prometheus.GaugeVec
	jobCurrentFileCount              *prometheus.GaugeVec
	jobCurrentFolderCount            *prometheus.GaugeVec
	jobTimeSinceLastSuccess          *prometheus.GaugeVec
	jobLatestSnapshotSize            *prometheus.GaugeVec
	jobsRunningTotal                 prometheus.Gauge
	jobsQueuedTotal                  prometheus.Gauge
	jobsLastRunFailedTotal           prometheus.Gauge
	jobsLastRunSuccessTotal          prometheus.Gauge
	targetsTotal                     prometheus.Gauge
	targetDriveTotalBytes            *prometheus.GaugeVec
	targetDriveUsedBytes             *prometheus.GaugeVec
	targetDriveFreeBytes             *prometheus.GaugeVec
	targetDriveUsagePercent          *prometheus.GaugeVec
	targetJobCount                   *prometheus.GaugeVec
	targetInfo                       *prometheus.GaugeVec
	targetsAgentTotal                prometheus.Gauge
	targetsS3Total                   prometheus.Gauge
	targetsLocalTotal                prometheus.Gauge
	targetLastSuccessfulJobTimestamp *prometheus.GaugeVec
	targetTimeSinceLastSuccessfulJob *prometheus.GaugeVec
	targetHasFailedJobs              *prometheus.GaugeVec
	targetFailedJobCount             *prometheus.GaugeVec
	targetSuccessfulJobCount         *prometheus.GaugeVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		jobsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_jobs_total",
			Help: "Total number of backup jobs",
		}),
		jobLastRunSuccess: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_last_run_success",
				Help: "Last run success status (1=success, 0=failure, -1=unknown)",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobLastRunTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_last_run_timestamp_seconds",
				Help: "Timestamp of the last job run",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobLastSuccessfulTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_last_successful_timestamp_seconds",
				Help: "Timestamp of the last successful job run",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobNextRunTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_next_run_timestamp_seconds",
				Help: "Timestamp of the next scheduled job run",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_duration_seconds",
				Help: "Duration of the last job run in seconds",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_running",
				Help: "Job currently running (1=running, 0=not running)",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobQueued: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_queued",
				Help: "Job is in queue (1=queued, 0=not queued)",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobExpectedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_expected_size_bytes",
				Help: "Expected size of the backup in bytes",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobCurrentBytesTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_current_bytes_total",
				Help: "Current bytes transferred during active backup",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobCurrentBytesSpeed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_current_bytes_speed",
				Help: "Current transfer speed in bytes per second",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobCurrentFilesSpeed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_current_files_speed",
				Help: "Current file processing speed in files per second",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobCurrentFileCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_current_file_count",
				Help: "Current number of files processed",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobCurrentFolderCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_current_folder_count",
				Help: "Current number of folders processed",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobTimeSinceLastSuccess: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_time_since_last_success_seconds",
				Help: "Time since last successful run in seconds",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobLatestSnapshotSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_job_latest_snapshot_size_bytes",
				Help: "Size of the latest snapshot in bytes",
			},
			[]string{"job_id", "target", "store", "mode", "schedule"},
		),
		jobsRunningTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_jobs_running_total",
			Help: "Number of currently running jobs",
		}),
		jobsQueuedTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_jobs_queued_total",
			Help: "Number of queued jobs",
		}),
		jobsLastRunFailedTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_jobs_last_run_failed_total",
			Help: "Number of jobs that failed on last run",
		}),
		jobsLastRunSuccessTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_jobs_last_run_success_total",
			Help: "Number of jobs that succeeded on last run",
		}),
		targetsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_targets_total",
			Help: "Total number of backup targets",
		}),
		targetDriveTotalBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_drive_total_bytes",
				Help: "Total capacity of target drive in bytes",
			},
			[]string{"target_name", "drive_name", "drive_type", "drive_fs", "os"},
		),
		targetDriveUsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_drive_used_bytes",
				Help: "Used capacity of target drive in bytes",
			},
			[]string{"target_name", "drive_name", "drive_type", "drive_fs", "os"},
		),
		targetDriveFreeBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_drive_free_bytes",
				Help: "Free capacity of target drive in bytes",
			},
			[]string{"target_name", "drive_name", "drive_type", "drive_fs", "os"},
		),
		targetDriveUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_drive_usage_percent",
				Help: "Drive usage percentage (0-100)",
			},
			[]string{"target_name", "drive_name", "drive_type", "drive_fs", "os"},
		),
		targetJobCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_job_count",
				Help: "Number of jobs associated with this target",
			},
			[]string{"target_name"},
		),
		targetInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_info",
				Help: "Target metadata (always 1, use labels for info)",
			},
			[]string{"target_name", "path", "auth", "drive_type", "drive_name", "drive_fs", "os", "is_agent", "is_s3"},
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
		targetLastSuccessfulJobTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_last_successful_job_timestamp_seconds",
				Help: "Timestamp of the most recent successful job for this target",
			},
			[]string{"target_name"},
		),
		targetTimeSinceLastSuccessfulJob: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_time_since_last_successful_job_seconds",
				Help: "Time since the most recent successful job for this target in seconds",
			},
			[]string{"target_name"},
		),
		targetHasFailedJobs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_has_failed_jobs",
				Help: "Target has at least one failed job (1=yes, 0=no)",
			},
			[]string{"target_name"},
		),
		targetFailedJobCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_failed_job_count",
				Help: "Number of jobs that failed on last run for this target",
			},
			[]string{"target_name"},
		),
		targetSuccessfulJobCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_target_successful_job_count",
				Help: "Number of jobs that succeeded on last run for this target",
			},
			[]string{"target_name"},
		),
	}

	reg.MustRegister(
		m.jobsTotal,
		m.jobLastRunSuccess,
		m.jobLastRunTimestamp,
		m.jobLastSuccessfulTimestamp,
		m.jobNextRunTimestamp,
		m.jobDuration,
		m.jobRunning,
		m.jobQueued,
		m.jobExpectedSize,
		m.jobCurrentBytesTotal,
		m.jobCurrentBytesSpeed,
		m.jobCurrentFilesSpeed,
		m.jobCurrentFileCount,
		m.jobCurrentFolderCount,
		m.jobTimeSinceLastSuccess,
		m.jobLatestSnapshotSize,
		m.jobsRunningTotal,
		m.jobsQueuedTotal,
		m.jobsLastRunFailedTotal,
		m.jobsLastRunSuccessTotal,
		m.targetsTotal,
		m.targetDriveTotalBytes,
		m.targetDriveUsedBytes,
		m.targetDriveFreeBytes,
		m.targetDriveUsagePercent,
		m.targetJobCount,
		m.targetInfo,
		m.targetsAgentTotal,
		m.targetsS3Total,
		m.targetsLocalTotal,
		m.targetLastSuccessfulJobTimestamp,
		m.targetTimeSinceLastSuccessfulJob,
		m.targetHasFailedJobs,
		m.targetFailedJobCount,
		m.targetSuccessfulJobCount,
	)

	return m
}

// PrometheusMetricsHandler returns an HTTP handler that exposes Prometheus metrics
func PrometheusMetricsHandler(storeInstance *store.Store) http.HandlerFunc {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		Registry: reg,
	})

	// Wrap with collector that updates metrics on each scrape
	return func(w http.ResponseWriter, r *http.Request) {
		// Collect job metrics
		jobs, err := storeInstance.Database.GetAllJobs()
		if err != nil {
			syslog.L.Error(err).
				WithField("handler", "prometheus_metrics").
				WithMessage("failed to get jobs").Write()
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		m.jobsTotal.Set(float64(len(jobs)))

		now := time.Now().Unix()
		var runningCount, queuedCount, failedCount, successCount int

		// Track per-target statistics
		targetStats := make(map[string]struct {
			lastSuccessfulTimestamp int64
			failedCount             int
			successfulCount         int
		})

		for _, job := range jobs {
			labels := prometheus.Labels{
				"job_id":   job.ID,
				"target":   job.Target,
				"store":    job.Store,
				"mode":     job.Mode,
				"schedule": job.Schedule,
			}

			// Last run success status
			// Consider successful if LastRunEndtime == LastSuccessfulEndtime OR if LastRunState == "OK"
			successValue := float64(-1)
			isSuccess := false
			if job.LastRunEndtime > 0 && job.LastSuccessfulEndtime > 0 && job.LastRunEndtime == job.LastSuccessfulEndtime {
				successValue = 1
				successCount++
				isSuccess = true
			} else if job.LastRunState == "OK" {
				successValue = 1
				successCount++
				isSuccess = true
			} else if job.LastRunEndtime > 0 || job.LastRunState != "" {
				// If we have a last run but it's not successful, mark as failed
				successValue = 0
				failedCount++
			}
			m.jobLastRunSuccess.With(labels).Set(successValue)

			// Update target statistics
			stats := targetStats[job.Target]
			if isSuccess {
				stats.successfulCount++
				// Track the most recent successful timestamp for this target
				if job.LastSuccessfulEndtime > stats.lastSuccessfulTimestamp {
					stats.lastSuccessfulTimestamp = job.LastSuccessfulEndtime
				}
			} else if successValue == 0 {
				stats.failedCount++
			}
			targetStats[job.Target] = stats

			// Timestamps
			if job.LastRunEndtime > 0 {
				m.jobLastRunTimestamp.With(labels).Set(float64(job.LastRunEndtime))
			}
			if job.LastSuccessfulEndtime > 0 {
				m.jobLastSuccessfulTimestamp.With(labels).Set(float64(job.LastSuccessfulEndtime))
				m.jobTimeSinceLastSuccess.With(labels).Set(float64(now - job.LastSuccessfulEndtime))
			}
			if job.NextRun > 0 {
				m.jobNextRunTimestamp.With(labels).Set(float64(job.NextRun))
			}

			// Duration
			if job.Duration > 0 {
				m.jobDuration.With(labels).Set(float64(job.Duration))
			}

			// Running status
			isRunning := float64(0)
			if job.CurrentPID > 0 {
				isRunning = 1
				runningCount++
			}
			m.jobRunning.With(labels).Set(isRunning)

			// Queued status
			isQueued := float64(0)
			if strings.Contains(job.LastRunUpid, "pbsplusgen-queue") {
				isQueued = 1
				queuedCount++
			}
			m.jobQueued.With(labels).Set(isQueued)

			// Size metrics
			if job.ExpectedSize > 0 {
				m.jobExpectedSize.With(labels).Set(float64(job.ExpectedSize))
			}
			if job.CurrentBytesTotal > 0 {
				m.jobCurrentBytesTotal.With(labels).Set(float64(job.CurrentBytesTotal))
			}
			if job.CurrentBytesSpeed > 0 {
				m.jobCurrentBytesSpeed.With(labels).Set(float64(job.CurrentBytesSpeed))
			}
			if job.CurrentFilesSpeed > 0 {
				m.jobCurrentFilesSpeed.With(labels).Set(float64(job.CurrentFilesSpeed))
			}
			if job.CurrentFileCount > 0 {
				m.jobCurrentFileCount.With(labels).Set(float64(job.CurrentFileCount))
			}
			if job.CurrentFolderCount > 0 {
				m.jobCurrentFolderCount.With(labels).Set(float64(job.CurrentFolderCount))
			}
			if job.LatestSnapshotSize > 0 {
				m.jobLatestSnapshotSize.With(labels).Set(float64(job.LatestSnapshotSize))
			}
		}

		// Aggregate job metrics
		m.jobsRunningTotal.Set(float64(runningCount))
		m.jobsQueuedTotal.Set(float64(queuedCount))
		m.jobsLastRunFailedTotal.Set(float64(failedCount))
		m.jobsLastRunSuccessTotal.Set(float64(successCount))

		// Collect target metrics
		targets, err := storeInstance.Database.GetAllTargets()
		if err != nil {
			syslog.L.Error(err).
				WithField("handler", "prometheus_metrics").
				WithMessage("failed to get targets").Write()
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		m.targetsTotal.Set(float64(len(targets)))

		var agentCount, s3Count, localCount int

		for _, target := range targets {
			driveLabels := prometheus.Labels{
				"target_name": target.Name,
				"drive_name":  target.DriveName,
				"drive_type":  target.DriveType,
				"drive_fs":    target.DriveFS,
				"os":          target.OperatingSystem,
			}

			// Drive capacity metrics
			if target.DriveTotalBytes > 0 {
				m.targetDriveTotalBytes.With(driveLabels).Set(float64(target.DriveTotalBytes))
			}
			if target.DriveUsedBytes > 0 {
				m.targetDriveUsedBytes.With(driveLabels).Set(float64(target.DriveUsedBytes))
			}
			if target.DriveFreeBytes > 0 {
				m.targetDriveFreeBytes.With(driveLabels).Set(float64(target.DriveFreeBytes))
			}

			// Calculate usage percentage
			if target.DriveTotalBytes > 0 {
				usagePercent := float64(target.DriveUsedBytes) / float64(target.DriveTotalBytes) * 100
				m.targetDriveUsagePercent.With(driveLabels).Set(usagePercent)
			}

			// Job count
			jobCountLabels := prometheus.Labels{
				"target_name": target.Name,
			}
			m.targetJobCount.With(jobCountLabels).Set(float64(target.JobCount))

			// Target-specific job statistics
			if stats, exists := targetStats[target.Name]; exists {
				if stats.lastSuccessfulTimestamp > 0 {
					m.targetLastSuccessfulJobTimestamp.With(jobCountLabels).Set(float64(stats.lastSuccessfulTimestamp))
					m.targetTimeSinceLastSuccessfulJob.With(jobCountLabels).Set(float64(now - stats.lastSuccessfulTimestamp))
				}

				m.targetFailedJobCount.With(jobCountLabels).Set(float64(stats.failedCount))
				m.targetSuccessfulJobCount.With(jobCountLabels).Set(float64(stats.successfulCount))

				hasFailedJobs := float64(0)
				if stats.failedCount > 0 {
					hasFailedJobs = 1
				}
				m.targetHasFailedJobs.With(jobCountLabels).Set(hasFailedJobs)
			} else {
				// Target has no jobs, set defaults
				m.targetFailedJobCount.With(jobCountLabels).Set(0)
				m.targetSuccessfulJobCount.With(jobCountLabels).Set(0)
				m.targetHasFailedJobs.With(jobCountLabels).Set(0)
			}

			// Target info (metadata)
			isAgent := "false"
			if target.IsAgent {
				isAgent = "true"
				agentCount++
			}
			isS3 := "false"
			if target.IsS3 {
				isS3 = "true"
				s3Count++
			}
			if !target.IsAgent && !target.IsS3 {
				localCount++
			}

			infoLabels := prometheus.Labels{
				"target_name": target.Name,
				"path":        target.Path,
				"auth":        target.Auth,
				"drive_type":  target.DriveType,
				"drive_name":  target.DriveName,
				"drive_fs":    target.DriveFS,
				"os":          target.OperatingSystem,
				"is_agent":    isAgent,
				"is_s3":       isS3,
			}
			m.targetInfo.With(infoLabels).Set(1)
		}

		// Aggregate target type metrics
		m.targetsAgentTotal.Set(float64(agentCount))
		m.targetsS3Total.Set(float64(s3Count))
		m.targetsLocalTotal.Set(float64(localCount))

		handler.ServeHTTP(w, r)
	}
}
