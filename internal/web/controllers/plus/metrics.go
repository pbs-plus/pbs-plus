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
	targetVolumesTotal               prometheus.Gauge
	volumeTotalBytes                 *prometheus.GaugeVec
	volumeUsedBytes                  *prometheus.GaugeVec
	volumeFreeBytes                  *prometheus.GaugeVec
	volumeUsagePercent               *prometheus.GaugeVec
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
	previousJobLabels                map[string]prometheus.Labels
	previousVolumeLabels             map[string]prometheus.Labels
	previousTargetInfoLabels         map[string]prometheus.Labels
	previousTargetOnlyLabels         map[string]prometheus.Labels
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
		targetVolumesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pbsplus_target_volumes_total",
			Help: "Total number of volumes across all targets",
		}),
		volumeTotalBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_volume_total_bytes",
				Help: "Total capacity of a volume in bytes",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
		),
		volumeUsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_volume_used_bytes",
				Help: "Used capacity of a volume in bytes",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
		),
		volumeFreeBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_volume_free_bytes",
				Help: "Free capacity of a volume in bytes",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
		),
		volumeUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pbsplus_volume_usage_percent",
				Help: "Volume usage percentage (0-100)",
			},
			[]string{"target_name", "volume_name", "volume_type", "volume_fs", "os"},
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
			[]string{"target_name", "local_path", "auth", "os", "is_agent", "is_s3"},
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
		previousJobLabels:        make(map[string]prometheus.Labels),
		previousVolumeLabels:     make(map[string]prometheus.Labels),
		previousTargetInfoLabels: make(map[string]prometheus.Labels),
		previousTargetOnlyLabels: make(map[string]prometheus.Labels),
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
		m.targetVolumesTotal,
		m.volumeTotalBytes,
		m.volumeUsedBytes,
		m.volumeFreeBytes,
		m.volumeUsagePercent,
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

func PrometheusMetricsHandler(storeInstance *store.Store) http.HandlerFunc {
	handler := promhttp.HandlerFor(globalRegistry, promhttp.HandlerOpts{
		Registry: globalRegistry,
	})
	return func(w http.ResponseWriter, r *http.Request) {
		updateMetrics(globalMetrics, storeInstance, time.Now().Unix())
		handler.ServeHTTP(w, r)
	}
}

func updateMetrics(m *metrics, storeInstance *store.Store, now int64) {
	currentJobLabels := make(map[string]prometheus.Labels)
	currentVolumeLabels := make(map[string]prometheus.Labels)
	currentTargetInfoLabels := make(map[string]prometheus.Labels)
	currentTargetOnlyLabels := make(map[string]prometheus.Labels)

	jobs, err := storeInstance.Database.GetAllJobs()
	if err != nil {
		syslog.L.Error(err).
			WithField("handler", "prometheus_metrics").
			WithMessage("failed to get jobs").Write()
		return
	}

	m.jobsTotal.Set(float64(len(jobs)))

	var runningCount, queuedCount, failedCount, successCount int

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
		currentJobLabels[job.ID] = labels

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
			successValue = 0
			failedCount++
		}
		m.jobLastRunSuccess.With(labels).Set(successValue)

		stats := targetStats[job.Target]
		if isSuccess {
			stats.successfulCount++
			if job.LastSuccessfulEndtime > stats.lastSuccessfulTimestamp {
				stats.lastSuccessfulTimestamp = job.LastSuccessfulEndtime
			}
		} else if successValue == 0 {
			stats.failedCount++
		}
		targetStats[job.Target] = stats

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
		if job.Duration > 0 {
			m.jobDuration.With(labels).Set(float64(job.Duration))
		}

		isRunning := float64(0)
		if job.CurrentPID > 0 {
			isRunning = 1
			runningCount++
		}
		m.jobRunning.With(labels).Set(isRunning)

		isQueued := float64(0)
		if strings.Contains(job.LastRunUpid, "pbsplusgen-queue") {
			isQueued = 1
			queuedCount++
		}
		m.jobQueued.With(labels).Set(isQueued)

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

	for jobID, labels := range m.previousJobLabels {
		if _, exists := currentJobLabels[jobID]; !exists {
			m.jobLastRunSuccess.Delete(labels)
			m.jobLastRunTimestamp.Delete(labels)
			m.jobLastSuccessfulTimestamp.Delete(labels)
			m.jobNextRunTimestamp.Delete(labels)
			m.jobDuration.Delete(labels)
			m.jobRunning.Delete(labels)
			m.jobQueued.Delete(labels)
			m.jobExpectedSize.Delete(labels)
			m.jobCurrentBytesTotal.Delete(labels)
			m.jobCurrentBytesSpeed.Delete(labels)
			m.jobCurrentFilesSpeed.Delete(labels)
			m.jobCurrentFileCount.Delete(labels)
			m.jobCurrentFolderCount.Delete(labels)
			m.jobTimeSinceLastSuccess.Delete(labels)
			m.jobLatestSnapshotSize.Delete(labels)
		}
	}
	m.previousJobLabels = currentJobLabels

	m.jobsRunningTotal.Set(float64(runningCount))
	m.jobsQueuedTotal.Set(float64(queuedCount))
	m.jobsLastRunFailedTotal.Set(float64(failedCount))
	m.jobsLastRunSuccessTotal.Set(float64(successCount))

	targets, err := storeInstance.Database.GetAllTargets()
	if err != nil {
		syslog.L.Error(err).
			WithField("handler", "prometheus_metrics").
			WithMessage("failed to get targets").Write()
		return
	}

	m.targetsTotal.Set(float64(len(targets)))

	var agentCount, s3Count, localCount int
	totalVolumes := 0

	for _, target := range targets {
		isAgent := "false"
		if target.TargetType == "agent" {
			isAgent = "true"
			agentCount++
		} else if target.TargetType == "s3" {
			s3Count++
		} else {
			localCount++
		}

		infoLabels := prometheus.Labels{
			"target_name": target.Name,
			"local_path":  target.LocalPath,
			"auth":        target.Auth,
			"os":          target.OperatingSystem,
			"is_agent":    isAgent,
			"is_s3":       map[bool]string{true: "true", false: "false"}[target.TargetType == "s3"],
		}
		currentTargetOnlyLabels[target.Name] = prometheus.Labels{"target_name": target.Name}
		currentTargetInfoLabels[target.Name] = infoLabels
		m.targetInfo.With(infoLabels).Set(1)
		m.targetJobCount.With(prometheus.Labels{"target_name": target.Name}).Set(float64(target.JobCount))

		if stats, exists := targetStats[target.Name]; exists {
			if stats.lastSuccessfulTimestamp > 0 {
				m.targetLastSuccessfulJobTimestamp.
					With(prometheus.Labels{"target_name": target.Name}).
					Set(float64(stats.lastSuccessfulTimestamp))
				m.targetTimeSinceLastSuccessfulJob.
					With(prometheus.Labels{"target_name": target.Name}).
					Set(float64(now - stats.lastSuccessfulTimestamp))
			}
			m.targetFailedJobCount.
				With(prometheus.Labels{"target_name": target.Name}).
				Set(float64(stats.failedCount))
			m.targetSuccessfulJobCount.
				With(prometheus.Labels{"target_name": target.Name}).
				Set(float64(stats.successfulCount))
			hasFailed := 0.0
			if stats.failedCount > 0 {
				hasFailed = 1
			}
			m.targetHasFailedJobs.
				With(prometheus.Labels{"target_name": target.Name}).
				Set(hasFailed)
		} else {
			m.targetFailedJobCount.
				With(prometheus.Labels{"target_name": target.Name}).Set(0)
			m.targetSuccessfulJobCount.
				With(prometheus.Labels{"target_name": target.Name}).Set(0)
			m.targetHasFailedJobs.
				With(prometheus.Labels{"target_name": target.Name}).Set(0)
		}

		for _, v := range target.Volumes {
			volLabels := prometheus.Labels{
				"target_name": target.Name,
				"volume_name": v.VolumeName,
				"volume_type": v.MetaType,
				"volume_fs":   v.MetaFS,
				"os":          target.OperatingSystem,
			}
			currentVolumeLabels[target.Name+"|"+v.VolumeName] = volLabels

			if v.MetaTotalBytes > 0 {
				m.volumeTotalBytes.With(volLabels).Set(float64(v.MetaTotalBytes))
			}
			if v.MetaUsedBytes > 0 {
				m.volumeUsedBytes.With(volLabels).Set(float64(v.MetaUsedBytes))
			}
			if v.MetaFreeBytes > 0 {
				m.volumeFreeBytes.With(volLabels).Set(float64(v.MetaFreeBytes))
			}
			if v.MetaTotalBytes > 0 {
				usage := float64(v.MetaUsedBytes) / float64(v.MetaTotalBytes) * 100
				m.volumeUsagePercent.With(volLabels).Set(usage)
			}

			totalVolumes++
		}
	}

	m.targetVolumesTotal.Set(float64(totalVolumes))

	for key, labels := range m.previousVolumeLabels {
		if _, ok := currentVolumeLabels[key]; !ok {
			m.volumeTotalBytes.Delete(labels)
			m.volumeUsedBytes.Delete(labels)
			m.volumeFreeBytes.Delete(labels)
			m.volumeUsagePercent.Delete(labels)
		}
	}
	m.previousVolumeLabels = currentVolumeLabels

	for key, info := range m.previousTargetInfoLabels {
		if _, ok := currentTargetInfoLabels[key]; !ok {
			m.targetInfo.Delete(info)
		}
	}
	m.previousTargetInfoLabels = currentTargetInfoLabels

	for key, only := range m.previousTargetOnlyLabels {
		if _, ok := currentTargetOnlyLabels[key]; !ok {
			lbl := prometheus.Labels{"target_name": only["target_name"]}
			m.targetJobCount.Delete(lbl)
			m.targetLastSuccessfulJobTimestamp.Delete(lbl)
			m.targetTimeSinceLastSuccessfulJob.Delete(lbl)
			m.targetHasFailedJobs.Delete(lbl)
			m.targetFailedJobCount.Delete(lbl)
			m.targetSuccessfulJobCount.Delete(lbl)
		}
	}
	m.previousTargetOnlyLabels = currentTargetOnlyLabels

	m.targetsAgentTotal.Set(float64(agentCount))
	m.targetsS3Total.Set(float64(s3Count))
	m.targetsLocalTotal.Set(float64(localCount))
}
