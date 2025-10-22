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
	jobsTotal                  prometheus.Gauge
	jobLastRunSuccess          *prometheus.GaugeVec
	jobLastRunTimestamp        *prometheus.GaugeVec
	jobLastSuccessfulTimestamp *prometheus.GaugeVec
	jobNextRunTimestamp        *prometheus.GaugeVec
	jobDuration                *prometheus.GaugeVec
	jobRunning                 *prometheus.GaugeVec
	jobQueued                  *prometheus.GaugeVec
	jobExpectedSize            *prometheus.GaugeVec
	jobCurrentBytesTotal       *prometheus.GaugeVec
	jobCurrentBytesSpeed       *prometheus.GaugeVec
	jobCurrentFilesSpeed       *prometheus.GaugeVec
	jobCurrentFileCount        *prometheus.GaugeVec
	jobCurrentFolderCount      *prometheus.GaugeVec
	jobTimeSinceLastSuccess    *prometheus.GaugeVec
	jobLatestSnapshotSize      *prometheus.GaugeVec
	jobsRunningTotal           prometheus.Gauge
	jobsQueuedTotal            prometheus.Gauge
	jobsLastRunFailedTotal     prometheus.Gauge
	jobsLastRunSuccessTotal    prometheus.Gauge
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
	)

	return m
}

// PrometheusMetricsHandler returns an HTTP handler that exposes Prometheus metrics
func PrometheusMetricsHandler(storeInstance *store.Store) http.Handler {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		Registry: reg,
	})

	// Wrap with collector that updates metrics on each scrape
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		for _, job := range jobs {
			labels := prometheus.Labels{
				"job_id":   job.ID,
				"target":   job.Target,
				"store":    job.Store,
				"mode":     job.Mode,
				"schedule": job.Schedule,
			}

			// Last run success status
			successValue := float64(-1)
			if job.LastRunState == "OK" {
				successValue = 1
				successCount++
			} else if job.LastRunState != "" {
				successValue = 0
				failedCount++
			}
			m.jobLastRunSuccess.With(labels).Set(successValue)

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

		// Aggregate metrics
		m.jobsRunningTotal.Set(float64(runningCount))
		m.jobsQueuedTotal.Set(float64(queuedCount))
		m.jobsLastRunFailedTotal.Set(float64(failedCount))
		m.jobsLastRunSuccessTotal.Set(float64(successCount))

		handler.ServeHTTP(w, r)
	})
}
