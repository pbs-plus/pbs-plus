//go:build linux

package plus

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// PrometheusMetricsHandler returns an HTTP handler that exposes Prometheus metrics
func PrometheusMetricsHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobs, err := storeInstance.Database.GetAllJobs()
		if err != nil {
			syslog.L.Error(fmt.Errorf("PrometheusMetricsHandler: failed to get jobs: %w", err)).Write()
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		var metrics strings.Builder

		// Job count metrics
		metrics.WriteString("# HELP pbsplus_jobs_total Total number of backup jobs\n")
		metrics.WriteString("# TYPE pbsplus_jobs_total gauge\n")
		metrics.WriteString(fmt.Sprintf("pbsplus_jobs_total %d\n", len(jobs)))

		// Job state metrics
		metrics.WriteString("# HELP pbsplus_job_last_run_success Last run success status (1=success, 0=failure, -1=unknown)\n")
		metrics.WriteString("# TYPE pbsplus_job_last_run_success gauge\n")

		metrics.WriteString("# HELP pbsplus_job_last_run_timestamp_seconds Timestamp of the last job run\n")
		metrics.WriteString("# TYPE pbsplus_job_last_run_timestamp_seconds gauge\n")

		metrics.WriteString("# HELP pbsplus_job_last_successful_timestamp_seconds Timestamp of the last successful job run\n")
		metrics.WriteString("# TYPE pbsplus_job_last_successful_timestamp_seconds gauge\n")

		metrics.WriteString("# HELP pbsplus_job_next_run_timestamp_seconds Timestamp of the next scheduled job run\n")
		metrics.WriteString("# TYPE pbsplus_job_next_run_timestamp_seconds gauge\n")

		metrics.WriteString("# HELP pbsplus_job_duration_seconds Duration of the last job run in seconds\n")
		metrics.WriteString("# TYPE pbsplus_job_duration_seconds gauge\n")

		metrics.WriteString("# HELP pbsplus_job_running Job currently running (1=running, 0=not running)\n")
		metrics.WriteString("# TYPE pbsplus_job_running gauge\n")

		metrics.WriteString("# HELP pbsplus_job_expected_size_bytes Expected size of the backup in bytes\n")
		metrics.WriteString("# TYPE pbsplus_job_expected_size_bytes gauge\n")

		metrics.WriteString("# HELP pbsplus_job_current_bytes_total Current bytes transferred during active backup\n")
		metrics.WriteString("# TYPE pbsplus_job_current_bytes_total gauge\n")

		metrics.WriteString("# HELP pbsplus_job_current_bytes_speed Current transfer speed in bytes per second\n")
		metrics.WriteString("# TYPE pbsplus_job_current_bytes_speed gauge\n")

		metrics.WriteString("# HELP pbsplus_job_current_files_speed Current file processing speed in files per second\n")
		metrics.WriteString("# TYPE pbsplus_job_current_files_speed gauge\n")

		metrics.WriteString("# HELP pbsplus_job_current_file_count Current number of files processed\n")
		metrics.WriteString("# TYPE pbsplus_job_current_file_count gauge\n")

		metrics.WriteString("# HELP pbsplus_job_current_folder_count Current number of folders processed\n")
		metrics.WriteString("# TYPE pbsplus_job_current_folder_count gauge\n")

		metrics.WriteString("# HELP pbsplus_job_queued Job is in queue (1=queued, 0=not queued)\n")
		metrics.WriteString("# TYPE pbsplus_job_queued gauge\n")

		metrics.WriteString("# HELP pbsplus_job_time_since_last_success_seconds Time since last successful run in seconds\n")
		metrics.WriteString("# TYPE pbsplus_job_time_since_last_success_seconds gauge\n")

		metrics.WriteString("# HELP pbsplus_job_latest_snapshot_size_bytes Size of the latest snapshot in bytes\n")
		metrics.WriteString("# TYPE pbsplus_job_latest_snapshot_size_bytes gauge\n")

		now := time.Now().Unix()

		for _, job := range jobs {
			labels := fmt.Sprintf(
				`job_id="%s",target="%s",store="%s",mode="%s",schedule="%s"`,
				sanitizeLabel(job.ID),
				sanitizeLabel(job.Target),
				sanitizeLabel(job.Store),
				sanitizeLabel(job.Mode),
				sanitizeLabel(job.Schedule),
			)

			// Last run success status
			successValue := -1
			if job.LastRunState == "OK" {
				successValue = 1
			} else if job.LastRunState != "" {
				successValue = 0
			}
			metrics.WriteString(fmt.Sprintf("pbsplus_job_last_run_success{%s} %d\n", labels, successValue))

			// Last run timestamp
			if job.LastRunEndtime > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_last_run_timestamp_seconds{%s} %d\n", labels, job.LastRunEndtime))
			}

			// Last successful run timestamp
			if job.LastSuccessfulEndtime > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_last_successful_timestamp_seconds{%s} %d\n", labels, job.LastSuccessfulEndtime))

				// Time since last success
				timeSinceSuccess := now - job.LastSuccessfulEndtime
				metrics.WriteString(fmt.Sprintf("pbsplus_job_time_since_last_success_seconds{%s} %d\n", labels, timeSinceSuccess))
			}

			// Next run timestamp
			if job.NextRun > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_next_run_timestamp_seconds{%s} %d\n", labels, job.NextRun))
			}

			// Duration
			if job.Duration > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_duration_seconds{%s} %d\n", labels, job.Duration))
			}

			// Running status
			isRunning := 0
			if job.CurrentPID > 0 {
				isRunning = 1
			}
			metrics.WriteString(fmt.Sprintf("pbsplus_job_running{%s} %d\n", labels, isRunning))

			// Queued status
			isQueued := 0
			if strings.Contains(job.LastRunUpid, "pbsplusgen-queue") {
				isQueued = 1
			}
			metrics.WriteString(fmt.Sprintf("pbsplus_job_queued{%s} %d\n", labels, isQueued))

			// Expected size
			if job.ExpectedSize > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_expected_size_bytes{%s} %d\n", labels, job.ExpectedSize))
			}

			// Current transfer metrics (for running jobs)
			if job.CurrentBytesTotal > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_current_bytes_total{%s} %d\n", labels, job.CurrentBytesTotal))
			}
			if job.CurrentBytesSpeed > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_current_bytes_speed{%s} %d\n", labels, job.CurrentBytesSpeed))
			}
			if job.CurrentFilesSpeed > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_current_files_speed{%s} %d\n", labels, job.CurrentFilesSpeed))
			}
			if job.CurrentFileCount > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_current_file_count{%s} %d\n", labels, job.CurrentFileCount))
			}
			if job.CurrentFolderCount > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_current_folder_count{%s} %d\n", labels, job.CurrentFolderCount))
			}
			if job.LatestSnapshotSize > 0 {
				metrics.WriteString(fmt.Sprintf("pbsplus_job_latest_snapshot_size_bytes{%s} %d\n", labels, job.LatestSnapshotSize))
			}
		}

		// Aggregate metrics
		var runningCount, queuedCount, failedCount, successCount int
		for _, job := range jobs {
			if job.CurrentPID > 0 {
				runningCount++
			}
			if strings.Contains(job.LastRunUpid, "pbsplusgen-queue") {
				queuedCount++
			}
			if job.LastRunState == "OK" {
				successCount++
			} else if job.LastRunState != "" && job.LastRunState != "OK" {
				failedCount++
			}
		}

		metrics.WriteString("# HELP pbsplus_jobs_running_total Number of currently running jobs\n")
		metrics.WriteString("# TYPE pbsplus_jobs_running_total gauge\n")
		metrics.WriteString(fmt.Sprintf("pbsplus_jobs_running_total %d\n", runningCount))

		metrics.WriteString("# HELP pbsplus_jobs_queued_total Number of queued jobs\n")
		metrics.WriteString("# TYPE pbsplus_jobs_queued_total gauge\n")
		metrics.WriteString(fmt.Sprintf("pbsplus_jobs_queued_total %d\n", queuedCount))

		metrics.WriteString("# HELP pbsplus_jobs_last_run_failed_total Number of jobs that failed on last run\n")
		metrics.WriteString("# TYPE pbsplus_jobs_last_run_failed_total gauge\n")
		metrics.WriteString(fmt.Sprintf("pbsplus_jobs_last_run_failed_total %d\n", failedCount))

		metrics.WriteString("# HELP pbsplus_jobs_last_run_success_total Number of jobs that succeeded on last run\n")
		metrics.WriteString("# TYPE pbsplus_jobs_last_run_success_total gauge\n")
		metrics.WriteString(fmt.Sprintf("pbsplus_jobs_last_run_success_total %d\n", successCount))

		w.Write([]byte(metrics.String()))
	}
}

// sanitizeLabel sanitizes label values for Prometheus
func sanitizeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}
