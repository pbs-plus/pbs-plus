//go:build linux

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/verification"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type VerificationJobConfigResponse struct {
	Errors  map[string]string        `json:"errors"`
	Message string                   `json:"message"`
	Data    database.VerificationJob `json:"data"`
	Status  int                      `json:"status"`
	Success bool                     `json:"success"`
}

type VerificationRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

func D2DVerificationHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		jobs, err := storeInstance.VerificationSvc.ListVerificationJobs()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		flatJobs := FlattenVerificationJobs(jobs)

		digest, err := calculateDigest(flatJobs)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		toReturn := map[string]any{
			"data":    flatJobs,
			"digest":  digest,
			"success": true,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func ExtJsVerificationRunHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		jobIDs := r.URL.Query()["job"]
		if len(jobIDs) == 0 {
			http.Error(w, "Missing job parameter(s)", http.StatusBadRequest)
			return
		}

		decodedJobIDs := []string{}
		for _, jobID := range jobIDs {
			decoded := validate.DecodePath(jobID)
			if err := validate.ValidateJobId(decoded); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			decodedJobIDs = append(decodedJobIDs, decoded)
		}

		stop := r.Method == http.MethodDelete

		go func() {
			for _, jobID := range decodedJobIDs {
				vJob, err := storeInstance.Database.GetVerificationJob(jobID)
				if err != nil {
					syslog.L.Error(err).WithField("verificationJobID", jobID).Write()
					continue
				}

				if stop {
					for _, jobID := range decodedJobIDs {
						if !verification.StopJob(jobID) {
							syslog.L.Warn().WithField("verificationJobID", jobID).WithMessage("job not running, cannot stop").Write()
						}
					}
					continue
				}

				vj, err := verification.NewVerificationJob(vJob, storeInstance, true)
				if err != nil {
					syslog.L.Error(err).WithField("verificationJobID", jobID).Write()
					continue
				}
				go func(id string) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
					defer cancel()

					verification.RegisterJob(id, cancel)
					defer verification.UnregisterJob(id)

					defer func() {
						if vj.Cleanup != nil {
							vj.Cleanup()
						}
					}()

					if vj.PreExec != nil {
						if err := vj.PreExec(ctx); err != nil {
							if vj.OnError != nil {
								vj.OnError(err)
							}
							return
						}
					}

					if err := vj.Execute(ctx); err != nil {
						if vj.OnError != nil {
							vj.OnError(err)
						}
						return
					}

					if vj.OnSuccess != nil {
						vj.OnSuccess()
					}
				}(jobID)
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		response := VerificationRunResponse{
			Status:  http.StatusOK,
			Success: true,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func ExtJsVerificationConfigHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if err := r.ParseForm(); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		retry, err := strconv.Atoi(r.FormValue("retry"))
		if err != nil {
			if r.FormValue("retry") == "" {
				retry = 0
			} else {
				WriteErrorResponse(w, err)
				return
			}
		}

		retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
		if err != nil {
			if r.FormValue("retry-interval") == "" {
				retryInterval = 1
			} else {
				WriteErrorResponse(w, err)
				return
			}
		}

		sampleCount := 10
		if sc := r.FormValue("sample_count"); sc != "" {
			if n, err := strconv.Atoi(sc); err == nil && n > 0 {
				sampleCount = n
			}
		}

		var sampleCountPercent float64
		if scp := r.FormValue("sample_count_percent"); scp != "" {
			if v, err := strconv.ParseFloat(scp, 64); err == nil && v > 0 {
				sampleCountPercent = v
			}
		}

		useLatest := r.FormValue("use_latest") == "true"

		var failThreshold int
		if ft := r.FormValue("fail_threshold"); ft != "" {
			if n, err := strconv.Atoi(ft); err == nil && n > 0 {
				failThreshold = n
			}
		}

		backupJobID := r.FormValue("backup_job_id")
		targetMode := r.FormValue("target_mode")
		if targetMode == "" {
			targetMode = "backup_job"
		}

		var store, namespace string

		if targetMode == "namespace" {
			store = r.FormValue("store")
			if store == "" {
				WriteErrorResponse(w, fmt.Errorf("store is required for namespace mode"))
				return
			}
			namespace = r.FormValue("ns")
		} else {
			if backupJobID == "" {
				WriteErrorResponse(w, fmt.Errorf("backup_job_id is required"))
				return
			}
			backup, err := storeInstance.Database.GetBackup(backupJobID)
			if err != nil {
				WriteErrorResponse(w, fmt.Errorf("failed to get backup job: %w", err))
				return
			}
			store = backup.Store
			namespace = backup.Namespace
		}

		job := database.VerificationJob{
			ID:               r.FormValue("id"),
			BackupJobID:      backupJobID,
			Store:            store,
			Namespace:        namespace,
			TargetMode:       targetMode,
			Recursive:        r.FormValue("recursive") == "true",
			Mode:             r.FormValue("mode"),
			Schedule:         r.FormValue("schedule"),
			Comment:          r.FormValue("comment"),
			NotificationMode: r.FormValue("notification-mode"),
			Retry:            retry,
			RetryInterval:    retryInterval,
			SpotConfig: database.SpotCheckConfig{
				SampleCount:        sampleCount,
				SampleCountPercent: sampleCountPercent,
				SamplingStrategy:   r.FormValue("sampling_strategy"),
				UseLatest:          useLatest,
				DateFrom:           r.FormValue("date_from"),
				DateTo:             r.FormValue("date_to"),
				FailThreshold:      failThreshold,
			},
			RunOnBackupComplete: r.FormValue("run_on_backup_complete") == "true",
		}

		if filtersJSON := r.FormValue("filters"); filtersJSON != "" {
			if err := json.Unmarshal([]byte(filtersJSON), &job.SpotConfig.Filters); err != nil {
				syslog.L.Error(err).WithMessage("failed to parse filters JSON").Write()
			}
		}

		if spotConfigJSON := r.FormValue("spot_config"); spotConfigJSON != "" {
			var sc database.SpotCheckConfig
			if err := json.Unmarshal([]byte(spotConfigJSON), &sc); err == nil {
				if sc.SampleCount > 0 {
					job.SpotConfig.SampleCount = sc.SampleCount
				}
				if sc.SamplingStrategy != "" {
					job.SpotConfig.SamplingStrategy = sc.SamplingStrategy
				}
				if sc.UseLatest {
					job.SpotConfig.UseLatest = true
				}
				if sc.DateFrom != "" {
					job.SpotConfig.DateFrom = sc.DateFrom
				}
				if sc.DateTo != "" {
					job.SpotConfig.DateTo = sc.DateTo
				}
				if len(sc.Filters) > 0 {
					job.SpotConfig.Filters = sc.Filters
				}

				if sc.FailThreshold > 0 {
					job.SpotConfig.FailThreshold = sc.FailThreshold
				}
			}
		}

		if err := storeInstance.VerificationSvc.CreateVerificationJob(job); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		ApplyJobBatchAssignment(storeInstance, "verification", job.ID, r.FormValue("notification-batch"))

		response := VerificationJobConfigResponse{
			Data:    job,
			Status:  http.StatusOK,
			Success: true,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

// ExtJsVerificationConfigSingleHandler handles GET/PUT/DELETE for a single verification job.
func ExtJsVerificationConfigSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			jobID := validate.DecodePath(r.PathValue("id"))
			if err := validate.ValidateJobId(jobID); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			job, err := storeInstance.VerificationSvc.GetVerificationJob(jobID)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response := VerificationJobConfigResponse{
				Status:  http.StatusOK,
				Success: true,
			}

			jobBytes, err := json.Marshal(job)
			if err != nil {
				syslog.L.Error(err).Write()
			}
			var jobMap map[string]any
			if err := json.Unmarshal(jobBytes, &jobMap); err != nil {
				syslog.L.Error(err).Write()
			}
			jobMap["notification-batch"] = GetJobBatchName(storeInstance, "verification", jobID)
			response.Data = job // keep struct for type compatibility

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"status":  http.StatusOK,
				"success": true,
				"data":    jobMap,
			}); err != nil {
				syslog.L.Error(err).Write()
			}
			return
		}

		if r.Method == http.MethodPut {
			jobID := validate.DecodePath(r.PathValue("id"))
			if err := validate.ValidateJobId(jobID); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			job, err := storeInstance.VerificationSvc.GetVerificationJob(jobID)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			if v := r.FormValue("backup_job_id"); v != "" {
				job.BackupJobID = v
			}
			if v := r.FormValue("target_mode"); v != "" {
				job.TargetMode = v
			}
			if r.FormValue("recursive") != "" {
				job.Recursive = r.FormValue("recursive") == "true"
			}
			// In namespace mode, store/ns come from the form; in backup_job mode, derive from backup job
			if job.TargetMode == "namespace" {
				if v := r.FormValue("store"); v != "" {
					job.Store = v
				}
				if v := r.FormValue("ns"); v != "" {
					job.Namespace = v
				}
			} else if job.BackupJobID != "" {
				backup, err := storeInstance.Database.GetBackup(job.BackupJobID)
				if err == nil {
					job.Store = backup.Store
					job.Namespace = backup.Namespace
				}
			}
			if v := r.FormValue("mode"); v != "" {
				job.Mode = v
			}
			if v := r.FormValue("schedule"); v != "" {
				job.Schedule = v
			}
			if v := r.FormValue("comment"); v != "" {
				job.Comment = v
			}
			if v := r.FormValue("notification-mode"); v != "" {
				job.NotificationMode = v
			}

			retry, err := strconv.Atoi(r.FormValue("retry"))
			if err == nil {
				job.Retry = retry
			}
			retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
			if err == nil {
				job.RetryInterval = retryInterval
			}

			if sc := r.FormValue("sample_count"); sc != "" {
				if n, err := strconv.Atoi(sc); err == nil && n > 0 {
					job.SpotConfig.SampleCount = n
				}
			}
			if scp := r.FormValue("sample_count_percent"); scp != "" {
				if v, err := strconv.ParseFloat(scp, 64); err == nil && v > 0 {
					job.SpotConfig.SampleCountPercent = v
				}
			}
			if ss := r.FormValue("sampling_strategy"); ss != "" {
				job.SpotConfig.SamplingStrategy = ss
			}
			if r.FormValue("use_latest") != "" {
				job.SpotConfig.UseLatest = r.FormValue("use_latest") == "true"
			}
			if v := r.FormValue("date_from"); v != "" {
				job.SpotConfig.DateFrom = v
			}
			if v := r.FormValue("date_to"); v != "" {
				job.SpotConfig.DateTo = v
			}
			if filtersJSON := r.FormValue("filters"); filtersJSON != "" {
				if err := json.Unmarshal([]byte(filtersJSON), &job.SpotConfig.Filters); err != nil {
					syslog.L.Error(err).Write()
				}
			}

			if v := r.FormValue("fail_threshold"); v != "" {
				if n, err := strconv.Atoi(v); err == nil {
					job.SpotConfig.FailThreshold = n
				}
			}
			if r.FormValue("run_on_backup_complete") != "" {
				job.RunOnBackupComplete = r.FormValue("run_on_backup_complete") == "true"
			}

			if err := storeInstance.VerificationSvc.UpdateVerificationJob(job); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			ApplyJobBatchAssignment(storeInstance, "verification", job.ID, r.FormValue("notification-batch"))

			response := VerificationJobConfigResponse{
				Data:    job,
				Status:  http.StatusOK,
				Success: true,
			}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				syslog.L.Error(err).Write()
			}
			return
		}

		if r.Method == http.MethodDelete {
			jobID := validate.DecodePath(r.PathValue("id"))
			if err := validate.ValidateJobId(jobID); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			if err := storeInstance.VerificationSvc.DeleteVerificationJob(jobID); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response := VerificationJobConfigResponse{
				Status:  http.StatusOK,
				Success: true,
			}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				syslog.L.Error(err).Write()
			}
			return
		}

		http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
	}
}

func VerificationAggregateHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		results, err := storeInstance.VerificationSvc.GetAllVerificationResults()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		jobs, err := storeInstance.VerificationSvc.ListVerificationJobs()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		agg := ComputeAggregate(results)
		agg.TotalJobs = len(jobs)

		response := map[string]any{
			"status":  http.StatusOK,
			"success": true,
			"data":    agg,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func ExtJsVerificationResultsHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		jobID := validate.DecodePath(r.PathValue("id"))
		if err := validate.ValidateJobId(jobID); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		results, err := storeInstance.VerificationSvc.GetVerificationResults(jobID)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		job, err := storeInstance.VerificationSvc.GetVerificationJob(jobID)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		flatResults := FlattenVerificationResults(results, job.Namespace)

		response := map[string]any{
			"status":  http.StatusOK,
			"success": true,
			"data":    flatResults,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func VerificationResultsExportHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		jobID := validate.DecodePath(r.PathValue("id"))
		if err := validate.ValidateJobId(jobID); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		results, err := storeInstance.VerificationSvc.GetVerificationResults(jobID)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		exportType := r.URL.Query().Get("type")
		if exportType == "" {
			exportType = "detail"
		}

		filename := fmt.Sprintf("verification-%s-%s.csv", jobID, time.Now().Format("20060102-150405"))
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

		switch exportType {
		case "summary":
			writeSummaryCSV(w, results)
		default:
			writeDetailCSV(w, results)
		}
	}
}

func writeSummaryCSV(w http.ResponseWriter, results []database.VerificationResult) {
	if _, err := fmt.Fprintln(w, "Job ID,Run ID,Snapshot,Status,Total Population,Sampled,Verified,Failed,Skipped,Confidence 95%,Confidence 99%,Started At,Completed At"); err != nil {
		syslog.L.Error(err).Write()
	}
	for _, r := range results {
		startedAt := formatTimestamp(r.StartedAt)
		completedAt := formatTimestamp(r.CompletedAt)
		conf := ComputeConfidence(r.TotalPopulation, r.TotalFiles, r.FailedFiles)
		if _, err := fmt.Fprintf(w, "%s,%d,%s,%s,%d,%d,%d,%d,%d,%.1f%%,%.1f%%,%s,%s\n",
			csvEscape(r.VerificationJobID),
			r.ID,
			csvEscape(r.Snapshot),
			r.Status,
			r.TotalPopulation,
			r.TotalFiles,
			r.VerifiedFiles,
			r.FailedFiles,
			r.SkippedFiles,
			conf.Confidence95,
			conf.Confidence99,
			startedAt,
			completedAt,
		); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func writeDetailCSV(w http.ResponseWriter, results []database.VerificationResult) {
	if _, err := fmt.Fprintln(w, "Job ID,Run ID,Snapshot,Total Population,Sample Size,File Path,File Size,Status,Message,Confidence 95%,Confidence 99%"); err != nil {
		syslog.L.Error(err).Write()
	}
	for _, r := range results {
		conf := ComputeConfidence(r.TotalPopulation, r.TotalFiles, r.FailedFiles)
		for _, f := range r.Details {
			if _, err := fmt.Fprintf(w, "%s,%d,%s,%d,%d,%s,%d,%s,%s,%.1f%%,%.1f%%\n",
				csvEscape(r.VerificationJobID),
				r.ID,
				csvEscape(r.Snapshot),
				r.TotalPopulation,
				r.TotalFiles,
				csvEscape(f.Path),
				f.Size,
				f.Status,
				csvEscape(f.Message),
				conf.Confidence95,
				conf.Confidence99,
			); err != nil {
				syslog.L.Error(err).Write()
			}
		}
	}
}

func formatTimestamp(unix int64) string {
	if unix <= 0 {
		return ""
	}
	return time.Unix(unix, 0).UTC().Format(time.RFC3339)
}

func csvEscape(s string) string {
	if strings.ContainsAny(s, ",\"\n\r") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}
