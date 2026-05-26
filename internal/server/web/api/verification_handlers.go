//go:build linux

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

type VerificationJobsResponse struct {
	Data   []database.VerificationJob `json:"data"`
	Digest string                     `json:"digest"`
}

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

type VerificationResultsResponse struct {
	Errors  map[string]string             `json:"errors"`
	Message string                        `json:"message"`
	Data    []database.VerificationResult `json:"data"`
	Status  int                           `json:"status"`
	Success bool                          `json:"success"`
}

// D2DVerificationHandler handles listing verification jobs.
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

		digest, err := calculateDigest(jobs)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		toReturn := VerificationJobsResponse{
			Data:   jobs,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

// ExtJsVerificationRunHandler handles running/stopping verification jobs.
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
					// TODO: implement stop via RPC
					continue
				}

				// Run the verification job
				vj, err := verification.NewVerificationJob(vJob, storeInstance, true)
				if err != nil {
					syslog.L.Error(err).WithField("verificationJobID", jobID).Write()
					continue
				}
				go func(id string) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
					defer cancel()

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
		json.NewEncoder(w).Encode(response)
	}
}

// ExtJsVerificationConfigHandler handles creating verification jobs.
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
			// Derive store and namespace from the backup job
			backup, err := storeInstance.Database.GetBackup(backupJobID)
			if err != nil {
				WriteErrorResponse(w, fmt.Errorf("failed to get backup job: %w", err))
				return
			}
			store = backup.Store
			namespace = backup.Namespace
		}

		job := database.VerificationJob{
			ID:            r.FormValue("id"),
			BackupJobID:   backupJobID,
			Store:         store,
			Namespace:     namespace,
			TargetMode:    targetMode,
			Recursive:     r.FormValue("recursive") == "true",
			Mode:          r.FormValue("mode"),
			Schedule:      r.FormValue("schedule"),
			Comment:       r.FormValue("comment"),
			Retry:         retry,
			RetryInterval: retryInterval,
			SpotConfig: database.SpotCheckConfig{
				SampleCount:      sampleCount,
				SamplingStrategy: r.FormValue("sampling_strategy"),
				UseLatest:        useLatest,
				DateFrom:         r.FormValue("date_from"),
				DateTo:           r.FormValue("date_to"),
				FailThreshold:    failThreshold,
			},
			RunOnBackupComplete: r.FormValue("run_on_backup_complete") == "true",
		}

		// Parse filters from form
		if filtersJSON := r.FormValue("filters"); filtersJSON != "" {
			if err := json.Unmarshal([]byte(filtersJSON), &job.SpotConfig.Filters); err != nil {
				syslog.L.Error(err).WithMessage("failed to parse filters JSON").Write()
			}
		}

		// Parse full spot_config JSON if provided (overrides individual fields)
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

		response := VerificationJobConfigResponse{
			Data:    job,
			Status:  http.StatusOK,
			Success: true,
		}
		json.NewEncoder(w).Encode(response)
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
				Data:    job,
			}
			json.NewEncoder(w).Encode(response)
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
				_ = json.Unmarshal([]byte(filtersJSON), &job.SpotConfig.Filters)
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

			response := VerificationJobConfigResponse{
				Data:    job,
				Status:  http.StatusOK,
				Success: true,
			}
			json.NewEncoder(w).Encode(response)
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
			json.NewEncoder(w).Encode(response)
			return
		}

		http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
	}
}

// ExtJsVerificationResultsHandler returns verification results for a job.
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

		response := VerificationResultsResponse{
			Status:  http.StatusOK,
			Success: true,
			Data:    results,
		}
		json.NewEncoder(w).Encode(response)
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
	fmt.Fprintln(w, "Job ID,Run ID,Snapshot,Status,Total Population,Sampled,Verified,Failed,Skipped,Confidence 95%,Confidence 99%,Started At,Completed At")
	for _, r := range results {
		startedAt := formatTimestamp(r.StartedAt)
		completedAt := formatTimestamp(r.CompletedAt)
		conf95, conf99 := computeConfidence(r.TotalPopulation, r.TotalFiles, r.FailedFiles)
		fmt.Fprintf(w, "%s,%d,%s,%s,%d,%d,%d,%d,%d,%.1f%%,%.1f%%,%s,%s\n",
			csvEscape(r.VerificationJobID),
			r.ID,
			csvEscape(r.Snapshot),
			r.Status,
			r.TotalPopulation,
			r.TotalFiles,
			r.VerifiedFiles,
			r.FailedFiles,
			r.SkippedFiles,
			conf95*100,
			conf99*100,
			startedAt,
			completedAt,
		)
	}
}

func writeDetailCSV(w http.ResponseWriter, results []database.VerificationResult) {
	fmt.Fprintln(w, "Job ID,Run ID,Snapshot,Total Population,Sample Size,File Path,File Size,Status,Message,Confidence 95%,Confidence 99%")
	for _, r := range results {
		conf95, conf99 := computeConfidence(r.TotalPopulation, r.TotalFiles, r.FailedFiles)
		for _, f := range r.Details {
			fmt.Fprintf(w, "%s,%d,%s,%d,%d,%s,%d,%s,%s,%.1f%%,%.1f%%\n",
				csvEscape(r.VerificationJobID),
				r.ID,
				csvEscape(r.Snapshot),
				r.TotalPopulation,
				r.TotalFiles,
				csvEscape(f.Path),
				f.Size,
				f.Status,
				csvEscape(f.Message),
				conf95*100,
				conf99*100,
			)
		}
	}
}

// computeConfidence returns the lower bound of the intact rate at 95% and 99% confidence.
// Uses the Rule of Three for zero-failure samples and the Wilson score interval otherwise.
func computeConfidence(population, sample, failures int) (float64, float64) {
	if sample <= 0 || failures >= sample {
		return 0, 0
	}

	n := float64(sample)
	N := float64(population)
	if N <= 0 || n > N {
		N = n
	}

	fHat := float64(failures) / n

	if failures == 0 {
		// Rule of Three with finite population correction
		fpc := math.Sqrt((N - n) / N)
		if fpc < 0 {
			fpc = 0
		}
		upper95 := 3.0 / n * fpc
		upper99 := 4.6 / n * fpc
		return clamp01(1 - upper95), clamp01(1 - upper99)
	}

	// Wilson score interval for non-zero failures
	pHat := 1 - fHat
	return wilsonLower(pHat, n, 1.96), wilsonLower(pHat, n, 2.576)
}

func wilsonLower(pHat, n, z float64) float64 {
	if n <= 0 {
		return 0
	}
	denom := 1 + z*z/n
	centre := pHat + z*z/(2*n)
	spread := z * math.Sqrt(pHat*(1-pHat)/n+z*z/(4*n*n))
	lower := (centre - spread) / denom
	return clamp01(lower)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
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
