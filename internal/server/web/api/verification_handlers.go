//go:build linux

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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

		backupJobID := r.FormValue("backup_job_id")
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

		job := database.VerificationJob{
			ID:            r.FormValue("id"),
			BackupJobID:   backupJobID,
			Store:         backup.Store,
			Namespace:     backup.Namespace,
			Mode:          r.FormValue("mode"),
			Schedule:      r.FormValue("schedule"),
			Comment:       r.FormValue("comment"),
			Retry:         retry,
			RetryInterval: retryInterval,
			SpotConfig: database.SpotCheckConfig{
				SampleCount: sampleCount,
				UseLatest:   useLatest,
				DateFrom:    r.FormValue("date_from"),
				DateTo:      r.FormValue("date_to"),
			},
		}

		// Parse filters from form
		if filtersJSON := r.FormValue("filters"); filtersJSON != "" {
			if err := json.Unmarshal([]byte(filtersJSON), &job.SpotConfig.Filters); err != nil {
				syslog.L.Error(err).WithMessage("failed to parse filters JSON").Write()
			}
		}

		if err := storeInstance.VerificationSvc.CreateVerificationJob(job); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		response := VerificationJobConfigResponse{
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
			if v := r.FormValue("store"); v != "" {
				job.Store = v
			}
			if v := r.FormValue("ns"); v != "" {
				job.Namespace = v
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

			if err := storeInstance.VerificationSvc.UpdateVerificationJob(job); err != nil {
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
