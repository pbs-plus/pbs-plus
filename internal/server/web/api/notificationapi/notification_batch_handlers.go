//go:build linux

package notificationapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/digest"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func NotificationBatchHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if name := r.URL.Query().Get("batch"); name != "" {
				getNotificationBatch(app, w, r, name)
			} else {
				listNotificationBatches(app, w, r)
			}
		case http.MethodPost:
			createNotificationBatch(app, w, r)
		case http.MethodPut:
			updateNotificationBatch(app, w, r)
		case http.MethodDelete:
			deleteNotificationBatch(app, w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func listNotificationBatches(app *application.Runtime, w http.ResponseWriter, r *http.Request) {
	batches, err := app.CoreDB.ListNotificationBatches()
	if err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	type batchWithCount struct {
		coredb.NotificationBatch
		JobCount int `json:"job-count"`
	}

	result := make([]batchWithCount, len(batches))
	for i, b := range batches {
		jobs, err := app.CoreDB.GetBatchJobs(b.Name)
		if err != nil {
			log.Error(err, "")
		}
		count := 0
		if jobs != nil {
			count = len(jobs)
		}
		result[i] = batchWithCount{
			NotificationBatch: b,
			JobCount:          count,
		}
	}

	digest, err := digest.Calculate(result)
	if err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    result,
		"digest":  digest,
	}); err != nil {
		log.Error(err, "")
	}
}

func getNotificationBatch(app *application.Runtime, w http.ResponseWriter, r *http.Request, name string) {
	batch, err := app.CoreDB.GetNotificationBatch(name)
	if err != nil || batch.Name == "" {
		http.Error(w, "Batch not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    batch,
	}); err != nil {
		log.Error(err, "")
	}
}

func createNotificationBatch(app *application.Runtime, w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Error(w, "Missing batch name", http.StatusBadRequest)
		return
	}
	if err := validate.ValidateJobId(name); err != nil {
		http.Error(w, "Invalid batch name: "+err.Error(), http.StatusBadRequest)
		return
	}

	existing, err := app.CoreDB.GetNotificationBatch(name)
	if err != nil {
		log.Error(err, "")
	}
	if existing.Name != "" {
		http.Error(w, "Batch already exists", http.StatusConflict)
		return
	}

	comment := r.FormValue("comment")
	mode := r.FormValue("notification-mode")
	timeoutSecs := formValueInt(r, "wait-timeout-secs", 300)
	sendOnTimeout := formValueBool(r, "send-on-timeout", true)

	batch := coredb.NotificationBatch{
		Name:             name,
		Comment:          comment,
		NotificationMode: mode,
		WaitTimeoutSecs:  timeoutSecs,
		SendOnTimeout:    sendOnTimeout,
	}

	if err := app.CoreDB.CreateNotificationBatch(batch); err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	if jobs := r.FormValue("jobs"); jobs != "" {
		var jobList []struct {
			JobType string `json:"job-type"`
			JobID   string `json:"job-id"`
		}
		if err := json.Unmarshal([]byte(jobs), &jobList); err == nil {
			for _, j := range jobList {
				if err := app.CoreDB.AddJobToBatch(name, j.JobType, j.JobID); err != nil {
					log.Error(err, "")
				}
			}
		}
	}

	created, err := app.CoreDB.GetNotificationBatch(name)
	if err != nil {
		log.Error(err, "")
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    created,
	}); err != nil {
		log.Error(err, "")
	}
}

func updateNotificationBatch(app *application.Runtime, w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("batch")
	if name == "" {
		http.Error(w, "Missing batch parameter", http.StatusBadRequest)
		return
	}

	existing, err := app.CoreDB.GetNotificationBatch(name)
	if err != nil || existing.Name == "" {
		http.Error(w, "Batch not found", http.StatusNotFound)
		return
	}

	if v := r.FormValue("comment"); v != "" || r.FormValue("delete") == "comment" {
		existing.Comment = v
	}
	if v := r.FormValue("notification-mode"); v != "" {
		existing.NotificationMode = v
	}
	if v := r.FormValue("wait-timeout-secs"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			existing.WaitTimeoutSecs = i
		}
	}
	if v := r.FormValue("send-on-timeout"); v != "" {
		existing.SendOnTimeout = v == "1" || v == "true"
	}

	if err := app.CoreDB.UpdateNotificationBatch(existing); err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	if jobs := r.FormValue("jobs"); jobs != "" {
		var jobList []struct {
			JobType string `json:"job-type"`
			JobID   string `json:"job-id"`
		}
		if err := json.Unmarshal([]byte(jobs), &jobList); err == nil {
			if err := app.CoreDB.RemoveJobsByBatch(name); err != nil {
				log.Error(err, "")
			}
			for _, j := range jobList {
				if err := app.CoreDB.AddJobToBatch(name, j.JobType, j.JobID); err != nil {
					log.Error(err, "")
				}
			}
		}
	}

	updated, err := app.CoreDB.GetNotificationBatch(name)
	if err != nil {
		log.Error(err, "")
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    updated,
	}); err != nil {
		log.Error(err, "")
	}
}

func deleteNotificationBatch(app *application.Runtime, w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("batch")
	if name == "" {
		http.Error(w, "Missing batch parameter", http.StatusBadRequest)
		return
	}

	if err := app.CoreDB.DeleteNotificationBatch(name); err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    nil,
	}); err != nil {
		log.Error(err, "")
	}
}

func NotificationBatchJobsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			listBatchJobs(app, w, r)
		case http.MethodPost:
			addBatchJob(app, w, r)
		case http.MethodDelete:
			removeBatchJob(app, w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func listBatchJobs(app *application.Runtime, w http.ResponseWriter, r *http.Request) {
	batchName := r.URL.Query().Get("batch")
	if batchName == "" {
		allJobs, err := app.CoreDB.ListBatchJobs()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"success": true, "data": allJobs}); err != nil {
			log.Error(err, "")
		}
		return
	}

	jobs, err := app.CoreDB.GetBatchJobs(batchName)
	if err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"success": true, "data": jobs}); err != nil {
		log.Error(err, "")
	}
}

func addBatchJob(app *application.Runtime, w http.ResponseWriter, r *http.Request) {
	batchName := r.FormValue("batch-name")
	jobType := r.FormValue("job-type")
	jobID := r.FormValue("job-id")

	if batchName == "" || jobType == "" || jobID == "" {
		http.Error(w, "Missing batch-name, job-type, or job-id", http.StatusBadRequest)
		return
	}

	if jobType != "backup" && jobType != "restore" && jobType != "verification" {
		http.Error(w, "Invalid job-type, must be backup, restore, or verification", http.StatusBadRequest)
		return
	}

	if err := app.CoreDB.AddJobToBatch(batchName, jobType, jobID); err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data": coredb.NotificationBatchJob{
			BatchName: batchName,
			JobType:   jobType,
			JobID:     jobID,
		},
	}); err != nil {
		log.Error(err, "")
	}
}

func removeBatchJob(app *application.Runtime, w http.ResponseWriter, r *http.Request) {
	batchName := r.FormValue("batch-name")
	jobType := r.FormValue("job-type")
	jobID := r.FormValue("job-id")

	if batchName == "" || jobType == "" || jobID == "" {
		http.Error(w, "Missing batch-name, job-type, or job-id", http.StatusBadRequest)
		return
	}

	if err := app.CoreDB.RemoveJobFromBatch(batchName, jobType, jobID); err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"success": true, "data": nil}); err != nil {
		log.Error(err, "")
	}
}

func NotificationBatchStatusHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if app.BatchTracker == nil {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]int{}}); err != nil {
				log.Error(err, "")
			}
			return
		}

		pending := app.BatchTracker.PendingBatches()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"success": true, "data": pending}); err != nil {
			log.Error(err, "")
		}
	}
}

func formValueInt(r *http.Request, key string, defaultVal int) int {
	v := r.FormValue(key)
	if v == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return i
}

func formValueBool(r *http.Request, key string, defaultVal bool) bool {
	v := r.FormValue(key)
	if v == "" {
		return defaultVal
	}
	return v == "1" || v == "true" || v == "on"
}

// ApplyJobBatchAssignment syncs a job's batch membership based on the
//   - If value is empty or matches delete: job is removed from all batches.
//   - If value is a batch name: job is added to that batch (and removed from others).
func ApplyJobBatchAssignment(app *application.Runtime, jobType, jobID, batchName string) {
	if err := app.CoreDB.RemoveJobFromAllBatches(jobType, jobID); err != nil {
		log.Error(err, "")
	}

	if batchName != "" {
		// Verify the batch exists before assigning
		batch, err := app.CoreDB.GetNotificationBatch(batchName)
		if err != nil || batch.Name == "" {
			return
		}
		if err := app.CoreDB.AddJobToBatch(batchName, jobType, jobID); err != nil {
			log.Error(err, "")
		}
	}
}

// GetJobBatchName returns the name of the batch a job is assigned to, or "" if none.
func GetJobBatchName(app *application.Runtime, jobType, jobID string) string {
	batch, err := app.CoreDB.GetBatchForJob(jobType, jobID)
	if err != nil {
		return ""
	}
	return batch.Name
}

var _ = notification.SpoolDir
