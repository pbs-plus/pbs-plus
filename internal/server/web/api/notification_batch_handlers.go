//go:build linux

package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func NotificationBatchHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if name := r.URL.Query().Get("batch"); name != "" {
				getNotificationBatch(storeInstance, w, r, name)
			} else {
				listNotificationBatches(storeInstance, w, r)
			}
		case http.MethodPost:
			createNotificationBatch(storeInstance, w, r)
		case http.MethodPut:
			updateNotificationBatch(storeInstance, w, r)
		case http.MethodDelete:
			deleteNotificationBatch(storeInstance, w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func listNotificationBatches(storeInstance *store.Store, w http.ResponseWriter, r *http.Request) {
	batches, err := storeInstance.Database.ListNotificationBatches()
	if err != nil {
		WriteErrorResponse(w, err)
		return
	}

	type batchWithCount struct {
		database.NotificationBatch
		JobCount int `json:"job-count"`
	}

	result := make([]batchWithCount, len(batches))
	for i, b := range batches {
		jobs, _ := storeInstance.Database.GetBatchJobs(b.Name)
		count := 0
		if jobs != nil {
			count = len(jobs)
		}
		result[i] = batchWithCount{
			NotificationBatch: b,
			JobCount:          count,
		}
	}

	digest, err := calculateDigest(result)
	if err != nil {
		WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    result,
		"digest":  digest,
	})
}

func getNotificationBatch(storeInstance *store.Store, w http.ResponseWriter, r *http.Request, name string) {
	batch, err := storeInstance.Database.GetNotificationBatch(name)
	if err != nil || batch.Name == "" {
		http.Error(w, "Batch not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    batch,
	})
}

func createNotificationBatch(storeInstance *store.Store, w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Error(w, "Missing batch name", http.StatusBadRequest)
		return
	}
	if err := validate.ValidateJobId(name); err != nil {
		http.Error(w, "Invalid batch name: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Check if batch already exists
	existing, _ := storeInstance.Database.GetNotificationBatch(name)
	if existing.Name != "" {
		http.Error(w, "Batch already exists", http.StatusConflict)
		return
	}

	comment := r.FormValue("comment")
	mode := r.FormValue("notification-mode")
	timeoutSecs := formValueInt(r, "wait-timeout-secs", 300)
	sendOnTimeout := formValueBool(r, "send-on-timeout", true)

	batch := database.NotificationBatch{
		Name:             name,
		Comment:          comment,
		NotificationMode: mode,
		WaitTimeoutSecs:  timeoutSecs,
		SendOnTimeout:    sendOnTimeout,
	}

	if err := storeInstance.Database.CreateNotificationBatch(batch); err != nil {
		WriteErrorResponse(w, err)
		return
	}

	if jobs := r.FormValue("jobs"); jobs != "" {
		var jobList []struct {
			JobType string `json:"job-type"`
			JobID   string `json:"job-id"`
		}
		if err := json.Unmarshal([]byte(jobs), &jobList); err == nil {
			for _, j := range jobList {
				_ = storeInstance.Database.AddJobToBatch(name, j.JobType, j.JobID)
			}
		}
	}

	created, _ := storeInstance.Database.GetNotificationBatch(name)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    created,
	})
}

func updateNotificationBatch(storeInstance *store.Store, w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("batch")
	if name == "" {
		http.Error(w, "Missing batch parameter", http.StatusBadRequest)
		return
	}

	existing, err := storeInstance.Database.GetNotificationBatch(name)
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

	if err := storeInstance.Database.UpdateNotificationBatch(existing); err != nil {
		WriteErrorResponse(w, err)
		return
	}

	if jobs := r.FormValue("jobs"); jobs != "" {
		var jobList []struct {
			JobType string `json:"job-type"`
			JobID   string `json:"job-id"`
		}
		if err := json.Unmarshal([]byte(jobs), &jobList); err == nil {
			_ = storeInstance.Database.RemoveJobsByBatch(name)
			for _, j := range jobList {
				_ = storeInstance.Database.AddJobToBatch(name, j.JobType, j.JobID)
			}
		}
	}

	updated, _ := storeInstance.Database.GetNotificationBatch(name)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    updated,
	})
}

func deleteNotificationBatch(storeInstance *store.Store, w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("batch")
	if name == "" {
		http.Error(w, "Missing batch parameter", http.StatusBadRequest)
		return
	}

	if err := storeInstance.Database.DeleteNotificationBatch(name); err != nil {
		WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data":    nil,
	})
}

func NotificationBatchJobsHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			listBatchJobs(storeInstance, w, r)
		case http.MethodPost:
			addBatchJob(storeInstance, w, r)
		case http.MethodDelete:
			removeBatchJob(storeInstance, w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func listBatchJobs(storeInstance *store.Store, w http.ResponseWriter, r *http.Request) {
	batchName := r.URL.Query().Get("batch")
	if batchName == "" {
		allJobs, err := storeInstance.Database.ListBatchJobs()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"success": true, "data": allJobs})
		return
	}

	jobs, err := storeInstance.Database.GetBatchJobs(batchName)
	if err != nil {
		WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"success": true, "data": jobs})
}

func addBatchJob(storeInstance *store.Store, w http.ResponseWriter, r *http.Request) {
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

	if err := storeInstance.Database.AddJobToBatch(batchName, jobType, jobID); err != nil {
		WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"data": database.NotificationBatchJob{
			BatchName: batchName,
			JobType:   jobType,
			JobID:     jobID,
		},
	})
}

func removeBatchJob(storeInstance *store.Store, w http.ResponseWriter, r *http.Request) {
	batchName := r.FormValue("batch-name")
	jobType := r.FormValue("job-type")
	jobID := r.FormValue("job-id")

	if batchName == "" || jobType == "" || jobID == "" {
		http.Error(w, "Missing batch-name, job-type, or job-id", http.StatusBadRequest)
		return
	}

	if err := storeInstance.Database.RemoveJobFromBatch(batchName, jobType, jobID); err != nil {
		WriteErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"success": true, "data": nil})
}

func NotificationBatchStatusHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if storeInstance.BatchTracker == nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]int{}})
			return
		}

		pending := storeInstance.BatchTracker.PendingBatches()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"success": true, "data": pending})
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
func ApplyJobBatchAssignment(storeInstance *store.Store, jobType, jobID, batchName string) {
	_ = storeInstance.Database.RemoveJobFromAllBatches(jobType, jobID)

	if batchName != "" {
		// Verify the batch exists before assigning
		batch, err := storeInstance.Database.GetNotificationBatch(batchName)
		if err != nil || batch.Name == "" {
			return
		}
		_ = storeInstance.Database.AddJobToBatch(batchName, jobType, jobID)
	}
}

// GetJobBatchName returns the name of the batch a job is assigned to, or "" if none.
func GetJobBatchName(storeInstance *store.Store, jobType, jobID string) string {
	batch, err := storeInstance.Database.GetBatchForJob(jobType, jobID)
	if err != nil {
		return ""
	}
	return batch.Name
}

// init ensures the notification package constants are referenced so the
var _ = notification.SpoolDir
