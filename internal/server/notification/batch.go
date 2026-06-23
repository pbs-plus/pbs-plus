//go:build linux

package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// The tracker checks if the job belongs to a notification batch:
//
//	(or the timeout fires), flush a single consolidated notification.
//
// Thread-safe. Designed to be embedded in the store or started as a
// long-running goroutine.
type BatchTracker struct {
	db *database.Database

	// mu protects the pending map.
	mu sync.Mutex

	pending map[string]*batchState

	// timers tracks active flush timers per batch.
	timers map[string]*time.Timer
}

type batchState struct {
	results []JobResult
	batch   database.NotificationBatch
}

type JobResult struct {
	JobType   string `json:"job-type"`
	JobID     string `json:"job-id"`
	Datastore string `json:"datastore"`
	Error     string `json:"error,omitempty"`
	Severity  string `json:"severity"`
	Timestamp int64  `json:"timestamp"`
}

func NewBatchTracker(db *database.Database) *BatchTracker {
	return &BatchTracker{
		db:      db,
		pending: make(map[string]*batchState),
		timers:  make(map[string]*time.Timer),
	}
}

func (bt *BatchTracker) RecordJobResult(mode string, jobType JobType, jobID, datastore string, jobErr error, details map[string]string) {
	batch, err := bt.db.GetBatchForJob(string(jobType), jobID)
	if err != nil {
		syslog.L.Error(err).
			WithField("jobID", jobID).
			WithMessage("failed to lookup batch for job, sending immediate notification").
			Write()
		Send(mode, jobType, jobID, datastore, jobErr, details)
		return
	}

	if batch.Name == "" {
		Send(mode, jobType, jobID, datastore, jobErr, details)
		return
	}

	// Determine severity for this individual result.
	severity := "info"
	if jobErr != nil {
		severity = "error"
	} else if details != nil {
		if warningsStr, ok := details["warnings"]; ok {
			if n, err := strconv.Atoi(warningsStr); err != nil {
				syslog.L.Error(err).Write()
			} else if n > 0 {
				severity = "notice"
			}
		}
		if errorsStr, ok := details["errors"]; ok {
			if n, err := strconv.Atoi(errorsStr); err != nil {
				syslog.L.Error(err).Write()
			} else if n > 0 {
				severity = "notice"
			}
		}
		if failedStr, ok := details["failed"]; ok {
			if n, err := strconv.Atoi(failedStr); err != nil {
				syslog.L.Error(err).Write()
			} else if n > 0 {
				severity = "notice"
			}
		}
	}

	result := JobResult{
		JobType:   string(jobType),
		JobID:     jobID,
		Datastore: datastore,
		Error:     errStr(jobErr),
		Severity:  severity,
		Timestamp: time.Now().Unix(),
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	state, exists := bt.pending[batch.Name]
	if !exists {
		state = &batchState{batch: batch}
		bt.pending[batch.Name] = state

		timeout := time.Duration(batch.WaitTimeoutSecs) * time.Second
		if timeout <= 0 {
			timeout = 5 * time.Minute
		}
		timer := time.AfterFunc(timeout, func() {
			bt.flushBatch(batch.Name, true)
		})
		bt.timers[batch.Name] = timer

		slog.Info("notification batch started collecting",
			"batch", batch.Name, "timeout", timeout)
	}

	state.results = append(state.results, result)

	if bt.allJobsReported(batch.Name, state) {
		if timer, ok := bt.timers[batch.Name]; ok {
			timer.Stop()
			delete(bt.timers, batch.Name)
		}
		go bt.flushBatch(batch.Name, false)
	}
}

// allJobsReported checks if all jobs in the batch have a result buffered.
// Must be called with bt.mu held.
func (bt *BatchTracker) allJobsReported(batchName string, state *batchState) bool {
	jobs, err := bt.db.GetBatchJobs(batchName)
	if err != nil {
		return false
	}

	reported := make(map[string]bool, len(state.results))
	for _, r := range state.results {
		reported[r.JobType+":"+r.JobID] = true
	}

	for _, j := range jobs {
		if !reported[j.JobType+":"+j.JobID] {
			return false
		}
	}
	return true
}

// flushBatch sends a consolidated notification for the batch.
func (bt *BatchTracker) flushBatch(batchName string, isTimeout bool) {
	bt.mu.Lock()
	state, exists := bt.pending[batchName]
	if !exists {
		bt.mu.Unlock()
		return
	}

	if isTimeout && !state.batch.SendOnTimeout {
		// Don't send on timeout  -  wait for all jobs.
		slog.Info("notification batch timeout reached but send-on-timeout is disabled, skipping",
			"batch", batchName, "collected", len(state.results))
		delete(bt.pending, batchName)
		delete(bt.timers, batchName)
		bt.mu.Unlock()
		return
	}

	delete(bt.pending, batchName)
	delete(bt.timers, batchName)
	bt.mu.Unlock()

	bt.sendBatchNotification(state.batch, state.results, isTimeout)
}

func (bt *BatchTracker) sendBatchNotification(batch database.NotificationBatch, results []JobResult, isTimeout bool) {
	if len(results) == 0 {
		return
	}

	// Determine overall severity.
	// Otherwise: info.
	severity := "info"
	hasErrors := 0
	hasWarnings := 0
	for _, r := range results {
		if r.Severity == "error" {
			severity = "error"
			hasErrors++
		}
		if r.Severity == "notice" {
			hasWarnings++
		}
	}
	if severity == "info" && hasWarnings > 0 {
		severity = "notice"
	}

	datastores := make(map[string]bool)
	for _, r := range results {
		datastores[r.Datastore] = true
	}
	dsList := make([]string, 0, len(datastores))
	for ds := range datastores {
		dsList = append(dsList, ds)
	}

	fields := map[string]string{
		"hostname":  getHostname(),
		"type":      "d2d-batch",
		"batch":     batch.Name,
		"datastore": dsList[0], // primary datastore for matcher compatibility
	}

	templateName := "d2d-batch-ok"
	if hasErrors > 0 {
		templateName = "d2d-batch-err"
	}

	tmplData, err := json.Marshal(map[string]any{
		"batch":      batch.Name,
		"total":      len(results),
		"errors":     hasErrors,
		"successful": len(results) - hasErrors,
		"timeout":    isTimeout,
		"jobs":       results,
		"datastores": dsList,
	})
	if err != nil {
		syslog.L.Error(err).Write()
	}

	tc := templateContent{
		TemplateName: templateName,
		Data:         tmplData,
	}
	tcJSON, err := json.Marshal(tc)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to marshal batch template content").Write()
		return
	}

	wrappedContent, err := json.Marshal(map[string]json.RawMessage{
		"template": tcJSON,
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to wrap batch template content").Write()
		return
	}

	n := notification{
		Content: wrappedContent,
		Metadata: metadata{
			Severity:         severity,
			Timestamp:        time.Now().Unix(),
			AdditionalFields: fields,
		},
		ID: uuid.New().String(),
	}

	mode := batch.NotificationMode
	if mode == "" {
		mode = "notification-system"
	}

	nm := NotificationMode(mode)
	switch nm {
	case ModeLegacySendmail:
		title := fmt.Sprintf("Batch '%s': %d/%d jobs succeeded", batch.Name, len(results)-hasErrors, len(results))
		if hasErrors > 0 {
			title = fmt.Sprintf("Batch '%s': %d/%d jobs failed", batch.Name, hasErrors, len(results))
		}
		sendLegacy(n, title)
	default:
		sendViaSpool(n)
	}

	slog.Info("sent batch notification",
		"batch", batch.Name,
		"total", len(results),
		"errors", hasErrors,
		"timeout", isTimeout)
}

// StartCleanup starts a periodic goroutine that removes stale batch state.
func (bt *BatchTracker) StartCleanup(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bt.mu.Lock()
			for name, state := range bt.pending {
				if len(state.results) > 0 {
					oldest := state.results[0].Timestamp
					timeout := int64(state.batch.WaitTimeoutSecs) * 2
					if timeout <= 0 {
						timeout = 600
					}
					if time.Now().Unix()-oldest > timeout {
						delete(bt.pending, name)
						if timer, ok := bt.timers[name]; ok {
							timer.Stop()
							delete(bt.timers, name)
						}
						slog.Warn("cleaned up stale notification batch", "batch", name)
					}
				}
			}
			bt.mu.Unlock()
		}
	}
}

func (bt *BatchTracker) PendingBatches() map[string]int {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	out := make(map[string]int, len(bt.pending))
	for name, state := range bt.pending {
		out[name] = len(state.results)
	}
	return out
}

// EnsureSpoolDir creates the notification spool directory if it doesn't exist.
func EnsureSpoolDir() error {
	return os.MkdirAll(SpoolDir, 0770)
}
