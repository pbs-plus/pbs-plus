//go:build linux

package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

// DefaultBatchWaitSecs applies when a batch has no configured wait timeout.
const DefaultBatchWaitSecs = 300

type batchDB interface {
	GetBatchForJob(jobType, jobID string) (coredb.NotificationBatch, error)
	GetNotificationBatch(name string) (coredb.NotificationBatch, error)
	GetBatchJobs(batchName string) ([]coredb.NotificationBatchJob, error)
	RecordBatchResult(batchName string, r coredb.NotificationBatchResult) error
	GetBatchResults(batchName string) ([]coredb.NotificationBatchResult, error)
	TakeBatchResults(batchName string) ([]coredb.NotificationBatchResult, error)
	ListBatchesWithResults() ([]string, error)
}

// BatchTracker collects job results into one notification; results are persisted so a restart resumes them.
type BatchTracker struct {
	db batchDB

	send func(batch coredb.NotificationBatch, results []JobResult, isTimeout bool)

	flushMu sync.Mutex
}

type JobResult struct {
	JobType   string `json:"job-type"`
	JobID     string `json:"job-id"`
	Datastore string `json:"datastore"`
	Error     string `json:"error,omitempty"`
	Severity  string `json:"severity"`
	Timestamp int64  `json:"timestamp"`
}

func NewBatchTracker(db *coredb.Store) *BatchTracker {
	bt := &BatchTracker{db: db}
	bt.send = bt.sendBatchNotification
	return bt
}

func (bt *BatchTracker) RecordJobResult(mode string, jobType JobType, jobID, datastore string, jobErr error, details map[string]string) {
	batch, err := bt.db.GetBatchForJob(string(jobType), jobID)
	if err != nil {
		log.Error(err, "failed to lookup batch for job, sending immediate notification", "jobID", jobID)
		Send(mode, jobType, jobID, datastore, jobErr, details)
		return
	}

	if batch.Name == "" {
		Send(mode, jobType, jobID, datastore, jobErr, details)
		return
	}

	result := coredb.NotificationBatchResult{
		JobType:    string(jobType),
		JobID:      jobID,
		Datastore:  datastore,
		Error:      errStr(jobErr),
		Severity:   resultSeverity(jobErr, details),
		RecordedAt: time.Now().Unix(),
	}

	if err := bt.db.RecordBatchResult(batch.Name, result); err != nil {
		log.Error(err, "failed to persist batch result, sending immediate notification",
			"batch", batch.Name, "jobID", jobID)
		Send(mode, jobType, jobID, datastore, jobErr, details)
		return
	}

	bt.evaluate(batch)
}

func resultSeverity(jobErr error, details map[string]string) string {
	if jobErr != nil {
		return "error"
	}
	for _, key := range []string{"warnings", "errors", "failed"} {
		v, ok := details[key]
		if !ok {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Error(err, "unparsable job detail count", "key", key, "value", v)
			continue
		}
		if n > 0 {
			return "notice"
		}
	}
	return "info"
}

// Run flushes ready batches on a ticker and recovers batches left pending by a restart.
func (bt *BatchTracker) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	bt.evaluateAll()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bt.evaluateAll()
		}
	}
}

func (bt *BatchTracker) evaluateAll() {
	names, err := bt.db.ListBatchesWithResults()
	if err != nil {
		log.Error(err, "failed to list pending notification batches")
		return
	}
	for _, name := range names {
		batch, err := bt.db.GetNotificationBatch(name)
		if err != nil {
			log.Error(err, "failed to load notification batch", "batch", name)
			continue
		}
		bt.evaluate(batch)
	}
}

// evaluate flushes once every job reported, or once the wait timeout elapsed with send-on-timeout set.
func (bt *BatchTracker) evaluate(batch coredb.NotificationBatch) {
	bt.flushMu.Lock()
	defer bt.flushMu.Unlock()

	results, err := bt.db.GetBatchResults(batch.Name)
	if err != nil {
		log.Error(err, "failed to read batch results", "batch", batch.Name)
		return
	}
	if len(results) == 0 {
		return
	}

	complete, err := bt.allJobsReported(batch.Name, results)
	if err != nil {
		log.Error(err, "failed to check batch completion", "batch", batch.Name)
		return
	}

	if !complete {
		if !batch.SendOnTimeout || !batchTimedOut(batch, results) {
			return
		}
		bt.flush(batch, results, true)
		return
	}
	bt.flush(batch, results, false)
}

func batchTimedOut(batch coredb.NotificationBatch, results []coredb.NotificationBatchResult) bool {
	waitSecs := int64(batch.WaitTimeoutSecs)
	if waitSecs <= 0 {
		waitSecs = DefaultBatchWaitSecs
	}
	oldest := results[0].RecordedAt
	for _, r := range results[1:] {
		if r.RecordedAt < oldest {
			oldest = r.RecordedAt
		}
	}
	return time.Now().Unix()-oldest >= waitSecs
}

// flush must be called with flushMu held.
func (bt *BatchTracker) flush(batch coredb.NotificationBatch, expected []coredb.NotificationBatchResult, isTimeout bool) {
	taken, err := bt.db.TakeBatchResults(batch.Name)
	if err != nil {
		log.Error(err, "failed to claim batch results, will retry on next tick", "batch", batch.Name)
		return
	}
	if len(taken) == 0 {
		return
	}
	if len(taken) != len(expected) {
		slog.Info("notification batch grew while flushing", "batch", batch.Name,
			"expected", len(expected), "sent", len(taken))
	}

	out := make([]JobResult, len(taken))
	for i, r := range taken {
		out[i] = JobResult{
			JobType:   r.JobType,
			JobID:     r.JobID,
			Datastore: r.Datastore,
			Error:     r.Error,
			Severity:  r.Severity,
			Timestamp: r.RecordedAt,
		}
	}
	bt.send(batch, out, isTimeout)
}

func (bt *BatchTracker) allJobsReported(batchName string, results []coredb.NotificationBatchResult) (bool, error) {
	jobs, err := bt.db.GetBatchJobs(batchName)
	if err != nil {
		return false, err
	}
	if len(jobs) == 0 {
		return false, nil
	}

	reported := make(map[string]bool, len(results))
	for _, r := range results {
		reported[r.JobType+":"+r.JobID] = true
	}

	for _, j := range jobs {
		if !reported[j.JobType+":"+j.JobID] {
			return false, nil
		}
	}
	return true, nil
}

func (bt *BatchTracker) sendBatchNotification(batch coredb.NotificationBatch, results []JobResult, isTimeout bool) {
	if len(results) == 0 {
		return
	}

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
	sort.Strings(dsList)

	fields := map[string]string{
		"hostname":  getHostname(),
		"type":      "d2d-batch",
		"batch":     batch.Name,
		"datastore": dsList[0],
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
		log.Error(err, "failed to marshal batch template data")
		return
	}

	tc := templateContent{
		TemplateName: templateName,
		Data:         tmplData,
	}
	tcJSON, err := json.Marshal(tc)
	if err != nil {
		log.Error(err, "failed to marshal batch template content")
		return
	}

	wrappedContent, err := json.Marshal(map[string]json.RawMessage{
		"template": tcJSON,
	})
	if err != nil {
		log.Error(err, "failed to wrap batch template content")
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
		mode = string(ModeNotificationSystem)
	}

	switch NotificationMode(mode) {
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

func (bt *BatchTracker) PendingBatches() map[string]int {
	names, err := bt.db.ListBatchesWithResults()
	if err != nil {
		log.Error(err, "failed to list pending notification batches")
		return map[string]int{}
	}

	out := make(map[string]int, len(names))
	for _, name := range names {
		results, err := bt.db.GetBatchResults(name)
		if err != nil {
			log.Error(err, "failed to read batch results", "batch", name)
			continue
		}
		out[name] = len(results)
	}
	return out
}
