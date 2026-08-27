//go:build linux

package notification

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

type fakeBatchDB struct {
	mu      sync.Mutex
	jobs    map[string]map[string]bool
	batch   map[string]coredb.NotificationBatch
	results map[string][]coredb.NotificationBatchResult
}

func newFakeBatchDB() *fakeBatchDB {
	return &fakeBatchDB{
		jobs:    make(map[string]map[string]bool),
		batch:   make(map[string]coredb.NotificationBatch),
		results: make(map[string][]coredb.NotificationBatchResult),
	}
}

func (f *fakeBatchDB) addBatch(b coredb.NotificationBatch, jobType, jobID string) {
	f.batch[b.Name] = b
	if f.jobs[b.Name] == nil {
		f.jobs[b.Name] = make(map[string]bool)
	}
	f.jobs[b.Name][jobType+":"+jobID] = true
}

// expireResults backdates collected results past any plausible wait timeout.
func (f *fakeBatchDB) expireResults(batchName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.results[batchName] {
		f.results[batchName][i].RecordedAt -= 86400
	}
}

func (f *fakeBatchDB) GetBatchForJob(jobType, jobID string) (coredb.NotificationBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := jobType + ":" + jobID
	for name, jobs := range f.jobs {
		if jobs[key] {
			return f.batch[name], nil
		}
	}
	return coredb.NotificationBatch{}, nil
}

func (f *fakeBatchDB) GetBatchJobs(batchName string) ([]coredb.NotificationBatchJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []coredb.NotificationBatchJob
	for key := range f.jobs[batchName] {
		jt, id, _ := strings.Cut(key, ":")
		out = append(out, coredb.NotificationBatchJob{BatchName: batchName, JobType: jt, JobID: id})
	}
	return out, nil
}

func (f *fakeBatchDB) GetNotificationBatch(name string) (coredb.NotificationBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.batch[name]
	if !ok {
		return coredb.NotificationBatch{}, fmt.Errorf("batch %q not found", name)
	}
	return b, nil
}

func (f *fakeBatchDB) RecordBatchResult(batchName string, r coredb.NotificationBatchResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.results[batchName] {
		if f.results[batchName][i].JobType == r.JobType && f.results[batchName][i].JobID == r.JobID {
			f.results[batchName][i] = r
			return nil
		}
	}
	f.results[batchName] = append(f.results[batchName], r)
	return nil
}

func (f *fakeBatchDB) GetBatchResults(batchName string) ([]coredb.NotificationBatchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]coredb.NotificationBatchResult, len(f.results[batchName]))
	copy(out, f.results[batchName])
	return out, nil
}

func (f *fakeBatchDB) TakeBatchResults(batchName string) ([]coredb.NotificationBatchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.results[batchName]
	delete(f.results, batchName)
	return out, nil
}

func (f *fakeBatchDB) ListBatchesWithResults() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for name := range f.results {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

type sentNotification struct {
	batch     string
	results   []JobResult
	isTimeout bool
}

type sentRecorder struct {
	mu   sync.Mutex
	list []sentNotification
}

func (s *sentRecorder) record(n sentNotification) {
	s.mu.Lock()
	s.list = append(s.list, n)
	s.mu.Unlock()
}

func (s *sentRecorder) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.list)
}

func (s *sentRecorder) snapshot() []sentNotification {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sentNotification, len(s.list))
	copy(out, s.list)
	return out
}

func (s *sentRecorder) waitForCount(t *testing.T, n int) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.count() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return s.count() >= n
}

func newTestTracker(db *fakeBatchDB) (*BatchTracker, *sentRecorder) {
	rec := &sentRecorder{}
	bt := &BatchTracker{
		db: db,
		send: func(b coredb.NotificationBatch, r []JobResult, isTimeout bool) {
			rec.record(sentNotification{batch: b.Name, results: r, isTimeout: isTimeout})
		},
	}
	return bt, rec
}

func TestBatchTracker_AllJobsReport_FlushesOnce(t *testing.T) {
	db := newFakeBatchDB()
	b := coredb.NotificationBatch{Name: "b1", WaitTimeoutSecs: 300, SendOnTimeout: true}
	db.addBatch(b, "backup", "job-a")
	db.addBatch(b, "backup", "job-b")

	bt, sent := newTestTracker(db)

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-a", "ds1", nil, nil)
	if sent.count() != 0 {
		t.Fatalf("should not flush before all jobs report, got %d", sent.count())
	}

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-b", "ds1", nil, nil)
	if !sent.waitForCount(t, 1) {
		t.Fatalf("expected exactly one flush, got %d", sent.count())
	}
	got := sent.snapshot()[0]
	if got.isTimeout {
		t.Error("completion flush should not be marked as timeout")
	}
	if len(got.results) != 2 {
		t.Errorf("expected 2 results in consolidated notification, got %d", len(got.results))
	}
}

func TestBatchTracker_SendOnTimeoutFalse_KeepsCollectingThenSends(t *testing.T) {
	db := newFakeBatchDB()
	b := coredb.NotificationBatch{Name: "b2", WaitTimeoutSecs: 300, SendOnTimeout: false}
	db.addBatch(b, "backup", "job-a")
	db.addBatch(b, "backup", "job-b")

	bt, sent := newTestTracker(db)

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-a", "ds1", nil, nil)

	db.expireResults("b2")
	bt.evaluateAll()
	if sent.count() != 0 {
		t.Fatalf("should not send on timeout when send-on-timeout is disabled, got %d sends", sent.count())
	}

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-b", "ds1", nil, nil)
	if !sent.waitForCount(t, 1) {
		t.Fatalf("batch was silently dropped after a timeout with send-on-timeout=false; "+
			"expected 1 consolidated notification once all jobs reported, got %d", sent.count())
	}
	if sent.snapshot()[0].isTimeout {
		t.Error("final flush after all jobs reported should not be marked as timeout")
	}
}

func TestBatchTracker_DeduplicatesReReportedJobs(t *testing.T) {
	db := newFakeBatchDB()
	b := coredb.NotificationBatch{Name: "b3", WaitTimeoutSecs: 300, SendOnTimeout: true}
	db.addBatch(b, "backup", "job-a")
	db.addBatch(b, "backup", "job-b")

	bt, sent := newTestTracker(db)

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-a", "ds1", nil, nil)
	bt.RecordJobResult("notification-system", JobTypeBackup, "job-a", "ds1",
		fmt.Errorf("boom"), nil)

	if sent.count() != 0 {
		t.Fatalf("should not flush before all jobs report, got %d", sent.count())
	}

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-b", "ds1", nil, nil)
	if !sent.waitForCount(t, 1) {
		t.Fatalf("expected one flush, got %d", sent.count())
	}

	got := sent.snapshot()[0]
	if len(got.results) != 2 {
		t.Errorf("expected 2 deduplicated results (one per job), got %d", len(got.results))
	}

	var aResult *JobResult
	for i := range got.results {
		if got.results[i].JobID == "job-a" {
			if aResult != nil {
				t.Fatal("job-a appeared more than once in consolidated results")
			}
			aResult = &got.results[i]
		}
	}
	if aResult == nil {
		t.Fatal("job-a result missing from consolidated notification")
	}
	if aResult.Severity != "error" {
		t.Errorf("expected job-a last result (error) to win, got severity %q", aResult.Severity)
	}
}

func TestBatchTracker_RecoversPendingBatchAfterRestart(t *testing.T) {
	db := newFakeBatchDB()
	b := coredb.NotificationBatch{Name: "b5", WaitTimeoutSecs: 300, SendOnTimeout: true}
	db.addBatch(b, "backup", "job-a")
	db.addBatch(b, "backup", "job-b")

	before, _ := newTestTracker(db)
	before.RecordJobResult("notification-system", JobTypeBackup, "job-a", "ds1", nil, nil)

	after, sent := newTestTracker(db)
	after.RecordJobResult("notification-system", JobTypeBackup, "job-b", "ds1", nil, nil)

	if !sent.waitForCount(t, 1) {
		t.Fatalf("restart lost results collected before it; expected 1 consolidated notification, got %d", sent.count())
	}
	if got := len(sent.snapshot()[0].results); got != 2 {
		t.Errorf("expected both pre- and post-restart results, got %d", got)
	}
}

func TestBatchTracker_TimeoutSendsPartialBatch(t *testing.T) {
	db := newFakeBatchDB()
	b := coredb.NotificationBatch{Name: "b6", WaitTimeoutSecs: 300, SendOnTimeout: true}
	db.addBatch(b, "backup", "job-a")
	db.addBatch(b, "backup", "job-b")

	bt, sent := newTestTracker(db)
	bt.RecordJobResult("notification-system", JobTypeBackup, "job-a", "ds1", nil, nil)
	if sent.count() != 0 {
		t.Fatalf("should not flush before the timeout elapsed, got %d", sent.count())
	}

	db.expireResults("b6")
	bt.evaluateAll()

	if !sent.waitForCount(t, 1) {
		t.Fatalf("expected a timeout flush, got %d", sent.count())
	}
	got := sent.snapshot()[0]
	if !got.isTimeout {
		t.Error("partial flush should be marked as timeout")
	}
	if len(got.results) != 1 {
		t.Errorf("expected the single reported result, got %d", len(got.results))
	}
}

func TestBatchTracker_EmptyBatchDoesNotPrematurelyFlush(t *testing.T) {
	db := newFakeBatchDB()
	b := coredb.NotificationBatch{Name: "b4", WaitTimeoutSecs: 300, SendOnTimeout: true}
	db.batch[b.Name] = b
	db.jobs[b.Name] = map[string]bool{}

	bt, sent := newTestTracker(db)

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-x", "ds1", nil, nil)

	time.Sleep(50 * time.Millisecond)
	if sent.count() != 0 {
		t.Fatalf("batch with no jobs should not flush on a result, got %d sends", sent.count())
	}
}
