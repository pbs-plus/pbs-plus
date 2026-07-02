//go:build linux

package notification

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

type fakeBatchDB struct {
	mu    sync.Mutex
	jobs  map[string]map[string]bool
	batch map[string]database.NotificationBatch
}

func newFakeBatchDB() *fakeBatchDB {
	return &fakeBatchDB{
		jobs:  make(map[string]map[string]bool),
		batch: make(map[string]database.NotificationBatch),
	}
}

func (f *fakeBatchDB) addBatch(b database.NotificationBatch, jobType, jobID string) {
	f.batch[b.Name] = b
	if f.jobs[b.Name] == nil {
		f.jobs[b.Name] = make(map[string]bool)
	}
	f.jobs[b.Name][jobType+":"+jobID] = true
}

func (f *fakeBatchDB) GetBatchForJob(jobType, jobID string) (database.NotificationBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := jobType + ":" + jobID
	for name, jobs := range f.jobs {
		if jobs[key] {
			return f.batch[name], nil
		}
	}
	return database.NotificationBatch{}, nil
}

func (f *fakeBatchDB) GetBatchJobs(batchName string) ([]database.NotificationBatchJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []database.NotificationBatchJob
	for key := range f.jobs[batchName] {
		jt, id, _ := strings.Cut(key, ":")
		out = append(out, database.NotificationBatchJob{BatchName: batchName, JobType: jt, JobID: id})
	}
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
		db:      db,
		pending: make(map[string]*batchState),
		timers:  make(map[string]*time.Timer),
		send: func(b database.NotificationBatch, r []JobResult, isTimeout bool) {
			rec.record(sentNotification{batch: b.Name, results: r, isTimeout: isTimeout})
		},
	}
	return bt, rec
}

func TestBatchTracker_AllJobsReport_FlushesOnce(t *testing.T) {
	db := newFakeBatchDB()
	b := database.NotificationBatch{Name: "b1", WaitTimeoutSecs: 300, SendOnTimeout: true}
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
	b := database.NotificationBatch{Name: "b2", WaitTimeoutSecs: 300, SendOnTimeout: false}
	db.addBatch(b, "backup", "job-a")
	db.addBatch(b, "backup", "job-b")

	bt, sent := newTestTracker(db)

	bt.RecordJobResult("notification-system", JobTypeBackup, "job-a", "ds1", nil, nil)

	bt.flushBatch("b2", true)
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
