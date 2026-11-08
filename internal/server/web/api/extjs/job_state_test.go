package extjs

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestClassifyJob(t *testing.T) {
	done := coredb.JobHistory{LastRunUpid: "u", LastRunEndtime: 10}
	running := coredb.JobHistory{LastRunUpid: "u"}
	engine := NewActiveJobs(map[string]bool{"active": true})
	tests := []struct {
		name    string
		active  ActiveJobs
		id      string
		history coredb.JobHistory
		state   string
		want    string
	}{
		{"never ran", ActiveJobs{}, "job", coredb.JobHistory{}, "", JobStateNever},
		{"queued without engine data", ActiveJobs{}, "job", coredb.JobHistory{}, "QUEUED: waiting", JobStateRunning},
		{"running without engine data", ActiveJobs{}, "job", running, "", JobStateRunning},
		{"engine says running despite a stale state", engine, "active", done, "OK", JobStateRunning},
		{"engine says running with an unknown task", engine, "active", running, "unknown", JobStateRunning},
		{"engine says idle for an orphaned run", engine, "idle", running, "", JobStateFailed},
		{"engine says idle for a stale queued state", engine, "idle", done, "QUEUED: waiting", JobStateFailed},
		{"ok", engine, "idle", done, "OK", JobStateOK},
		{"warnings", engine, "idle", done, "WARNINGS: 2", JobStateWarning},
		{"error", engine, "idle", done, "connection refused", JobStateFailed},
		{"unknown", engine, "idle", done, "unknown", JobStateFailed},
	}
	for _, tt := range tests {
		if got := tt.active.Classify(tt.id, tt.history, ParseTaskStatus(tt.state)); got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestFlattenBackupsFlagsJobsNeedingAttention(t *testing.T) {
	backups := []coredb.Backup{
		{ID: "fresh", Schedule: "daily", History: coredb.JobHistory{LastRunUpid: "u", LastRunEndtime: 1, LastSuccessfulEndtime: 1 << 40}},
		{ID: "never", Schedule: "daily"},
		{ID: "excluded", Schedule: "daily"},
		{ID: "unscheduled"},
	}
	flat := FlattenBackups(backups, 7, true, map[string]bool{"excluded": true}, ActiveJobs{})
	want := map[string]bool{"fresh": false, "never": true, "excluded": false, "unscheduled": false}
	for _, f := range flat {
		if f.NeedsAttention != want[f.ID] || f.Stale != want[f.ID] {
			t.Errorf("%s: stale=%v attention=%v, want %v", f.ID, f.Stale, f.NeedsAttention, want[f.ID])
		}
	}
}

func TestWithStatusDerivesRunningAndDuration(t *testing.T) {
	runs := WithStatus([]coredb.Tasks{
		{UPID: "a", Starttime: 100},
		{UPID: "b", Starttime: 100, Endtime: 160, Status: "OK"},
		{UPID: "c", Starttime: 100, Status: "unknown"},
	})
	if runs[0].StatusParsed.Category != "running" || runs[0].Duration != nil {
		t.Errorf("running task = %#v", runs[0])
	}
	if runs[1].StatusParsed.Category != "ok" || runs[1].Duration == nil || *runs[1].Duration != 60 {
		t.Errorf("finished task = %#v", runs[1])
	}
	if runs[2].StatusParsed.Category != "unknown" {
		t.Errorf("orphaned task = %#v", runs[2])
	}
}
