package extjs

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestClassifyJob(t *testing.T) {
	done := coredb.JobHistory{LastRunUpid: "u", LastRunEndtime: 10}
	tests := []struct {
		name    string
		history coredb.JobHistory
		state   string
		want    string
	}{
		{"never ran", coredb.JobHistory{}, "", JobStateNever},
		{"queued", coredb.JobHistory{}, "QUEUED: waiting", JobStateRunning},
		{"running", coredb.JobHistory{LastRunUpid: "u"}, "", JobStateRunning},
		{"ok", done, "OK", JobStateOK},
		{"warnings", done, "WARNINGS: 2", JobStateWarning},
		{"error", done, "connection refused", JobStateFailed},
		{"unknown", done, "unknown", JobStateFailed},
	}
	for _, tt := range tests {
		if got := ClassifyJob(tt.history, ParseTaskStatus(tt.state)); got != tt.want {
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
	flat := FlattenBackups(backups, 7, true, map[string]bool{"excluded": true})
	want := map[string]bool{"fresh": false, "never": true, "excluded": false, "unscheduled": false}
	for _, f := range flat {
		if f.NeedsAttention != want[f.ID] || f.Stale != want[f.ID] {
			t.Errorf("%s: stale=%v attention=%v, want %v", f.ID, f.Stale, f.NeedsAttention, want[f.ID])
		}
	}
}
