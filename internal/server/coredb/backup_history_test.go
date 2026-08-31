package coredb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

func TestUpdateBackupHistoryPreservesConcurrentConfiguration(t *testing.T) {
	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "backup-history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := Target{
		Name:   "local",
		Type:   TargetTypeFilesystem,
		Access: FilesystemAccessLocal,
		Path:   t.TempDir(),
	}
	if err := db.CreateTarget(nil, target); err != nil {
		t.Fatal(err)
	}
	running := Backup{ID: "job", Store: "store", Target: target, Comment: "original"}
	if err := db.CreateBackup(nil, running); err != nil {
		t.Fatal(err)
	}

	edited := running
	edited.Comment = "edited while running"
	if err := db.UpdateBackup(nil, edited); err != nil {
		t.Fatal(err)
	}

	if err := db.UpdateBackupNamespace(running.ID, "runtime"); err != nil {
		t.Fatal(err)
	}
	running.History = JobHistory{LastRunStatus: JobStatusSuccess, RetryCount: 2, LastRunEndtime: 200, Duration: 100}
	if err := db.UpdateBackupHistory(running.ID, running.History, 0); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetBackup(running.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Comment != edited.Comment {
		t.Fatalf("comment = %q, want %q", got.Comment, edited.Comment)
	}
	if got.Namespace != "runtime" {
		t.Fatalf("namespace = %q, want runtime", got.Namespace)
	}
	if got.History.LastRunStatus != JobStatusSuccess || got.History.RetryCount != 2 {
		t.Fatalf("history = %#v", got.History)
	}
}

func TestApplyBackupTaskHistoryUsesWorkflowBounds(t *testing.T) {
	tests := []struct {
		name     string
		history  JobHistory
		resolved tasklog.ResolvedHistory
		now      int64
		start    int64
		end      int64
		duration int64
	}{
		{
			name:     "active backup includes pre-work",
			history:  JobHistory{LastRunStarttime: 100, LastRunStatus: JobStatusUnknown},
			resolved: tasklog.ResolvedHistory{Starttime: 130},
			now:      170,
			start:    100,
			duration: 70,
		},
		{
			name:     "finished backup includes post-work",
			history:  JobHistory{LastRunStarttime: 100, LastRunEndtime: 190, LastRunStatus: JobStatusSuccess},
			resolved: tasklog.ResolvedHistory{Starttime: 130, Endtime: 170, Duration: 40},
			now:      200,
			start:    100,
			end:      190,
			duration: 90,
		},
		{
			name:     "generated error uses terminal task time",
			history:  JobHistory{LastRunStatus: JobStatusUnknown},
			resolved: tasklog.ResolvedHistory{Starttime: 130, Endtime: 170, Duration: 40, State: "TASK ERROR"},
			now:      200,
			start:    130,
			end:      170,
			duration: 40,
		},
		{
			name:     "legacy backup uses native task bounds",
			history:  JobHistory{LastRunStatus: JobStatusSuccess},
			resolved: tasklog.ResolvedHistory{Starttime: 130, Endtime: 170, Duration: 40},
			now:      200,
			start:    130,
			end:      170,
			duration: 40,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := tt.history
			applyBackupTaskHistory(&history, tt.resolved, tt.now)
			if history.LastRunStarttime != tt.start || history.LastRunEndtime != tt.end || history.Duration != tt.duration {
				t.Fatalf("history times = (%d, %d, %d), want (%d, %d, %d)", history.LastRunStarttime, history.LastRunEndtime, history.Duration, tt.start, tt.end, tt.duration)
			}
		})
	}
}
