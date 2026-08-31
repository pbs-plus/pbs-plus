package coredb

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

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
