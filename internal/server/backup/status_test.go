//go:build linux

package backup

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestApplyWorkflowBounds(t *testing.T) {
	tests := []struct {
		name       string
		start      int64
		end        int64
		succeeded  bool
		duration   int64
		successEnd int64
	}{
		{name: "running workflow", start: 100},
		{name: "successful workflow", start: 100, end: 190, succeeded: true, duration: 90, successEnd: 190},
		{name: "clock adjustment does not produce negative duration", start: 190, end: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := coredb.JobHistory{}
			applyWorkflowBounds(&history, tt.start, tt.end, tt.succeeded)
			if history.LastRunStarttime != tt.start || history.LastRunEndtime != tt.end || history.Duration != tt.duration || history.LastSuccessfulEndtime != tt.successEnd {
				t.Fatalf("history times = (%d, %d, %d, %d), want (%d, %d, %d, %d)", history.LastRunStarttime, history.LastRunEndtime, history.Duration, history.LastSuccessfulEndtime, tt.start, tt.end, tt.duration, tt.successEnd)
			}
		})
	}
}
