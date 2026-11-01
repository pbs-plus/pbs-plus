//go:build unix

package log

import (
	"errors"
	"strings"
	"testing"
)

func TestFormatJobLogLineReadsAsOneSentence(t *testing.T) {
	got := formatJobLogLine(&entry{
		level:   "error",
		message: "failed to assign queued task to backup job",
		err:     errors.New("job run already finalized"),
		fields: map[string]any{
			"hostname": "phoenix-pbs.sgl.com",
			"jobID":    "DP-D030-D",
		},
	})

	want := "pbs-plus: WARNING: failed to assign queued task to backup job: job run already finalized"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatJobLogLineKeepsTheWarningMarkerErrorsAreCountedBy(t *testing.T) {
	for _, level := range []string{"error", "warn"} {
		if got := formatJobLogLine(&entry{level: level, message: "x"}); !strings.Contains(got, "WARNING: ") {
			t.Errorf("level %q produced %q, want a WARNING marker", level, got)
		}
	}
	if got := formatJobLogLine(&entry{level: "info", message: "x"}); strings.Contains(got, "WARNING") {
		t.Errorf("info produced %q, want no WARNING marker", got)
	}
}

func TestFormatJobLogLineRendersFieldsSortedAsKeyValue(t *testing.T) {
	got := formatJobLogLine(&entry{
		level:   "info",
		message: "agent files are mounted and readable",
		fields: map[string]any{
			"mountpoint": "/mnt/pbs-plus-mounts/DP-D030-D",
			"agent":      "DP-D030",
			"jobID":      "DP-D030-D",
		},
	})

	want := "pbs-plus: agent files are mounted and readable agent=DP-D030 mountpoint=/mnt/pbs-plus-mounts/DP-D030-D"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if strings.Contains(got, "map[") {
		t.Error("job log must not expose a Go map dump")
	}
}

func TestFormatJobLogLineHandlesAMessageOrErrorAlone(t *testing.T) {
	if got := formatJobLogLine(&entry{level: "info", message: "started"}); got != "pbs-plus: started" {
		t.Errorf("message only = %q", got)
	}
	if got := formatJobLogLine(&entry{level: "error", err: errors.New("boom")}); got != "pbs-plus: WARNING: boom" {
		t.Errorf("error only = %q", got)
	}
}
