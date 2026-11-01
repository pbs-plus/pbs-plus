//go:build linux

package backup

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

func agentJob() coredb.Backup {
	return coredb.Backup{
		ID:         "DP-D030-D",
		Store:      "phoenix",
		Namespace:  "DP/DP-D030/D",
		SourceMode: "snapshot",
		Subpath:    "Data",
		Target: coredb.Target{
			Name: "DP-D030",
			Path: "/C",
			AgentHost: coredb.AgentHost{
				Name: "DP-D030",
				IP:   "10.1.24.9",
			},
		},
	}
}

func TestFailureSummaryNamesTheFailure(t *testing.T) {
	job := agentJob()

	cases := map[string]struct {
		err  error
		want string
	}{
		"unreachable agent": {fmt.Errorf("mounting: %w", jobs.ErrTargetUnreachable), "cannot reach agent DP-D030 (10.1.24.9)"},
		"severed session":   {fmt.Errorf("reading: %w", arpcfs.ErrSessionLost), "cannot reach agent DP-D030 (10.1.24.9)"},
		"stalled run":       {fmt.Errorf("%w: down 15m", ErrBackupStalled), "cancelled after the connection to agent DP-D030 (10.1.24.9) stayed down"},
		"empty source":      {jobs.ErrMountEmpty, "source /C/Data is empty, nothing was backed up"},
		"missing subpath":   {jobs.ErrSubpathNotFound, "subpath Data does not exist on agent DP-D030 (10.1.24.9)"},
		"duplicate run":     {sessions.ErrMountInUse, "a previous run of this job is still active"},
		"cancelled":         {jobs.ErrCanceled, "cancelled before it finished"},
		"client failure":    {ErrTaskFailed, "proxmox-backup-client reported a failure"},
		"unmapped":          {errors.New("something nobody mapped"), "backup failed"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := failureSummary(tc.err, job); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReportLinesCarryIdentityWindowAndResult(t *testing.T) {
	lines := reportLines(agentJob(), jobs.ErrTargetUnreachable, 1700000000, 1700003600, "")

	want := []string{
		"Job: DP-D030-D",
		"Datastore: phoenix, namespace DP/DP-D030/D",
		"Source: agent DP-D030 (10.1.24.9), path /C/Data, mode snapshot",
		"Result: cannot reach agent DP-D030 (10.1.24.9)",
		"Detail: target unreachable",
	}
	joined := strings.Join(lines, "\n")
	for _, w := range want {
		if !strings.Contains(joined, w) {
			t.Errorf("report missing %q; got:\n%s", w, joined)
		}
	}
	if !strings.Contains(joined, "duration 1h0m0s") {
		t.Errorf("report missing the duration; got:\n%s", joined)
	}
}

func TestReportLinesOmitDetailWhenThereIsNoError(t *testing.T) {
	joined := strings.Join(reportLines(agentJob(), nil, 1700000000, 1700000010, ""), "\n")
	if strings.Contains(joined, "Detail:") {
		t.Fatalf("want no detail line on success, got:\n%s", joined)
	}
	if !strings.Contains(joined, "Result: backup finished") {
		t.Fatalf("want a result line, got:\n%s", joined)
	}
}

func TestReportLinesTolerateAnUnfinishedRun(t *testing.T) {
	joined := strings.Join(reportLines(agentJob(), jobs.ErrCanceled, 0, 0, ""), "\n")
	if !strings.Contains(joined, "Started: unknown, ended unknown, duration unknown") {
		t.Fatalf("want unknown timestamps, got:\n%s", joined)
	}
}

func TestSourcePathFallsBackToTheTargetPath(t *testing.T) {
	job := agentJob()
	job.Subpath = ""
	if got := sourcePath(job); got != "/C" {
		t.Fatalf("sourcePath = %q, want /C", got)
	}
}
