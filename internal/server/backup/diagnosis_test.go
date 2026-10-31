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

func TestDiagnoseNamesCauseAndActionForOperators(t *testing.T) {
	job := agentJob()

	cases := map[string]struct {
		err      error
		problem  string
		wantCase bool
	}{
		"unreachable agent": {
			err:      fmt.Errorf("mounting: %w", jobs.ErrTargetUnreachable),
			problem:  "could not reach",
			wantCase: true,
		},
		"severed session": {
			err:      fmt.Errorf("reading: %w", arpcfs.ErrSessionLost),
			problem:  "could not reach",
			wantCase: true,
		},
		"stalled run": {
			err:      fmt.Errorf("%w: agent transport down for 15m0s", ErrBackupStalled),
			problem:  "stopped transferring",
			wantCase: true,
		},
		"empty source": {
			err:      jobs.ErrMountEmpty,
			problem:  "contained no files",
			wantCase: true,
		},
		"missing subpath": {
			err:      jobs.ErrSubpathNotFound,
			problem:  "does not exist",
			wantCase: true,
		},
		"duplicate run": {
			err:      sessions.ErrMountInUse,
			problem:  "still active",
			wantCase: true,
		},
		"cancelled": {
			err:     jobs.ErrCanceled,
			problem: "was cancelled",
		},
		"unknown": {
			err:     errors.New("something nobody mapped"),
			problem: "The backup failed.",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			d := diagnose(tc.err, job)
			if !strings.Contains(d.Problem, tc.problem) {
				t.Fatalf("problem = %q, want it to contain %q", d.Problem, tc.problem)
			}
			if d.Action == "" {
				t.Error("every diagnosis must tell the operator what to do")
			}
			if tc.wantCase && d.Cause == "" {
				t.Error("want a likely cause for a diagnosable failure")
			}
		})
	}
}

func TestDiagnoseIdentifiesTheSourceMachine(t *testing.T) {
	d := diagnose(jobs.ErrTargetUnreachable, agentJob())
	if !strings.Contains(d.Problem, "DP-D030") || !strings.Contains(d.Problem, "10.1.24.9") {
		t.Fatalf("problem = %q, want the agent name and address", d.Problem)
	}
}

func TestReportLinesCarryIdentityWindowAndDiagnosis(t *testing.T) {
	lines := reportLines(agentJob(), jobs.ErrTargetUnreachable, 1700000000, 1700003600)
	joined := strings.Join(lines, "\n")

	for _, want := range []string{
		`Backup job "DP-D030-D"`,
		`datastore "phoenix"`,
		"DP/DP-D030/D",
		"/C/Data",
		"ran for 1h0m0s",
		"PROBLEM:",
		"LIKELY CAUSE:",
		"WHAT TO DO:",
		"TECHNICAL DETAIL: target unreachable",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("report missing %q; got:\n%s", want, joined)
		}
	}
}

func TestReportLinesTolerateAnUnfinishedRun(t *testing.T) {
	joined := strings.Join(reportLines(agentJob(), jobs.ErrCanceled, 0, 0), "\n")
	if !strings.Contains(joined, "Started unknown, ended unknown, ran for unknown") {
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
