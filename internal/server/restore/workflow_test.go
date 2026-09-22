//go:build linux

package restore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

func TestRegisterSelectsRestoreWorkflowVersion2(t *testing.T) {
	db, err := jobdb.Open(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	engine, err := jobs.NewEngine(db, jobs.EngineConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := Register(engine, nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterVersion(jobs.WorkflowRestore, "1", func(*jobs.WorkflowContext) error { return nil }); err == nil {
		t.Fatal("restore workflow version 1 was not retained")
	}
	request, err := jobs.NewWorkflowSubmit(jobs.WorkflowRestore, "restore-id", "test", "restore-v2", jobs.RestoreInput{}, nil, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := engine.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if execution.WorkflowVersion != "2" {
		t.Fatalf("current restore workflow version = %q", execution.WorkflowVersion)
	}
}
