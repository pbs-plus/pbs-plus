//go:build linux

package backup

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

// A slot-waiting backup (engine never started, execution pending) is visible
// as queued via PrepareQueue and cancels to JobStatusCanceled without
// bumping the retry counter.
func TestPrepareAndCancelQueuedSlotWait(t *testing.T) {
	restoreTask := tasklog.UseTaskDir(t.TempDir())
	defer restoreTask()

	ctx := context.Background()
	cdb, err := coredb.Initialize(ctx, filepath.Join(t.TempDir(), "core.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cdb.Close() })

	engineDB, err := jobdb.Open(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engineDB.Close() })

	engine, err := jobs.NewEngine(engineDB, jobs.EngineConfig{Owner: "test", MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(jobs.WorkflowBackup, func(*jobs.WorkflowContext) error { return nil }); err != nil {
		t.Fatal(err)
	}

	job := coredb.Backup{ID: "queue-test-1", Store: "test-store", Target: coredb.Target{Name: "test-host", Path: "/data", Type: "s3"}}
	if err := cdb.CreateBackup(nil, job); err != nil {
		t.Fatal(err)
	}
	app := &application.Runtime{Ctx: ctx, CoreDB: cdb, Engine: engine}

	request, err := jobs.NewWorkflowSubmit(jobs.WorkflowBackup, job.ID, "manual", "", jobs.BackupInput{}, []string{"backup:" + job.ID}, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := engine.Submit(ctx, request); err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}

	if err := PrepareQueue(app, job, true); err != nil {
		t.Fatal(err)
	}
	stored, err := cdb.GetBackup(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.History.LastRunUpid == "" {
		t.Fatal("history upid not set at mint")
	}

	if err := CancelQueued(app, job); err != nil {
		t.Fatal(err)
	}
	stored, err = cdb.GetBackup(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.History.LastRunStatus != coredb.JobStatusCanceled {
		t.Fatalf("status = %q, want canceled", stored.History.LastRunStatus)
	}
	if stored.History.RetryCount != 0 {
		t.Fatalf("retry count = %d, want 0", stored.History.RetryCount)
	}
	taskFound, err := tasklog.GetTaskByUPID(stored.History.LastRunUpid)
	if err != nil {
		t.Fatal(err)
	}
	if taskFound.ExitStatus != "operation canceled" {
		t.Fatalf("task exit = %q, want operation canceled", taskFound.ExitStatus)
	}

	if err := CancelQueued(app, job); err != nil {
		t.Fatalf("second cancel: %v", err)
	}
}
