//go:build linux

package backup

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

func raceTestHarness(t *testing.T, workflow jobs.Workflow) (*application.Runtime, coredb.Backup) {
	t.Helper()
	restoreTask := tasklog.UseTaskDir(t.TempDir())
	t.Cleanup(restoreTask)

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

	engine, err := jobs.NewEngine(engineDB, jobs.EngineConfig{Owner: "race-test", MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	if workflow != nil {
		if err := engine.Register(jobs.WorkflowBackup, workflow); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	job := coredb.Backup{ID: "race-test-1", Store: "race-store", Target: coredb.Target{Name: "race-host", Path: "/data", Type: "s3"}}
	if err := cdb.CreateBackup(nil, job); err != nil {
		t.Fatal(err)
	}
	return &application.Runtime{Ctx: ctx, CoreDB: cdb, Engine: engine}, job
}

func waitTerminal(t *testing.T, app *application.Runtime, execID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		current, err := app.Engine.Get(app.Ctx, execID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State != jobdb.StatePending && current.State != jobdb.StateRunning {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution %s did not reach a terminal state (state=%s)", execID, current.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A late PrepareQueue (scheduler fires it after Submit) must never replace a finished run's terminal task.
func TestPrepareQueueAfterFinishedRunKeepsRealTasklog(t *testing.T) {
	var (
		app      *application.Runtime
		job      coredb.Backup
		wid      string
		mu       sync.Mutex
		realUPID string
	)
	app, job = raceTestHarness(t, func(w *jobs.WorkflowContext) error {
		queued, err := tasklog.NewQueuedTask("backup", wid, false)
		if err != nil {
			return err
		}
		w.BindTask(queued)
		start := queued.Task.StartTime
		if err := updateBackupStatus(false, 0, job, proxmox.Task{UPID: queued.UPID()}, w.Execution.ID, start, 0, app); err != nil {
			return err
		}
		wt, err := tasklog.NewWorkerTask("pbsplus", "backup", wid)
		if err != nil {
			return err
		}
		mu.Lock()
		realUPID = wt.UPID()
		mu.Unlock()
		if err := updateBackupStatus(false, 0, job, proxmox.Task{UPID: wt.UPID()}, w.Execution.ID, start, 0, app); err != nil {
			return err
		}
		queued.Close()
		wt.CloseWithStatus(tasklog.CreateState(errors.New("agent unreachable"), 0))
		return updateBackupStatus(false, 0, job, proxmox.Task{UPID: wt.UPID()}, w.Execution.ID, start, time.Now().Unix(), app)
	})
	wid, err := backupWorkerID(job)
	if err != nil {
		t.Fatal(err)
	}

	request, err := jobs.NewWorkflowSubmit(jobs.WorkflowBackup, job.ID, "scheduled", "backup:race-test-1:scheduled:1000", jobs.BackupInput{}, []string{"backup:" + job.ID}, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	exec, created, err := app.Engine.Submit(app.Ctx, request)
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}
	waitTerminal(t, app, exec.ID)

	if err := PrepareQueue(app, job, false); err != nil {
		t.Fatal(err)
	}

	stored, err := app.CoreDB.GetBackup(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	want := realUPID
	mu.Unlock()
	if stored.History.LastRunUpid != want {
		t.Fatalf("history upid = %q, want real task %q", stored.History.LastRunUpid, want)
	}
	if tasklog.IsQueuedUPID(stored.History.LastRunUpid) {
		t.Fatal("history upid is a queued placeholder after the run finished")
	}
	if stored.History.LastRunEndtime == 0 {
		t.Fatal("terminal endtime wiped by late placeholder write")
	}
	if stored.History.LastRunStatus != coredb.JobStatusFailed {
		t.Fatalf("status = %q, want failed", stored.History.LastRunStatus)
	}
}

// The guard must keep recording placeholders for new, still-active runs.
func TestPrepareQueueMarksNewActiveRun(t *testing.T) {
	app, job := raceTestHarness(t, func(w *jobs.WorkflowContext) error {
		<-w.Context.Done()
		return w.Context.Err()
	})

	request, err := jobs.NewWorkflowSubmit(jobs.WorkflowBackup, job.ID, "scheduled", "backup:race-test-1:scheduled:2000", jobs.BackupInput{}, []string{"backup:" + job.ID}, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := app.Engine.Submit(app.Ctx, request); err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}

	if err := PrepareQueue(app, job, false); err != nil {
		t.Fatal(err)
	}
	stored, err := app.CoreDB.GetBackup(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !tasklog.IsQueuedUPID(stored.History.LastRunUpid) {
		t.Fatalf("active new run not marked queued: %q", stored.History.LastRunUpid)
	}
}
