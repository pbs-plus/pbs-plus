//go:build linux

package backup

import (
	"context"
	"errors"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

// PrepareQueue mints the queued task at submit time so a slot-waiting job is
// visible (log, history, stoppable); the workflow's NewQueuedTask attaches by key.
func PrepareQueue(app *application.Runtime, job coredb.Backup, web bool) error {
	ctx := app.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	exec, err := app.Engine.ActiveExecution(ctx, jobs.WorkflowBackup, job.ID)
	if errors.Is(err, jobdb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	workerID, err := backupWorkerID(job)
	if err != nil {
		return err
	}
	queued, err := tasklog.NewQueuedTask("backup", workerID, web)
	if err != nil {
		return err
	}
	queued.OnAbort(func() { _ = CancelQueued(app, job) })
	if err := updateBackupStatus(false, 0, job, proxmox.Task{UPID: queued.UPID()}, exec.ID, queued.Task.StartTime, 0, app); err != nil {
		if errors.Is(err, jobs.ErrRunFinalized) {
			queued.Close()
			return nil
		}
		return err
	}
	return nil
}

// CancelQueued stops a queued job: pre-claim it closes the task as canceled
// (JobStatusCanceled, retry counter untouched); claimed ones finalize normally.
func CancelQueued(app *application.Runtime, job coredb.Backup) error {
	ctx := context.Background()
	exec, err := app.Engine.ActiveExecution(ctx, jobs.WorkflowBackup, job.ID)
	if errors.Is(err, jobdb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	exec, err = app.Engine.Cancel(ctx, exec.ID)
	if errors.Is(err, jobdb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if exec.State != jobdb.StateCanceled {
		return nil
	}
	workerID, err := backupWorkerID(job)
	if err != nil {
		return err
	}
	queued, err := tasklog.NewQueuedTask("backup", workerID, false)
	if err != nil {
		return err
	}
	queued.CloseErr(jobs.ErrCanceled)
	if err := updateBackupStatus(false, 0, job, proxmox.Task{UPID: queued.UPID()}, exec.ID, queued.Task.StartTime, time.Now().Unix(), app); err != nil {
		if errors.Is(err, jobs.ErrRunFinalized) {
			return nil
		}
		return err
	}
	return nil
}
