//go:build linux

package restore

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
func PrepareQueue(app *application.Runtime, job coredb.Restore, web bool) error {
	workerID := tasklog.FormatWorkerID(job.Store, "host-", job.DestTarget.GetHostname())
	queued, err := tasklog.NewQueuedTask("reader", workerID, web)
	if err != nil {
		return err
	}
	queued.OnAbort(func() { _ = CancelQueued(app, job) })
	return updateRestoreStatus(false, 0, job, proxmox.Task{UPID: queued.UPID()}, app)
}

// CancelQueued stops a queued job: pre-claim it closes the task as canceled
// (JobStatusCanceled, retry counter untouched); claimed ones finalize normally.
func CancelQueued(app *application.Runtime, job coredb.Restore) error {
	ctx := context.Background()
	exec, err := app.Engine.ActiveExecution(ctx, jobs.WorkflowRestore, job.ID)
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
	workerID := tasklog.FormatWorkerID(job.Store, "host-", job.DestTarget.GetHostname())
	queued, err := tasklog.NewQueuedTask("reader", workerID, false)
	if err != nil {
		return err
	}
	queued.CloseErr(jobs.ErrCanceled)
	task := proxmox.Task{UPID: queued.UPID(), StartTime: queued.Task.StartTime, EndTime: time.Now().Unix()}
	return updateRestoreStatus(false, 0, job, task, app)
}
