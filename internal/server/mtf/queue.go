//go:build linux

package mtf

import (
	"context"
	"errors"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
)

// PrepareQueue mints the queued task at submit time so a slot-waiting job is
// visible (log, history, stoppable); the workflow's NewQueuedTask attaches by key.
func PrepareQueue(app *application.Runtime, jobID string, web bool) error {
	ctx := app.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	exec, err := app.Engine.ActiveExecution(ctx, jobs.WorkflowMtfMigration, jobID)
	if errors.Is(err, jobdb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	job, err := app.MtfDB.GetMtfJob(ctx, jobID)
	if err != nil {
		return err
	}
	queued, err := tasklog.NewQueuedTask(mtfWorkerType, tasklog.FormatWorkerID(job.Datastore, "mtf-", job.ID), web)
	if err != nil {
		return err
	}
	queued.OnAbort(func() { _ = CancelQueued(app, jobID) })
	if jobs.RunFinalized(job.ID, exec.ID) {
		queued.Close()
		return nil
	}
	h := mtfdb.JobHistory{
		LastRunUpid:      queued.UPID(),
		LastRunStatus:    coredb.JobStatusUnknown,
		LastRunStarttime: queued.Task.StartTime,
	}
	return app.MtfDB.UpdateMtfJobHistory(ctx, job.ID, h, "")
}

// CancelQueued stops a queued job: pre-claim it closes the task as canceled
// (JobStatusCanceled, retry counter untouched); claimed ones finalize normally.
func CancelQueued(app *application.Runtime, jobID string) error {
	ctx := app.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	exec, err := app.Engine.ActiveExecution(ctx, jobs.WorkflowMtfMigration, jobID)
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
	job, err := app.MtfDB.GetMtfJob(ctx, jobID)
	if err != nil {
		return err
	}
	queued, err := tasklog.NewQueuedTask(mtfWorkerType, tasklog.FormatWorkerID(job.Datastore, "mtf-", job.ID), false)
	if err != nil {
		return err
	}
	queued.CloseErr(jobs.ErrCanceled)
	h := mtfdb.JobHistory{
		LastRunUpid:      queued.UPID(),
		LastRunStatus:    coredb.JobStatusCanceled,
		LastRunStarttime: queued.Task.StartTime,
		LastRunEndtime:   time.Now().Unix(),
	}
	if err := app.MtfDB.UpdateMtfJobHistory(ctx, job.ID, h, ""); err != nil {
		return err
	}
	jobs.MarkRunFinalized(job.ID, exec.ID)
	return nil
}
