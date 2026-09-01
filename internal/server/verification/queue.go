//go:build linux

package verification

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

func queuedWorkerID(job coredb.VerificationJob) string {
	return proxmox.EncodeToHexEscapes(job.ID)
}

// PrepareQueue mints the queued task at submit time so a slot-waiting job is
// visible (log, history, stoppable); the workflow's NewQueuedTask attaches by key.
func PrepareQueue(app *application.Runtime, job coredb.VerificationJob, web bool) error {
	queued, err := tasklog.NewQueuedTask("verification", queuedWorkerID(job), web)
	if err != nil {
		return err
	}
	queued.OnAbort(func() { _ = CancelQueued(app, job) })
	current, err := app.CoreDB.GetVerificationJob(job.ID)
	if err != nil {
		return err
	}
	current.History.LastRunUpid = queued.UPID()
	current.History.LastRunStarttime = queued.Task.StartTime
	current.History.LastRunState = ""
	return app.CoreDB.UpdateVerificationJob(nil, current)
}

// CancelQueued stops a queued job: pre-claim it closes the task as canceled
// (JobStatusCanceled, retry counter untouched); claimed ones finalize normally.
func CancelQueued(app *application.Runtime, job coredb.VerificationJob) error {
	ctx := context.Background()
	exec, err := app.Engine.ActiveExecution(ctx, jobs.WorkflowVerification, job.ID)
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
	queued, err := tasklog.NewQueuedTask("verification", queuedWorkerID(job), false)
	if err != nil {
		return err
	}
	queued.CloseErr(jobs.ErrCanceled)
	current, err := app.CoreDB.GetVerificationJob(job.ID)
	if err != nil {
		return err
	}
	current.History.LastRunUpid = queued.UPID()
	current.History.LastRunStarttime = queued.Task.StartTime
	current.History.LastRunEndtime = time.Now().Unix()
	current.History.LastRunState = "operation canceled"
	current.History.LastRunStatus = coredb.JobStatusCanceled
	return app.CoreDB.UpdateVerificationJob(nil, current)
}
