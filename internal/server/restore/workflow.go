//go:build linux

package restore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

type startResult struct {
	UPID string `json:"upid"`
}

type runResult struct {
	ErrCount int32 `json:"errCount"`
}

// Register registers the restore workflow: pre-script,
// start-task, run, finalize. Each stage is a durable activity.
func Register(engine *jobs.Engine, app *application.Runtime) error {
	return engine.RegisterVersion(jobs.WorkflowRestore, "1", func(w *jobs.WorkflowContext) error {
		var input jobs.RestoreInput
		if err := json.Unmarshal(w.Execution.Payload, &input); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding restore workflow input: %w", err))
		}
		job, err := app.CoreDB.GetRestore(w.Execution.DefinitionID)
		if err != nil {
			return jobs.NonRetryable(fmt.Errorf("getting restore workflow definition: %w", err))
		}
		return runWorkflow(w, app, job, input)
	})
}

func runWorkflow(w *jobs.WorkflowContext, app *application.Runtime, job coredb.Restore, input jobs.RestoreInput) error {
	b := &restoreJob{
		job:       job,
		app:       app,
		skipCheck: input.SkipCheck,
		waitGroup: &sync.WaitGroup{},
		logger:    log.WithScope(log.Scope{JobID: job.ID}),
	}
	defer b.cleanup()
	queued, err := tasklog.NewQueuedTask("reader", tasklog.FormatWorkerID(job.Store, "host-", job.DestTarget.GetHostname()), input.Web)
	if err != nil {
		return fmt.Errorf("creating queued restore task: %w", err)
	}
	defer queued.Close()
	w.BindTask(queued)

	if err := updateRestoreStatus(false, 0, job, proxmox.Task{UPID: queued.UPID()}, app); err != nil {
		b.logger.Error(err, "failed to assign queued task to restore job")
	}

	if err := w.Step("pre-script", b.runPreScript); err != nil {
		return b.finalizeFailed(w, err)
	}

	startResRaw, err := w.Activity("start-task", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		task, err := GetRestoreTask(job)
		if err != nil {
			return nil, err
		}
		b.mu.Lock()
		b.task = task
		b.mu.Unlock()
		w.BindTask(task)
		return json.Marshal(startResult{UPID: task.UPID()})
	})
	if err != nil {
		return b.finalizeFailed(w, err)
	}
	var startRes startResult
	if err := json.Unmarshal(startResRaw, &startRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding restore start result: %w", err))
	}
	b.upid = startRes.UPID
	queued.Close()
	if err := updateRestoreStatus(false, 0, job, proxmox.Task{UPID: startRes.UPID}, app); err != nil {
		b.logger.Error(err, "failed to assign restore task to job", "upid", startRes.UPID)
	}

	runResRaw, err := w.Activity("run", json.RawMessage(`{}`), func(ctx context.Context, info jobs.ActivityInfo) (json.RawMessage, error) {
		if b.task == nil {
			task, err := ReopenRestoreTask(job, b.upid)
			if err != nil {
				return nil, err
			}
			b.mu.Lock()
			b.task = task
			b.mu.Unlock()
			w.BindTask(task)
		}
		if err := b.execute(ctx, info.IdempotencyKey); err != nil {
			return nil, err
		}
		return json.Marshal(runResult{ErrCount: b.errCount.Load()})
	})
	if err != nil {
		return b.finalizeFailed(w, err)
	}
	var runRes runResult
	if err := json.Unmarshal(runResRaw, &runRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding restore run result: %w", err))
	}
	b.errCount.Store(runRes.ErrCount)

	return w.Step("finalize", func(context.Context) error {
		if b.task == nil {
			task, err := ReopenRestoreTask(job, b.upid)
			if err == nil {
				b.mu.Lock()
				b.task = task
				b.mu.Unlock()
			}
		}
		b.finalizeSuccess()
		return nil
	})
}

func (b *restoreJob) finalizeFailed(w *jobs.WorkflowContext, runErr error) error {
	if errors.Is(runErr, jobs.ErrOneInstance) {
		return nil
	}
	if errors.Is(runErr, jobs.ErrMountEmpty) {
		b.createOK(runErr)
		return nil
	}
	if !errors.Is(runErr, context.Canceled) && !jobs.IsFinalAttempt(w.Execution) {
		return runErr
	}

	err := w.Finalize(func(context.Context) error {
		if b.task == nil && b.upid != "" {
			if task, err := ReopenRestoreTask(b.job, b.upid); err == nil {
				b.mu.Lock()
				b.task = task
				b.mu.Unlock()
			}
		}
		b.finalizeFailure(runErr)
		return nil
	})
	if err != nil {
		b.logger.Error(err, "failed to run restore failure finalizer")
	}
	return runErr
}
