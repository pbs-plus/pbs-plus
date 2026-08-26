//go:build linux

package restore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
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

// Register registers the restore workflow: queue, pre-script,
// start-task, run, finalize. Each stage is a durable activity.
func Register(engine *jobs.Engine, app *application.Runtime) error {
	return engine.Register(jobs.WorkflowRestore, func(w *jobs.WorkflowContext) error {
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
		web:       input.Web,
		waitGroup: &sync.WaitGroup{},
		logger:    log.WithScope(log.Scope{JobID: job.ID}),
	}
	defer b.cleanup()

	stage := func(name string, body func(context.Context) error) error {
		_, err := w.Activity(name, json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
			if err := body(ctx); err != nil {
				return nil, err
			}
			return json.RawMessage(`{}`), nil
		})
		return err
	}

	if err := stage("queue", b.enqueue); err != nil {
		return b.finalizeFailed(w, err)
	}
	if err := stage("pre-script", b.runPreScript); err != nil {
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

	runResRaw, err := w.Activity("run", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if b.task == nil {
			task, err := ReopenRestoreTask(job, b.upid)
			if err != nil {
				return nil, err
			}
			b.mu.Lock()
			b.task = task
			b.mu.Unlock()
		}
		if err := b.execute(ctx); err != nil {
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

	_, err = w.Activity("finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if b.task == nil {
			task, err := ReopenRestoreTask(job, b.upid)
			if err == nil {
				b.mu.Lock()
				b.task = task
				b.mu.Unlock()
			}
		}
		b.finalizeSuccess()
		return json.RawMessage(`{}`), nil
	})
	return err
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

	ctx := w.Detached()
	_, err := w.ActivityCtx(ctx, "finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if b.task == nil && b.upid != "" {
			if task, err := ReopenRestoreTask(b.job, b.upid); err == nil {
				b.mu.Lock()
				b.task = task
				b.mu.Unlock()
			}
		}
		b.finalizeFailure(runErr)
		return json.RawMessage(`{}`), nil
	})
	if err != nil {
		b.logger.Error(err, "failed to run restore failure finalizer")
	}
	return runErr
}
