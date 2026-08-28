//go:build linux

package jobs

import (
	"context"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

type Abortable interface {
	OnAbort(func())
}

type abortBinderKey struct{}

type abortBinder struct {
	engine      *Engine
	executionID string
}

// BindTask makes a PBS stop request on task cancel the execution running ctx.
func BindTask(ctx context.Context, task Abortable) {
	binder, ok := ctx.Value(abortBinderKey{}).(abortBinder)
	if !ok {
		return
	}
	binder.engine.BindTaskAbort(binder.executionID, task)
}

// BindTask cancels this workflow's execution when task is stopped.
func (w *WorkflowContext) BindTask(task Abortable) {
	if w.engine == nil {
		return
	}
	w.engine.BindTaskAbort(w.Execution.ID, task)
}

func (e *Engine) BindTaskAbort(executionID string, task Abortable) {
	if executionID == "" || task == nil {
		return
	}
	task.OnAbort(func() {
		execution, err := e.Cancel(context.Background(), executionID)
		if err != nil {
			log.Error(err, "workflow engine task abort failed", "executionID", executionID)
			return
		}
		if execution.State == jobdb.StateCanceled {
			if closer, ok := task.(interface{ CloseErr(error) }); ok {
				closer.CloseErr(context.Canceled)
			}
		}
	})
}

func (e *Engine) withAbortBinder(ctx context.Context, executionID string) context.Context {
	return context.WithValue(ctx, abortBinderKey{}, abortBinder{engine: e, executionID: executionID})
}
