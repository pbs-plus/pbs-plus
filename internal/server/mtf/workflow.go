//go:build linux

package mtf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

type taskResult struct {
	UPID string `json:"upid"`
}

// RegisterMigration registers the MTF migration workflow: start-task,
// run, finalize. Task creation is exactly-once; the tape run reattaches
// to the task log by UPID across retries.
func RegisterMigration(engine *jobs.Engine, app *application.Runtime) error {
	return engine.Register(jobs.WorkflowMtfMigration, func(w *jobs.WorkflowContext) error {
		j, err := newMigrationJob(w.Execution.DefinitionID, app)
		if err != nil {
			return jobs.NonRetryable(err)
		}
		return runMigration(w, j)
	})
}

func runMigration(w *jobs.WorkflowContext, j *mtfJob) error {
	defer j.cleanup()
	queued, err := tasklog.NewQueuedTask(mtfWorkerType, tasklog.FormatWorkerID(j.job.Datastore, "mtf-", j.job.ID), w.Execution.Trigger == "manual")
	if err != nil {
		return fmt.Errorf("creating queued MTF migration task: %w", err)
	}
	defer queued.Close()

	startResRaw, err := w.Activity("start-task", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		task, err := startTask(j.job)
		if err != nil {
			return nil, fmt.Errorf("start task: %w", err)
		}
		j.mu.Lock()
		j.task = task
		j.mu.Unlock()
		return json.Marshal(taskResult{UPID: task.UPID()})
	})
	if err != nil {
		return j.finalizeFailed(w, "", err)
	}
	var startRes taskResult
	if err := json.Unmarshal(startResRaw, &startRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding mtf start result: %w", err))
	}
	queued.Close()

	_, err = w.Activity("run", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if j.task == nil {
			if err := j.reattach(startRes.UPID); err != nil {
				return nil, err
			}
		}
		if err := j.execute(ctx); err != nil {
			return nil, err
		}
		return json.RawMessage(`{}`), nil
	})
	if err != nil {
		return j.finalizeFailed(w, startRes.UPID, err)
	}

	_, err = w.Activity("finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if j.task == nil {
			if err := j.reattach(startRes.UPID); err != nil {
				return nil, err
			}
		}
		j.finalizeSuccess()
		return json.RawMessage(`{}`), nil
	})
	return err
}

func (j *mtfJob) finalizeFailed(w *jobs.WorkflowContext, upid string, runErr error) error {
	if !errors.Is(runErr, context.Canceled) && !jobs.IsFinalAttempt(w.Execution) {
		return runErr
	}

	ctx := w.Detached()
	_, err := w.ActivityCtx(ctx, "finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if j.task == nil && upid != "" {
			_ = j.reattach(upid)
		}
		j.finalizeFailure(runErr)
		return json.RawMessage(`{}`), nil
	})
	if err != nil {
		j.logger.Error(err, "failed to run mtf failure finalizer")
	}
	return runErr
}

// RegisterScan registers the MTF inventory scan workflow: start-task,
// scan, finalize, under the global mtf-scan resource lock.
func RegisterScan(engine *jobs.Engine, app *application.Runtime) error {
	return engine.Register(jobs.WorkflowMtfScan, func(w *jobs.WorkflowContext) error {
		var input jobs.MtfScanInput
		if err := json.Unmarshal(w.Execution.Payload, &input); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding mtf scan workflow input: %w", err))
		}
		if app.MtfDB == nil {
			return jobs.NonRetryable(fmt.Errorf("mtf store unavailable"))
		}
		opts := Options{
			ChangerDevice: input.ChangerDevice,
			TapeDevice:    input.TapeDevice,
			DriveIndex:    input.DriveIndex,
			BKFPath:       input.BKFPath,
			Label:         input.Label,
			Barcodes:      input.Barcodes,
		}
		return runScan(w, app, opts)
	})
}

func runScan(w *jobs.WorkflowContext, app *application.Runtime, opts Options) error {
	var task *ScanTask
	queued, err := tasklog.NewQueuedTask("mtfscan", scanWID(opts), w.Execution.Trigger == "manual")
	if err != nil {
		return fmt.Errorf("creating queued MTF scan task: %w", err)
	}
	defer queued.Close()

	startResRaw, err := w.Activity("start-task", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		var err error
		task, err = NewScanTask(opts)
		if err != nil {
			return nil, fmt.Errorf("create scan task: %w", err)
		}
		return json.Marshal(taskResult{UPID: task.UPID()})
	})
	if err != nil {
		return finalizeScanFailed(w, task, "", err)
	}
	var startRes taskResult
	if err := json.Unmarshal(startResRaw, &startRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding mtf scan start result: %w", err))
	}
	queued.Close()

	scanResRaw, err := w.Activity("run", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if task == nil {
			wt, err := tasklog.ReopenWorkerTask(startRes.UPID)
			if err != nil {
				return nil, err
			}
			task = &ScanTask{WorkerTask: wt}
		}
		return runScanActivity(ctx, app, opts, task)
	})
	if err != nil {
		return finalizeScanFailed(w, task, startRes.UPID, err)
	}
	var scanRes Result
	if err := json.Unmarshal(scanResRaw, &scanRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding mtf scan result: %w", err))
	}

	_, err = w.Activity("finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if task == nil {
			wt, err := tasklog.ReopenWorkerTask(startRes.UPID)
			if err != nil {
				return nil, err
			}
			task = &ScanTask{WorkerTask: wt}
		}
		task.CloseOK(&scanRes)
		return json.RawMessage(`{}`), nil
	})
	return err
}

func runScanActivity(ctx context.Context, app *application.Runtime, opts Options, st *ScanTask) (result json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("scan panic: %v", r)
			st.LogString(err.Error())
		}
	}()

	st.LogString("MTF inventory scan started")
	res, err := NewScanner(app.MtfDB).ScanWithLog(ctx, opts, log.WithScope(log.Scope{Task: st.WorkerTask}))
	if err != nil {
		st.LogString(err.Error())
		return nil, err
	}
	return json.Marshal(res)
}

func finalizeScanFailed(w *jobs.WorkflowContext, task *ScanTask, upid string, runErr error) error {
	if !errors.Is(runErr, context.Canceled) && !jobs.IsFinalAttempt(w.Execution) {
		return runErr
	}
	if upid == "" {
		return runErr
	}

	ctx := w.Detached()
	_, err := w.ActivityCtx(ctx, "finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if task == nil {
			wt, err := tasklog.ReopenWorkerTask(upid)
			if err != nil {
				return nil, err
			}
			task = &ScanTask{WorkerTask: wt}
		}
		task.CloseErr(runErr)
		return json.RawMessage(`{}`), nil
	})
	if err != nil {
		log.Error(err, "failed to run MTF scan failure finalizer", "upid", upid)
	}
	return runErr
}
