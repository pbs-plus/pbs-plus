//go:build linux

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

// ErrTaskFailed marks a PBS backup task that terminated unsuccessfully;
// the workflow invalidates the start activity so a retry launches a
// fresh backup instead of re-watching the dead task.
var ErrTaskFailed = errors.New("backup task failed")

type startResult struct {
	UPID  string `json:"upid"`
	Owner string `json:"owner"`
}

type waitResult struct {
	Succeeded bool `json:"succeeded"`
	Warnings  int  `json:"warnings"`
}

// Register registers the backup workflow: queue, pre-script, validate,
// mount-script, start, wait, finalize. Each stage is a durable
// activity; completed stages are skipped on replay after a crash.
func Register(engine *jobs.Engine, storeInstance *store.Store) error {
	return engine.Register(jobs.WorkflowBackup, func(w *jobs.WorkflowContext) error {
		var input jobs.BackupInput
		if err := json.Unmarshal(w.Execution.Payload, &input); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding backup workflow input: %w", err))
		}
		job, err := storeInstance.Database.GetBackup(w.Execution.DefinitionID)
		if err != nil {
			return jobs.NonRetryable(fmt.Errorf("getting backup workflow definition: %w", err))
		}
		return runWorkflow(w, storeInstance, job, input)
	})
}

func runWorkflow(w *jobs.WorkflowContext, storeInstance *store.Store, job coredb.Backup, input jobs.BackupInput) error {
	b := &backupJob{
		job:             job,
		storeInstance:   storeInstance,
		skipCheck:       input.SkipCheck,
		web:             input.Web,
		logger:          log.WithScope(log.Scope{JobID: job.ID}),
		extraExclusions: input.ExtraExclusions,
		waitGroup:       &sync.WaitGroup{},
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
	if err := stage("validate", b.validateTargetConnection); err != nil {
		return b.finalizeFailed(w, err)
	}
	if err := stage("mount-script", func(ctx context.Context) error {
		return b.runTargetMountScript(ctx, job.Target)
	}); err != nil {
		return b.finalizeFailed(w, err)
	}

	startResRaw, err := w.Activity("start", json.RawMessage(`{}`), func(ctx context.Context, info jobs.ActivityInfo) (json.RawMessage, error) {
		return b.start(ctx, info)
	})
	if err != nil {
		return b.finalizeFailed(w, err)
	}
	var startRes startResult
	if err := json.Unmarshal(startResRaw, &startRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding backup start result: %w", err))
	}
	b.upid = startRes.UPID

	waitResRaw, err := w.Activity("wait", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if err := b.waitForCompletion(ctx, b.cmd, startRes.UPID); err != nil {
			return nil, err
		}
		succeeded, warnings := b.processPBSLogs(nil, b.upid)
		if !succeeded {
			return nil, fmt.Errorf("%w: upid %s", ErrTaskFailed, startRes.UPID)
		}
		return json.Marshal(waitResult{Succeeded: succeeded, Warnings: warnings})
	})
	if err != nil {
		if errors.Is(err, ErrTaskFailed) {
			if invErr := w.Invalidate("start"); invErr != nil {
				b.logger.Error(invErr, "failed to invalidate start activity")
			}
		}
		return b.finalizeFailed(w, err)
	}
	var waitRes waitResult
	if err := json.Unmarshal(waitResRaw, &waitRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding backup wait result: %w", err))
	}

	_, err = w.Activity("finalize", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		b.finalizeSuccess()
		return json.RawMessage(`{}`), nil
	})
	return err
}

// start mounts the source and launches proxmox-backup-client, returning
// the durable task identity for the wait activity.
func (b *backupJob) start(ctx context.Context, info jobs.ActivityInfo) (json.RawMessage, error) {
	if qt := b.queueTask; qt != nil {
		if err := qt.UpdateDescription("operation ready, waiting for queue to free up"); err != nil {
			b.logger.Error(err, "failed to update queue task description")
		}
	}

	srcPath, agentMount, s3Mount, err := b.mountSource(ctx, b.job.Target)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.srcPath = srcPath
	b.agentMount = agentMount
	b.s3Mount = s3Mount
	b.mu.Unlock()

	cmd, task, currOwner, err := b.startBackup(ctx, srcPath, b.job.Target)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.Task = task
	b.currOwner = currOwner
	b.cmd = cmd
	b.mu.Unlock()

	if err := updateBackupStatus(false, 0, b.job, task, b.storeInstance); err != nil {
		if currOwner != "" {
			if err := SetDatastoreOwner(b.job, b.storeInstance, currOwner); err != nil {
				b.logger.Error(err, "failed to update backup status after task creation")
			}
		}
	}

	b.started.Store(true)
	b.logger.Info("backup task started", "upid", task.UPID)

	if cmd.Process != nil {
		checkpoint, err := json.Marshal(map[string]int{"pid": cmd.Process.Pid})
		if err == nil {
			_ = info.Checkpoint(ctx, checkpoint)
		}
	}

	return json.Marshal(startResult{UPID: task.UPID, Owner: currOwner})
}

// finalizeFailed runs the failure finalizer exactly once: on cancel or
// when the attempt budget is exhausted. Earlier attempts return the
// error so the engine retries the failed stage.
func (b *backupJob) finalizeFailed(w *jobs.WorkflowContext, runErr error) error {
	if errors.Is(runErr, jobs.ErrOneInstance) || errors.Is(runErr, jobs.ErrMountEmpty) {
		if errors.Is(runErr, jobs.ErrMountEmpty) {
			b.createOK(runErr)
		}
		return nil
	}
	if !errors.Is(runErr, context.Canceled) && !jobs.IsFinalAttempt(w.Execution) {
		return runErr
	}

	ctx := w.Detached()
	_, err := w.ActivityCtx(ctx, "finalize", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		b.finalizeFailure(runErr)
		return json.RawMessage(`{}`), nil
	})
	if err != nil {
		b.logger.Error(err, "failed to run backup failure finalizer")
	}
	return runErr
}
