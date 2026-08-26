//go:build linux

package verification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/verification"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

type startResult struct {
	UPID string `json:"upid"`
}

type selectResult struct {
	BackupJobIDs []string `json:"backupJobIds"`
}

type verifyResult struct {
	TotalFiles   int `json:"totalFiles"`
	FailedFiles  int `json:"failedFiles"`
	SkippedFiles int `json:"skippedFiles"`
}

// Register registers the verification workflow: select,
// start-task, verify, finalize. Candidate order is pinned by the
// select activity; verify checkpoints its position across retries.
func Register(engine *jobs.Engine, app *application.Runtime) error {
	return engine.Register(jobs.WorkflowVerification, func(w *jobs.WorkflowContext) error {
		var input jobs.VerificationInput
		if err := json.Unmarshal(w.Execution.Payload, &input); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding verification workflow input: %w", err))
		}
		job, err := app.CoreDB.GetVerificationJob(w.Execution.DefinitionID)
		if err != nil {
			return jobs.NonRetryable(fmt.Errorf("getting verification workflow definition: %w", err))
		}
		return runWorkflow(w, app, job, input)
	})
}

func runWorkflow(w *jobs.WorkflowContext, app *application.Runtime, job coredb.VerificationJob, input jobs.VerificationInput) error {
	v := &verificationJob{
		job:    job,
		app:    app,
		logger: log.WithScope(log.Scope{JobID: job.ID}),
	}
	defer v.cleanup()

	selectResRaw, err := w.Activity("select", json.RawMessage(`{}`), func(ctx context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		backups := v.selectCandidates(ctx)
		if len(backups) == 0 {
			if job.TargetMode == "namespace" {
				return nil, fmt.Errorf("no agent backup jobs found in namespace '%s'", job.Namespace)
			}
			return nil, ErrNotAgentTarget
		}
		ids := make([]string, len(backups))
		for i, b := range backups {
			ids[i] = b.ID
		}
		return json.Marshal(selectResult{BackupJobIDs: ids})
	})
	if err != nil {
		return v.finalizeFailed(w, err)
	}
	var selectRes selectResult
	if err := json.Unmarshal(selectResRaw, &selectRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding verification select result: %w", err))
	}

	startResRaw, err := w.Activity("start-task", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		vTask, err := NewVerificationTask(job)
		if err != nil {
			return nil, err
		}
		v.mu.Lock()
		v.task = vTask
		v.mu.Unlock()
		if err := v.updateJobStatus(false, vTask.Task); err != nil {
			v.logger.Error(err, "failed to update job with task UPID")
		}
		return json.Marshal(startResult{UPID: vTask.UPID()})
	})
	if err != nil {
		return v.finalizeFailed(w, err)
	}
	var startRes startResult
	if err := json.Unmarshal(startResRaw, &startRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding verification start result: %w", err))
	}
	v.upid = startRes.UPID

	verifyResRaw, err := w.Activity("verify", json.RawMessage(`{}`), func(ctx context.Context, info jobs.ActivityInfo) (json.RawMessage, error) {
		if v.task == nil {
			vTask, err := ReopenVerificationTask(job, v.upid)
			if err != nil {
				return nil, err
			}
			v.mu.Lock()
			v.task = vTask
			v.mu.Unlock()
		}

		backups := make([]coredb.Backup, len(selectRes.BackupJobIDs))
		for i, id := range selectRes.BackupJobIDs {
			backup, err := app.CoreDB.GetBackup(id)
			if err != nil {
				return nil, fmt.Errorf("getting backup job %s: %w", id, err)
			}
			backups[i] = backup
		}
		v.mu.Lock()
		v.backupJobs = backups
		v.mu.Unlock()

		start := 0
		if len(info.ResumeCheckpoint) > 0 {
			var cp struct {
				Next int `json:"next"`
			}
			if err := json.Unmarshal(info.ResumeCheckpoint, &cp); err == nil {
				start = cp.Next
			}
		}

		if err := v.verifyCandidates(ctx, backups, start, info); err != nil {
			return nil, err
		}

		v.mu.RLock()
		defer v.mu.RUnlock()
		return json.Marshal(verifyResult{
			TotalFiles:   v.totalFiles,
			FailedFiles:  v.failedFiles,
			SkippedFiles: v.skippedFiles,
		})
	})
	if err != nil {
		return v.finalizeFailed(w, err)
	}
	var verifyRes verifyResult
	if err := json.Unmarshal(verifyResRaw, &verifyRes); err != nil {
		return jobs.NonRetryable(fmt.Errorf("decoding verification result: %w", err))
	}
	v.mu.Lock()
	v.totalFiles = verifyRes.TotalFiles
	v.failedFiles = verifyRes.FailedFiles
	v.skippedFiles = verifyRes.SkippedFiles
	v.mu.Unlock()

	_, err = w.Activity("finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if v.task == nil {
			if vTask, err := ReopenVerificationTask(job, v.upid); err == nil {
				v.mu.Lock()
				v.task = vTask
				v.mu.Unlock()
			}
		}
		v.finalizeSuccess()
		return json.RawMessage(`{}`), nil
	})
	return err
}

// verifyCandidates walks the pinned candidate list from `start`,
// checkpointing candidates that were consumed so retries resume at the
// next candidate instead of re-verifying completed ones.
func (v *verificationJob) verifyCandidates(ctx context.Context, backups []coredb.Backup, start int, info jobs.ActivityInfo) error {
	vTask := v.task
	if vTask == nil {
		return fmt.Errorf("verification task not started")
	}
	if v.job.TargetMode == "namespace" {
		vTask.WriteString(fmt.Sprintf("starting verification job '%s' targeting namespace '%s' (%d backup jobs)", v.job.ID, v.job.Namespace, len(backups)))
	} else {
		vTask.WriteString(fmt.Sprintf("starting verification job '%s' for backup job '%s'", v.job.ID, v.job.BackupJobID))
	}

	var lastStartupErr error
	for i := start; i < len(backups); i++ {
		backup := backups[i]
		consumed := func() {
			cp, err := json.Marshal(struct {
				Next int `json:"next"`
			}{Next: i + 1})
			if err == nil {
				_ = info.Checkpoint(ctx, cp)
			}
		}

		snapshot, snapErr := v.selectSnapshot(ctx, v.job, backup)
		if snapErr != nil {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': failed to select snapshot: %v", backup.ID, snapErr))
			lastStartupErr = snapErr
			consumed()
			continue
		}

		hostname := backup.Target.GetHostname()
		streamID := hostname + "|" + v.job.ID + "|verify"

		type caller interface {
			CallMessage(ctx context.Context, method string, payload any) (string, error)
		}
		var controlSess caller
		if sess, ok := v.app.Agents.GetQuicPipe(hostname); ok {
			controlSess = sess
		} else if sess, ok := v.app.Agents.GetStreamPipe(hostname); ok {
			controlSess = sess
		} else {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': agent '%s' not connected", backup.ID, hostname))
			lastStartupErr = ErrAgentNotConnected
			consumed()
			continue
		}

		v.app.Agents.Expect(streamID)

		verifyReq := verification.VerifyStartReq{VerifyID: v.job.ID}
		forkCtx, forkCancel := context.WithTimeout(ctx, 30*time.Second)
		_, forkErr := controlSess.CallMessage(forkCtx, "verify_start", &verifyReq)
		forkCancel()
		if forkErr != nil {
			v.app.Agents.NotExpect(streamID)
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': failed to fork verification worker: %v", backup.ID, forkErr))
			lastStartupErr = forkErr
			consumed()
			continue
		}

		pipeCtx, pipeCancel := context.WithTimeout(ctx, 30*time.Second)
		agentTCP, waitErr := v.app.Agents.WaitStreamPipe(pipeCtx, streamID)
		pipeCancel()
		if waitErr != nil {
			v.app.Agents.NotExpect(streamID)
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': verification worker did not connect: %v", backup.ID, waitErr))
			lastStartupErr = waitErr
			consumed()
			continue
		}
		v.app.Agents.NotExpect(streamID)

		vTask.WriteString(fmt.Sprintf("verification worker connected via TCP for job '%s'", backup.ID))

		vs, archiveErr := v.openArchive(backup, snapshot)
		if archiveErr != nil {
			agentTCP.Close()
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s' snapshot '%s': failed to open archive: %v", backup.ID, snapshot.Snapshot, archiveErr))
			lastStartupErr = archiveErr
			consumed()
			continue
		}

		vTask.WriteString(fmt.Sprintf("selected backup job '%s', snapshot: %s", backup.ID, snapshot.Snapshot))

		err := v.executeVerification(ctx, vTask, v.job, backup, snapshot, vs, agentTCP)
		if err == nil {
			consumed()
			return nil
		}

		if errors.Is(err, ErrNoFilesToVerify) && v.job.TargetMode == "namespace" {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': no eligible files found, trying next candidate", backup.ID))
			lastStartupErr = err
			consumed()
			continue
		}
		return err
	}

	if lastStartupErr != nil {
		vTask.WriteString(fmt.Sprintf("all candidates exhausted, last error: %v", lastStartupErr))
		return lastStartupErr
	}
	return fmt.Errorf("no eligible backup jobs found")
}

func (v *verificationJob) finalizeFailed(w *jobs.WorkflowContext, runErr error) error {
	if !errors.Is(runErr, context.Canceled) && !jobs.IsFinalAttempt(w.Execution) {
		return runErr
	}

	ctx := w.Detached()
	_, err := w.ActivityCtx(ctx, "finalize", json.RawMessage(`{}`), func(_ context.Context, _ jobs.ActivityInfo) (json.RawMessage, error) {
		if v.task == nil && v.upid != "" {
			if vTask, terr := ReopenVerificationTask(v.job, v.upid); terr == nil {
				v.mu.Lock()
				v.task = vTask
				v.mu.Unlock()
			}
		}
		v.finalizeFailure(runErr)
		return json.RawMessage(`{}`), nil
	})
	if err != nil {
		v.logger.Error(err, "failed to run verification failure finalizer")
	}
	return runErr
}
