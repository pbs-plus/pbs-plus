//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func (b *backupJob) runPreScript(ctx context.Context) error {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	if job.PreScript == "" {
		return nil
	}

	b.mu.RLock()
	task := b.scriptTask
	b.mu.RUnlock()
	if task != nil {
		task.SetState("RUNNING: pre-backup script")
	}
	b.logger.Info("running pre-backup script", "script", job.PreScript)

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	envVars, err := jobs.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	_, modEnvVars, err := jobs.RunShellScriptWithOutput(ctx, job.PreScript, envVars, func(line string) {
		b.logScriptLine(job.PreScript, line)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			b.logger.Info("pre-backup script canceled")
			return jobs.ErrCanceled
		}
		b.logger.Error(err,

			"error encountered while running job pre-backup script")

		return err
	}

	if newNs, ok := modEnvVars["PBS_PLUS__NAMESPACE"]; ok {
		b.mu.Lock()
		b.job.Namespace = newNs
		jobID := b.job.ID
		b.mu.Unlock()
		if err := b.app.CoreDB.UpdateBackupNamespace(jobID, newNs); err != nil {
			b.logger.Error(err, "failed to update backup namespace from pre-script")
		}
	}

	return nil
}

func (b *backupJob) runTargetMountScript(ctx context.Context, target coredb.Target) error {
	if target.MountScript == "" {
		return nil
	}

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	envVars, err := jobs.StructToEnvVars(target)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, _, err := jobs.RunShellScript(ctx, target.MountScript, envVars)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return jobs.ErrCanceled
		}
		b.logger.Error(err, "error encountered while running mount script")
	}
	b.logger.Info(scriptOut, "script", target.MountScript)

	return nil
}

func (b *backupJob) runPostScript(success bool, warningsNum int) {
	b.mu.RLock()
	job := b.job
	workerID := b.workerID
	workflowStart := b.workflowStart
	executionID := b.executionID
	b.mu.RUnlock()

	if job.PostScript == "" {
		return
	}

	task, taskErr := tasklog.NewQueuedTask("backup", workerID, false)
	if taskErr != nil {
		b.logger.Error(taskErr, "failed to create post-backup script task")
	} else {
		task.SetState("RUNNING: post-backup script")
		b.mu.Lock()
		b.scriptTask = task
		b.mu.Unlock()
		if err := updateBackupStatus(false, 0, job, proxmox.Task{UPID: task.UPID()}, executionID, workflowStart, 0, b.app); err != nil {
			b.logger.Error(err, "failed to assign post-backup script task to backup job")
		}
		defer func() {
			task.Close()
			b.mu.Lock()
			b.scriptTask = nil
			b.mu.Unlock()
		}()
	}

	b.logger.Info("running post-backup script", "script", job.PostScript)

	envVars, err := jobs.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_SUCCESS=%t", success))
	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_WARNINGS=%d", warningsNum))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, _, err = jobs.RunShellScriptWithOutput(ctx, job.PostScript, envVars, func(line string) {
		b.logScriptLine(job.PostScript, line)
	})
	if err != nil {
		b.logger.Error(err, "error encountered while running job post-backup script")
	}
}

func (b *backupJob) logScriptLine(script, line string) {
	b.logger.Info(line, "script", script)
	b.mu.RLock()
	task := b.scriptTask
	b.mu.RUnlock()
	if task != nil {
		task.LogString(line)
	}
}
