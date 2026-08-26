//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

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

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		if err := qt.UpdateDescription("running pre-backup script"); err != nil {
			b.logger.Error(err, "failed to update queue task description")
		}
	}

	envVars, err := jobs.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, modEnvVars, err := jobs.RunShellScript(ctx, job.PreScript, envVars)
	b.logger.Info(scriptOut, "script", job.PreScript)
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
		latestBackup, err := b.app.CoreDB.GetBackup(b.job.ID)
		if err == nil {
			b.job = latestBackup
		}
		b.job.Namespace = newNs
		if err := b.app.CoreDB.UpdateBackup(nil, b.job); err != nil {
			b.logger.Error(err, "failed to update backup namespace from pre-script")
		}
		b.mu.Unlock()
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

	b.mu.RLock()
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		if err := qt.UpdateDescription("running target mount script"); err != nil {
			b.logger.Error(err, "failed to update queue task description")
		}
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
	b.mu.RUnlock()

	if job.PostScript == "" {
		return
	}

	b.mu.RLock()
	job = b.job
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		if err := qt.UpdateDescription("running post-backup script"); err != nil {
			b.logger.Error(err, "failed to update queue task description")
		}
	}
	b.logger.Info("running post-backup script",
		"script", job.PostScript)

	envVars, err := jobs.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_SUCCESS=%t", success))
	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_WARNINGS=%d", warningsNum))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	scriptOut, _, err := jobs.RunShellScript(ctx, job.PostScript, envVars)
	if err != nil {
		b.logger.Error(err,
			"error encountered while running job post-backup script")

	}
	b.logger.Info(scriptOut,
		"script", job.PostScript)

}
