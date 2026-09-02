//go:build linux

package backup

import (
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
)

func (b *backupJob) finalizeFailure(err error) {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()
	b.logger.Error(err, "backup job failed")

	if errors.Is(err, jobs.ErrOneInstance) {
		return
	}

	if errors.Is(err, jobs.ErrMountEmpty) {
		b.createOK(err)
		return
	}

	if b.started.Load() {
		b.waitGroup.Wait()
		succeeded, warningsNum := b.processPBSLogs(err, b.upid)
		b.logger.Info("backup completed, running post-backup script")
		b.runPostScript(succeeded, warningsNum)
		succeeded, warningsNum = b.processPBSLogs(err, b.upid)
		b.updatePBSStatus(succeeded, warningsNum, b.upid)

		var notifyErr error
		if !succeeded {
			notifyErr = fmt.Errorf("backup failed: %w", err)
		}
		if b.app.BatchTracker != nil {
			b.app.BatchTracker.RecordJobResult(
				job.NotificationMode,
				notification.JobTypeBackup,
				job.ID,
				job.Store,
				notifyErr,
				map[string]string{
					"target":    job.Target.Name,
					"succeeded": fmt.Sprintf("%v", succeeded),
					"warnings":  fmt.Sprintf("%d", warningsNum),
				},
			)
		}
		return
	}

	if flushErr := b.logger.FlushJobLog(); flushErr != nil {
		b.logger.Error(flushErr, "failed to flush backup job log")
	}
	task, terr := GenerateBackupTaskErrorFile(
		b.job,
		err,
		[]string{
			"Error handling from a scheduled job run request",
			"Backup ID: " + job.ID,
			"Source Mode: " + job.SourceMode,
		},
		b.logger.JobLogPath(),
	)
	if terr != nil {
		b.logger.Error(terr, "failed to generate backup task error file")
	} else {
		b.updateBackupWithTask(task)
	}

	if b.app.BatchTracker != nil {
		b.app.BatchTracker.RecordJobResult(
			job.NotificationMode,
			notification.JobTypeBackup,
			job.ID,
			job.Store,
			fmt.Errorf("backup failed to start: %w", err),
			map[string]string{
				"target":    job.Target.Name,
				"succeeded": "false",
				"phase":     "pre-start",
			},
		)
	}
}

func (b *backupJob) finalizeSuccess(succeeded bool, warningsNum int) {
	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	currOwner := b.currOwner
	b.mu.RUnlock()

	for _, ext := range extraExclusions {
		b.logger.Warn(fmt.Sprintf("skipped %s due to an error from previous retry attempts", ext))

	}

	b.waitGroup.Wait()

	if currOwner != "" {
		b.logger.Info("setting owner to datastore owner")
		if err := SetDatastoreOwner(job, b.app, currOwner); err != nil {
			b.logger.Error(err, "failed to set datastore owner")
		}
	}
	b.runPostScript(succeeded, warningsNum)
	succeeded, warningsNum = b.processPBSLogs(nil, b.upid)
	b.updatePBSStatus(succeeded, warningsNum, b.upid)

	var notifyErr error
	if !succeeded {
		notifyErr = fmt.Errorf("backup failed")
	}
	if b.app.BatchTracker != nil {
		b.app.BatchTracker.RecordJobResult(
			job.NotificationMode,
			notification.JobTypeBackup,
			job.ID,
			job.Store,
			notifyErr,
			map[string]string{
				"target":    job.Target.Name,
				"succeeded": fmt.Sprintf("%v", succeeded),
				"warnings":  fmt.Sprintf("%d", warningsNum),
			},
		)
	}

	if succeeded && b.app.OnBackupComplete != nil {
		go b.app.OnBackupComplete(b.job.ID)
	}
}

func (b *backupJob) cleanup() {
	b.cleanupOnce.Do(func() {
		b.waitGroup.Wait()

		b.mu.Lock()
		b.job.CurrentPID = 0
		agentMount := b.agentMount
		s3Mount := b.s3Mount
		stagedDump := b.stagedDump
		logger := b.logger
		cancel := b.cancel
		b.mu.Unlock()

		if cancel != nil {
			cancel()
		}

		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if s3Mount != nil {
			s3Mount.Unmount()
			s3Mount.CloseMount()
		}
		if stagedDump != nil {
			if err := stagedDump.Cleanup(); err != nil && logger != nil {
				logger.Error(err, "failed to remove database backup staging data")
			}
		}
		if logger != nil {
			logger.Close()
		}
	})
}

func (b *backupJob) processPBSLogs(logErr error, upid string) (bool, int) {
	b.mu.RLock()
	agentMount := b.agentMount
	logger := b.logger
	b.mu.RUnlock()
	gracefulEnd := agentMount == nil || agentMount.IsConnected()

	if err := logger.FlushJobLog(); err != nil {
		b.logger.Error(err, "failed to flush job log")
	}

	succeeded, cancelled, warningsNum, err := processPBSProxyLogs(gracefulEnd, upid, logger, logErr)
	if err != nil {
		b.logger.Error(err, "failed to process logs")
	}

	if succeeded || cancelled {
		b.logger.Info("backup succeeded or cancelled")
	} else {
		b.logger.Error(logErr, "backup failed, scheduler will retry")
	}

	return succeeded, warningsNum
}

func databaseLogLabel(target coredb.Target) string {
	if !target.IsDatabase() {
		return ""
	}
	if target.Type == coredb.TargetTypePostgreSQL {
		return "PostgreSQL"
	}
	if target.DatabaseVariant == "mariadb" {
		return "MariaDB"
	}
	return "MySQL"
}

func (b *backupJob) updatePBSStatus(succeeded bool, warningsNum int, upid string) {
	b.logger.Info("updating job status", "succeeded", succeeded, "warnings", warningsNum)
	b.mu.RLock()
	currentJob := b.job
	workflowStart := b.workflowStart
	b.mu.RUnlock()
	if err := updateBackupStatus(succeeded, warningsNum, currentJob, proxmox.Task{UPID: upid}, b.executionID, workflowStart, time.Now().Unix(), b.app); err != nil {
		b.logger.Error(err, "failed to update job status - post cmd.Wait")
	}
}

func (b *backupJob) createOK(err error) {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	task, terr := GenerateBackupTaskOKFile(
		job,
		[]string{
			"Done handling from a job run request",
			"Job ID: " + job.ID,
			"Source Mode: " + job.SourceMode,
			"Response: " + err.Error(),
		},
	)
	if terr != nil {
		return
	}

	workflowStart, workflowEnd := b.workflowTimes()
	b.mu.Lock()
	defer b.mu.Unlock()

	latest, gerr := b.app.CoreDB.GetBackup(b.job.ID)
	if gerr != nil {
		latest = b.job
	}

	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunStatus = coredb.JobStatusSuccess
	latest.History.LastRunStarttime = workflowStart
	latest.History.LastRunEndtime = workflowEnd
	latest.History.Duration = latest.History.LastRunEndtime - latest.History.LastRunStarttime
	latest.History.LastSuccessfulEndtime = latest.History.LastRunEndtime
	latest.History.LastSuccessfulUpid = task.UPID
	latest.History.RetryCount = 0

	b.job = latest
	if err := b.app.CoreDB.UpdateBackupHistory(latest.ID, latest.History, latest.CurrentPID); err != nil {
		b.logger.Error(err, "failed to persist backup OK state")
		return
	}
	jobs.MarkRunFinalized(latest.ID, b.executionID)
}

func (b *backupJob) updateBackupWithTask(task proxmox.Task) {
	workflowStart, workflowEnd := b.workflowTimes()
	b.mu.Lock()
	defer b.mu.Unlock()

	latest, gerr := b.app.CoreDB.GetBackup(b.job.ID)
	if gerr != nil {
		latest = b.job
	}

	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunStatus = coredb.JobStatusFailed
	latest.History.LastRunStarttime = workflowStart
	latest.History.LastRunEndtime = workflowEnd
	latest.History.Duration = latest.History.LastRunEndtime - latest.History.LastRunStarttime
	latest.History.RetryCount++

	b.job = latest
	if uerr := b.app.CoreDB.UpdateBackupHistory(latest.ID, latest.History, latest.CurrentPID); uerr != nil {
		b.logger.Error(uerr, "", "upid", task.UPID)
		return
	}
	jobs.MarkRunFinalized(latest.ID, b.executionID)
}

func (b *backupJob) workflowTimes() (int64, int64) {
	b.mu.RLock()
	start := b.workflowStart
	b.mu.RUnlock()
	end := time.Now().Unix()
	if start <= 0 {
		start = end
	}
	return start, end
}
