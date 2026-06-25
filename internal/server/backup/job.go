//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

type backupJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	Task      proxmox.Task
	currOwner string
	queueTask *tasklog.QueuedTask
	waitGroup *sync.WaitGroup
	err       error

	logger *log.Logger

	job database.Backup

	storeInstance   *store.Store
	skipCheck       bool
	web             bool
	extraExclusions []string

	cleanupOnce sync.Once
	started     atomic.Bool

	agentMount *rpc.AgentMount
	s3Mount    *rpc.S3Mount
	srcPath    string
}

func NewBackupJob(
	job database.Backup,
	storeInstance *store.Store,
	skipCheck bool,
	web bool,
	extraExclusions []string,
) *jobs.Job {
	j := &backupJob{
		job:             job,
		storeInstance:   storeInstance,
		skipCheck:       skipCheck,
		web:             web,
		logger:          log.WithScope(log.Scope{JobID: job.ID}),
		extraExclusions: extraExclusions,
		waitGroup:       &sync.WaitGroup{},
	}

	return &jobs.Job{
		ID:        job.ID,
		PreExec:   j.preExecute,
		Execute:   j.execute,
		OnSuccess: j.onSuccess,
		OnError:   j.onError,
		Cleanup:   j.cleanup,
	}
}

func (b *backupJob) execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	srcPath := b.srcPath
	target := b.job.Target
	b.mu.RUnlock()

	cmd, task, currOwner, err := b.startBackup(ctx, srcPath, target)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.Task = task
	b.currOwner = currOwner
	b.mu.Unlock()

	b.mu.RLock()
	job := b.job
	taskCopy := b.Task
	b.mu.RUnlock()

	if err := updateBackupStatus(false, 0, job, taskCopy, b.storeInstance); err != nil {
		if currOwner != "" {
			if err := SetDatastoreOwner(job, b.storeInstance, currOwner); err != nil {
				b.logger.Error(err, "")
			}
		}
	}

	b.started.Store(true)

	return b.waitForCompletion(ctx, cmd)
}

func (b *backupJob) preExecute(ctx context.Context) error {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	wid := tasklog.FormatWorkerID(job.Store, "host-", job.Target.GetHostname())
	queueTask, err := tasklog.WriteQueuedLog("pbsplusgen-queue", "backup", wid, b.web)
	if err != nil {
		b.logger.Error(err, "failed to create queue task, not fatal")
	} else {
		if err := updateBackupStatus(false, 0, job, queueTask.Task, b.storeInstance); err != nil {
			b.logger.Error(err, "failed to set queue task, not fatal")
		}
	}

	b.mu.Lock()
	b.queueTask = queueTask
	b.mu.Unlock()

	if err := b.runPreScript(ctx); err != nil {
		return err
	}

	if err := b.validateTargetConnection(ctx); err != nil {
		return err
	}

	b.mu.RLock()
	job = b.job
	b.mu.RUnlock()
	if err := b.runTargetMountScript(ctx, job.Target); err != nil {
		return err
	}

	b.mu.RLock()
	job = b.job
	b.mu.RUnlock()
	srcPath, agentMount, s3Mount, err := b.mountSource(ctx, job.Target)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.srcPath = srcPath
	b.agentMount = agentMount
	b.s3Mount = s3Mount
	b.mu.Unlock()

	b.mu.RLock()
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		if err := qt.UpdateDescription("operation ready, waiting for queue to free up"); err != nil {
			b.logger.Error(err, "")
		}
	}

	return nil
}

func (b *backupJob) onError(err error) {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()
	b.logger.Error(err, "")

	if errors.Is(err, jobs.ErrOneInstance) {
		return
	}

	if errors.Is(err, ErrMountEmpty) {
		b.createOK(err)
		return
	}

	if b.started.Load() {
		b.waitGroup.Wait()
		succeeded, warningsNum := b.processPBSLogs(err)
		b.logger.Info("checking post-backup script")
		b.runPostScript(succeeded, warningsNum)

		var notifyErr error
		if !succeeded {
			notifyErr = fmt.Errorf("backup failed: %v", err)
		}
		if b.storeInstance.BatchTracker != nil {
			b.storeInstance.BatchTracker.RecordJobResult(
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

	task, terr := GenerateBackupTaskErrorFile(
		b.job,
		err,
		[]string{
			"Error handling from a scheduled job run request",
			"Backup ID: " + job.ID,
			"Source Mode: " + job.SourceMode,
		},
	)
	if terr != nil {
		b.logger.Error(terr, "")
	} else {
		b.updateBackupWithTask(task)
	}

	if b.storeInstance.BatchTracker != nil {
		b.storeInstance.BatchTracker.RecordJobResult(
			job.NotificationMode,
			notification.JobTypeBackup,
			job.ID,
			job.Store,
			fmt.Errorf("backup failed to start: %v", err),
			map[string]string{
				"target":    job.Target.Name,
				"succeeded": "false",
				"phase":     "pre-start",
			},
		)
	}
}

func (b *backupJob) onSuccess() {
	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	currOwner := b.currOwner
	b.mu.RUnlock()

	for _, ext := range extraExclusions {
		b.logger.Warn(fmt.Sprintf("skipped %s due to an error from previous retry attempts", ext))

	}

	b.waitGroup.Wait()

	succeeded, warningsNum := b.processPBSLogs(nil)

	if currOwner != "" {
		b.logger.Info("setting owner to datastore owner")
		if err := SetDatastoreOwner(job, b.storeInstance, currOwner); err != nil {
			b.logger.Error(err, "")
		}
	}
	b.logger.Info("checking post-backup script")
	b.runPostScript(succeeded, warningsNum)

	var notifyErr error
	if !succeeded {
		notifyErr = fmt.Errorf("backup failed")
	}
	if b.storeInstance.BatchTracker != nil {
		b.storeInstance.BatchTracker.RecordJobResult(
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

	if succeeded && b.storeInstance.OnBackupComplete != nil {
		go b.storeInstance.OnBackupComplete(b.job.ID)
	}
}

func (b *backupJob) cleanup() {
	b.cleanupOnce.Do(func() {
		b.waitGroup.Wait()

		b.mu.Lock()
		b.job.CurrentPID = 0
		agentMount := b.agentMount
		s3Mount := b.s3Mount
		logger := b.logger
		qt := b.queueTask
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
		if logger != nil {
			logger.Close()
		}
		if qt != nil {
			qt.Close()
		}
	})
}

func (b *backupJob) waitForCompletion(ctx context.Context, cmd *exec.Cmd) error {
	done := make(chan error, 1)

	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			b.mu.Lock()
			b.err = err
			b.mu.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				b.logger.Error(err, "")
			}
		}
		<-done
		b.mu.Lock()
		b.err = jobs.ErrCanceled
		b.mu.Unlock()
		return jobs.ErrCanceled
	}
}

func (b *backupJob) processPBSLogs(logErr error) (bool, int) {
	b.mu.RLock()
	agentMount := b.agentMount
	b.mu.RUnlock()
	gracefulEnd := true
	if agentMount != nil && !agentMount.IsConnected() {
		gracefulEnd = false
	}

	b.mu.RLock()
	logger := b.logger
	b.mu.RUnlock()

	if err := logger.FlushJobLog(); err != nil {
		b.logger.Error(err, "")
	}

	b.mu.RLock()
	currentUPID := b.Task.UPID
	b.mu.RUnlock()

	succeeded, cancelled, warningsNum, err := processPBSProxyLogs(gracefulEnd, currentUPID, logger, logErr)
	if err != nil {
		b.logger.Error(err, "failed to process logs")
	}

	b.logger.Info("updating job status")

	b.mu.RLock()
	currentUPID = b.Task.UPID
	startTime := logger.JobStartTime()
	b.mu.RUnlock()

	if newUpid, err := proxmox.ChangeUPIDStartTime(currentUPID, startTime); err == nil {
		b.mu.Lock()
		b.Task.UPID = newUpid
		b.mu.Unlock()
	}

	b.mu.RLock()
	currentJob := b.job
	taskCopy := b.Task
	b.mu.RUnlock()

	if err := updateBackupStatus(succeeded, warningsNum, currentJob, taskCopy, b.storeInstance); err != nil {
		b.logger.Error(err, "failed to update job status - post cmd.Wait")
	}

	if succeeded || cancelled {
		b.logger.Info("succeeded/cancelled")
	} else {
		b.logger.Info("failed, scheduler will retry")
	}

	return succeeded, warningsNum
}

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
			b.logger.Error(err, "")
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
		latestBackup, err := b.storeInstance.Database.GetBackup(b.job.ID)
		if err == nil {
			b.job = latestBackup
		}
		b.job.Namespace = newNs
		if err := b.storeInstance.Database.UpdateBackup(nil, b.job); err != nil {
			b.logger.Error(err, "")
		}
		b.mu.Unlock()
	}

	return nil
}

func (b *backupJob) validateTargetConnection(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	if b.skipCheck {
		return nil
	}

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	switch job.Target.Type {
	case database.TargetTypeAgent:
		qSess, qExists := b.storeInstance.ARPCAgentsManager.GetQuicPipe(job.Target.GetHostname())
		tSess, tExists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(job.Target.GetHostname())
		if !qExists && !tExists {
			return fmt.Errorf("%w: %s", ErrTargetUnreachable, job.Target.Name)
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var respMsg string
		var err error
		if qExists {
			respMsg, err = qSess.CallMessage(
				timeoutCtx,
				"target_status",
				&types.TargetStatusReq{Drive: job.Target.VolumeID},
			)
		} else {
			respMsg, err = tSess.CallMessage(
				timeoutCtx,
				"target_status",
				&types.TargetStatusReq{Drive: job.Target.VolumeID},
			)
		}
		if err != nil || !isReachable(respMsg) {
			return fmt.Errorf("%w: %s", ErrTargetUnreachable, job.Target.Name)
		}

	case database.TargetTypeLocal:
		if _, err := os.Stat(job.Target.Path); err != nil {
			return fmt.Errorf("%w: %s (%v)", ErrTargetUnreachable, job.Target.Name, err)
		}

	case database.TargetTypeS3:
	}

	return nil
}

func isReachable(msg string) bool {
	return len(msg) >= 9 && msg[:9] == "reachable"
}

func (b *backupJob) runTargetMountScript(ctx context.Context, target database.Target) error {
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
			b.logger.Error(err, "")
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

func (b *backupJob) mountSource(ctx context.Context, target database.Target) (string, *rpc.AgentMount, *rpc.S3Mount, error) {
	select {
	case <-ctx.Done():
		return "", nil, nil, jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		if err := qt.UpdateDescription("mounting target to server"); err != nil {
			b.logger.Error(err, "")
		}
	}

	var (
		srcPath    = target.Path
		agentMount *rpc.AgentMount
		s3Mount    *rpc.S3Mount
		err        error
	)

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	if target.IsAgent() {
		if job.SourceMode == "snapshot" {
			b.mu.RLock()
			qt := b.queueTask
			b.mu.RUnlock()
			if qt != nil {
				if err := qt.UpdateDescription("waiting for agent to finish snapshot"); err != nil {
					b.logger.Error(err, "")
				}
			}
		}

		timedCtx, timedCtxCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer timedCtxCancel()

		agentMount, err = rpc.AgentFSMount(timedCtx, b.storeInstance, job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = agentMount.Path

		select {
		case <-ctx.Done():
			agentMount.Unmount()
			agentMount.CloseMount()
			return "", nil, nil, jobs.ErrCanceled
		default:
		}

		b.mu.Lock()
		if latestBackup, err := b.storeInstance.Database.GetBackup(b.job.ID); err == nil {
			b.job = latestBackup
		}
		job = b.job
		b.mu.Unlock()

		if agentMount.IsEmpty() {
			return "", agentMount, nil, ErrMountEmpty
		}
	} else if target.IsS3() {
		timedCtx, timedCtxCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer timedCtxCancel()

		s3Mount, err = rpc.S3FSMount(timedCtx, b.storeInstance, job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = s3Mount.Path

		select {
		case <-ctx.Done():
			s3Mount.Unmount()
			s3Mount.CloseMount()
			return "", nil, nil, jobs.ErrCanceled
		default:
		}

		b.mu.Lock()
		if latestBackup, err := b.storeInstance.Database.GetBackup(b.job.ID); err == nil {
			b.job = latestBackup
		}
		job = b.job
		b.mu.Unlock()

		if s3Mount.IsEmpty() {
			return "", nil, s3Mount, ErrMountEmpty
		}
	}

	srcPath = filepath.Join(srcPath, job.Subpath)

	if job.Subpath != "" && !target.IsS3() {
		info, err := os.Stat(srcPath)
		if err != nil {
			if os.IsNotExist(err) {
				return "", agentMount, s3Mount, fmt.Errorf("%w: %q does not exist under the mount point", ErrSubpathNotFound, job.Subpath)
			}
			return "", agentMount, s3Mount, fmt.Errorf("%w: cannot access subpath %q: %v", ErrSubpathNotFound, job.Subpath, err)
		}
		if !info.IsDir() {
			return "", agentMount, s3Mount, fmt.Errorf("%w: %q is not a directory", ErrSubpathNotFound, job.Subpath)
		}
	}

	return srcPath, agentMount, s3Mount, nil
}

func (b *backupJob) startBackup(ctx context.Context, srcPath string, target database.Target) (*exec.Cmd, proxmox.Task, string, error) {
	select {
	case <-ctx.Done():
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		if err := qt.UpdateDescription("waiting for proxmox-backup-client to start"); err != nil {
			b.logger.Error(err, "")
		}
	}

	startupMu := b.storeInstance.Manager.StartupMu()
	startupMu.Lock()

	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	b.mu.RUnlock()

	cmd, err := prepareBackupCommand(ctx, job, b.storeInstance, srcPath, target.IsAgent(), extraExclusions, b.logger)
	if err != nil {
		startupMu.Unlock()
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrPrepareBackupCommand, err)
	}

	taskChan, readyChan, errChan := b.startTaskMonitoring(ctx, target)

	select {
	case <-readyChan:
	case err := <-errChan:
		startupMu.Unlock()
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskMonitoringInitializationFailed, err)
	case <-ctx.Done():
		startupMu.Unlock()
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, proxmox.Task{}, "", jobs.ErrCanceled
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskMonitoringTimedOut, ctx.Err())
	}

	currOwner, err := GetCurrentOwner(job, b.storeInstance)
	if err != nil {
		b.logger.Error(err, "")
	}
	if err := FixDatastore(job, b.storeInstance); err != nil {
		b.logger.Error(err, "")
	}

	b.mu.RLock()
	logger := b.logger
	b.mu.RUnlock()

	stdoutWriter := io.MultiWriter(logger.JobStdoutWriter(), os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter
	b.logger.Info("starting backup job", "args", cmd.Args)

	if err := cmd.Start(); err != nil {
		startupMu.Unlock()
		if currOwner != "" {
			if err := SetDatastoreOwner(job, b.storeInstance, currOwner); err != nil {
				b.logger.Error(err, "")
			}
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w (%s): %v", ErrProxmoxBackupClientStart, cmd.String(), err)
	}

	if cmd.Process != nil {
		b.mu.Lock()
		b.job.CurrentPID = cmd.Process.Pid
		b.mu.Unlock()
	}

	b.mu.RLock()
	loggerPath := logger.JobLogPath()
	b.mu.RUnlock()

	go monitorPBSClientLogs(ctx, loggerPath, cmd, b.logger)

	var task proxmox.Task
	select {
	case task = <-taskChan:
		startupMu.Unlock()
	case err := <-errChan:
		startupMu.Unlock()
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskDetectionFailed, err)
	case <-ctx.Done():
		startupMu.Unlock()
		if err := cmd.Process.Kill(); err != nil {
			b.logger.Error(err, "")
		}
		if currOwner != "" {
			if err := SetDatastoreOwner(job, b.storeInstance, currOwner); err != nil {
				b.logger.Error(err, "")
			}
		}
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	}

	return cmd, task, currOwner, nil
}

func (b *backupJob) startTaskMonitoring(ctx context.Context, target database.Target) (<-chan proxmox.Task, <-chan struct{}, <-chan error) {
	readyChan := make(chan struct{})
	taskChan := make(chan proxmox.Task, 1)
	errChan := make(chan error, 1)

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	go func() {
		defer b.logger.Info("monitor goroutine closing")

		timedCtx, timedCancel := context.WithTimeout(ctx, 20*time.Second)
		defer timedCancel()

		task, err := GetBackupTask(timedCtx, readyChan, job, target)
		if err != nil {
			errChan <- err
			return
		}
		taskChan <- task
	}()

	return taskChan, readyChan, errChan
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
			b.logger.Error(err, "")
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

	b.mu.Lock()
	defer b.mu.Unlock()

	latest, gerr := b.storeInstance.Database.GetBackup(b.job.ID)
	if gerr != nil {
		latest = b.job
	}

	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunEndtime = task.EndTime
	latest.History.LastSuccessfulEndtime = task.EndTime
	latest.History.LastSuccessfulUpid = task.UPID

	b.job = latest
	if err := b.storeInstance.Database.UpdateBackup(nil, latest); err != nil {
		b.logger.Error(err, "")
	}
}

func (b *backupJob) updateBackupWithTask(task proxmox.Task) {
	b.mu.Lock()
	defer b.mu.Unlock()

	latest, gerr := b.storeInstance.Database.GetBackup(b.job.ID)
	if gerr != nil {
		latest = b.job
	}

	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunEndtime = task.EndTime

	b.job = latest
	if uerr := b.storeInstance.Database.UpdateBackup(nil, latest); uerr != nil {
		b.logger.Error(uerr, "", "upid", task.UPID)

	}
}
