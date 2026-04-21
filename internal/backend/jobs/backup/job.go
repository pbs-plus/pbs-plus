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
	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/backend/mount"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// BackupJob holds the state for a backup operation.
type backupJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	Task      proxmox.Task
	currOwner string
	queueTask *tasks.QueuedTask
	waitGroup *sync.WaitGroup
	err       error

	logger *syslog.JobLogger

	job database.Backup

	storeInstance   *store.Store
	skipCheck       bool
	web             bool
	extraExclusions []string

	cleanupOnce sync.Once
	started     atomic.Bool

	agentMount *mount.AgentMount
	s3Mount    *mount.S3Mount
	srcPath    string
}

// NewBackupJob creates a new backup job with the given parameters.
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
		logger:          syslog.CreateJobLogger(job.ID),
		extraExclusions: extraExclusions,
		waitGroup:       &sync.WaitGroup{},
	}

	return &jobs.Job{
		ID:        job.ID,
		Execute:   j.execute,
		OnSuccess: j.onSuccess,
		OnError:   j.onError,
		Cleanup:   j.cleanup,
	}
}

// execute performs the backup operation.
func (b *backupJob) execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	// Pre-execution phase
	if err := b.preExecute(ctx); err != nil {
		return err
	}

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
			_ = SetDatastoreOwner(job, b.storeInstance, currOwner)
		}
	}

	b.started.Store(true)

	// Wait for completion
	return b.waitForCompletion(ctx, cmd)
}

func (b *backupJob) preExecute(ctx context.Context) error {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	queueTask, err := tasks.GenerateBackupQueuedTask(job, b.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		if err := updateBackupStatus(false, 0, job, queueTask.Task, b.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}

	b.mu.Lock()
	b.queueTask = &queueTask
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
		qt.UpdateDescription("operation ready, waiting for queue to free up")
	}

	return nil
}

func (b *backupJob) onError(err error) {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	syslog.L.Error(err).WithField("jobId", job.ID).Write()

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
		syslog.L.Info().WithJob(job.ID).WithMessage("checking post-backup script")
		b.runPostScript(succeeded, warningsNum)
		return
	}

	task, terr := tasks.GenerateBackupTaskErrorFile(
		b.job,
		err,
		[]string{
			"Error handling from a scheduled job run request",
			"Backup ID: " + job.ID,
			"Source Mode: " + job.SourceMode,
		},
	)
	if terr != nil {
		syslog.L.Error(terr).WithField("jobId", job.ID).Write()
	} else {
		b.updateBackupWithTask(task)
	}
}

func (b *backupJob) onSuccess() {
	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	currOwner := b.currOwner
	b.mu.RUnlock()

	for _, ext := range extraExclusions {
		syslog.L.Warn().
			WithJob(job.ID).
			WithMessage(fmt.Sprintf("skipped %s due to an error from previous retry attempts", ext)).
			Write()
	}

	b.waitGroup.Wait()

	succeeded, warningsNum := b.processPBSLogs(nil)

	if currOwner != "" {
		syslog.L.Info().WithJob(job.ID).WithMessage("setting owner to datastore owner")
		_ = SetDatastoreOwner(job, b.storeInstance, currOwner)
	}

	syslog.L.Info().WithJob(b.job.ID).WithMessage("checking post-backup script")
	b.runPostScript(succeeded, warningsNum)
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
			_ = logger.Close()
		}
		if qt != nil {
			qt.Close()
		}
	})
}

// Helper methods (copied and adapted from operation.go)

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
			_ = cmd.Process.Kill()
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

	_ = logger.Flush()

	b.mu.RLock()
	currentUPID := b.Task.UPID
	b.mu.RUnlock()

	succeeded, cancelled, warningsNum, err := processPBSProxyLogs(gracefulEnd, currentUPID, logger, logErr)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to process logs").Write()
	}

	b.mu.RLock()
	jobID := b.job.ID
	b.mu.RUnlock()

	syslog.L.Info().WithJob(jobID).WithMessage("updating job status")

	b.mu.RLock()
	currentUPID = b.Task.UPID
	startTime := logger.StartTime
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
		syslog.L.Error(err).WithMessage("failed to update job status - post cmd.Wait").Write()
	}

	if succeeded || cancelled {
		syslog.L.Info().WithJob(jobID).WithMessage("succeeded/cancelled")
	} else {
		syslog.L.Info().WithJob(jobID).WithMessage("failed, scheduler will retry")
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
		qt.UpdateDescription("running pre-backup script")
	}

	envVars, err := jobs.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, modEnvVars, err := jobs.RunShellScript(ctx, job.PreScript, envVars)
	syslog.L.Info().WithJob(job.ID).WithMessage(scriptOut).WithField("script", job.PreScript).Write()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			syslog.L.Info().WithJob(job.ID).WithMessage("pre-backup script canceled").Write()
			return jobs.ErrCanceled
		}
		syslog.L.Error(err).
			WithJob(job.ID).
			WithMessage("error encountered while running job pre-backup script").
			Write()
		return err
	}

	if newNs, ok := modEnvVars["PBS_PLUS__NAMESPACE"]; ok {
		b.mu.Lock()
		latestBackup, err := b.storeInstance.Database.GetBackup(b.job.ID)
		if err == nil {
			b.job = latestBackup
		}
		b.job.Namespace = newNs
		_ = b.storeInstance.Database.UpdateBackup(nil, b.job)
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
		sess, exists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(job.Target.GetHostname())
		if !exists {
			return fmt.Errorf("%w: %s", ErrTargetUnreachable, job.Target.Name)
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		respMsg, err := sess.CallMessage(
			timeoutCtx,
			"target_status",
			&types.TargetStatusReq{Drive: job.Target.VolumeID},
		)
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
		qt.UpdateDescription("running target mount script")
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
		syslog.L.Error(err).WithMessage("error encountered while running mount script").Write()
	}
	syslog.L.Info().WithMessage(scriptOut).WithField("script", target.MountScript).Write()

	return nil
}

func (b *backupJob) mountSource(ctx context.Context, target database.Target) (string, *mount.AgentMount, *mount.S3Mount, error) {
	select {
	case <-ctx.Done():
		return "", nil, nil, jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	qt := b.queueTask
	b.mu.RUnlock()
	if qt != nil {
		qt.UpdateDescription("mounting target to server")
	}

	var (
		srcPath    = target.Path
		agentMount *mount.AgentMount
		s3Mount    *mount.S3Mount
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
				qt.UpdateDescription("waiting for agent to finish snapshot")
			}
		}

		timedCtx, timedCtxCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer timedCtxCancel()

		agentMount, err = mount.AgentFSMount(timedCtx, b.storeInstance, job, target)
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

		s3Mount, err = mount.S3FSMount(timedCtx, b.storeInstance, job, target)
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
		qt.UpdateDescription("waiting for proxmox-backup-client to start")
	}

	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	b.mu.RUnlock()

	cmd, err := prepareBackupCommand(ctx, job, b.storeInstance, srcPath, target.IsAgent(), extraExclusions)
	if err != nil {
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrPrepareBackupCommand, err)
	}

	taskChan, readyChan, errChan := b.startTaskMonitoring(ctx, target)

	select {
	case <-readyChan:
	case err := <-errChan:
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskMonitoringInitializationFailed, err)
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, proxmox.Task{}, "", jobs.ErrCanceled
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskMonitoringTimedOut, ctx.Err())
	}

	currOwner, _ := GetCurrentOwner(job, b.storeInstance)
	_ = FixDatastore(job, b.storeInstance)

	b.mu.RLock()
	logger := b.logger
	b.mu.RUnlock()

	stdoutWriter := io.MultiWriter(logger, os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter

	syslog.L.Info().WithMessage("starting backup job").WithField("args", cmd.Args).Write()

	if err := cmd.Start(); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(job, b.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w (%s): %v", ErrProxmoxBackupClientStart, cmd.String(), err)
	}

	if cmd.Process != nil {
		b.mu.Lock()
		b.job.CurrentPID = cmd.Process.Pid
		b.mu.Unlock()
	}

	b.mu.RLock()
	loggerPath := logger.Path
	b.mu.RUnlock()

	go monitorPBSClientLogs(ctx, loggerPath, cmd)

	var task proxmox.Task
	select {
	case task = <-taskChan:
	case err := <-errChan:
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskDetectionFailed, err)
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, b.storeInstance, currOwner)
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
		defer syslog.L.Info().WithMessage("monitor goroutine closing").Write()

		timedCtx, timedCancel := context.WithTimeout(ctx, 20*time.Second)
		defer timedCancel()

		task, err := tasks.GetBackupTask(timedCtx, readyChan, job, target)
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
		qt.UpdateDescription("running post-backup script")
	}

	syslog.L.Info().
		WithMessage("running post-backup script").
		WithField("script", job.PostScript).
		WithJob(job.ID).
		Write()

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
		syslog.L.Error(err).
			WithMessage("error encountered while running job post-backup script").
			WithJob(job.ID).
			Write()
	}

	syslog.L.Info().
		WithMessage(scriptOut).
		WithField("script", job.PostScript).
		WithJob(job.ID).
		Write()
}

func (b *backupJob) createOK(err error) {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	task, terr := tasks.GenerateBackupTaskOKFile(
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
	_ = b.storeInstance.Database.UpdateBackup(nil, latest)
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
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}
