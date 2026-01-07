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
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/backend/mount"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var (
	ErrJobMutexCreation = errors.New("failed to create job mutex")
	ErrOneInstance      = errors.New("a job is still running; only one instance allowed")

	ErrStdoutTempCreation = errors.New("failed to create stdout temp file")

	ErrBackupMutexCreation = errors.New("failed to create backup mutex")
	ErrBackupMutexLock     = errors.New("failed to lock backup mutex")

	ErrAPITokenRequired = errors.New("API token is required")

	ErrTargetGet         = errors.New("failed to get target")
	ErrTargetNotFound    = errors.New("target does not exist")
	ErrTargetUnreachable = errors.New("target unreachable")

	ErrPrepareBackupCommand = errors.New("failed to prepare backup command")

	ErrTaskMonitoringInitializationFailed = errors.New("task monitoring initialization failed")
	ErrTaskMonitoringTimedOut             = errors.New("task monitoring initialization timed out")

	ErrProxmoxBackupClientStart = errors.New("proxmox-backup-client start error")

	ErrNilTask               = errors.New("received nil task")
	ErrTaskDetectionFailed   = errors.New("task detection failed")
	ErrTaskDetectionTimedOut = errors.New("task detection timed out")
	ErrMountEmpty            = errors.New("target directory is empty, skipping backup")

	ErrJobStatusUpdateFailed = errors.New("failed to update job status")
	ErrCanceled              = errors.New("operation canceled")
)

type BackupOperation struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex

	Task      proxmox.Task
	queueTask *proxmox.QueuedTask
	waitGroup *sync.WaitGroup
	err       error

	logger *syslog.BackupLogger

	job             types.Job
	storeInstance   *store.Store
	skipCheck       bool
	web             bool
	extraExclusions []string

	cleanupOnce sync.Once
}

var _ jobs.Operation = (*BackupOperation)(nil)

func NewBackupOperation(
	job types.Job,
	storeInstance *store.Store,
	skipCheck bool,
	web bool,
	extraExclusions []string,
) *BackupOperation {
	return &BackupOperation{
		job:             job,
		storeInstance:   storeInstance,
		skipCheck:       skipCheck,
		web:             web,
		logger:          syslog.CreateBackupLogger(job.ID),
		extraExclusions: extraExclusions,
		waitGroup:       &sync.WaitGroup{},
	}
}

func (b *BackupOperation) GetID() string {
	return b.job.ID
}

func (b *BackupOperation) SetContext(ctx context.Context, cancel context.CancelFunc) {
	b.ctx = ctx
	b.cancel = cancel
}

func (b *BackupOperation) Context() context.Context {
	return b.ctx
}

func (b *BackupOperation) PreExecute() error {
	queueTask, err := proxmox.GenerateQueuedTask(b.job, b.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		if err := updateJobStatus(false, 0, b.job, queueTask.Task, b.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}
	b.queueTask = &queueTask

	return b.runPreScript()
}

func (b *BackupOperation) Execute() error {
	errorMonitorDone := make(chan struct{})
	var agentMount *mount.AgentMount
	var s3Mount *mount.S3Mount

	cleanup := func() {
		b.cleanupOnce.Do(func() {
			b.mu.Lock()
			utils.ClearIOStats(b.job.CurrentPID)
			b.job.CurrentPID = 0
			b.mu.Unlock()

			if agentMount != nil {
				agentMount.Unmount()
				agentMount.CloseMount()
			}
			if s3Mount != nil {
				s3Mount.Unmount()
			}
			if b.logger != nil {
				_ = b.logger.Close()
			}
			close(errorMonitorDone)
		})
	}

	select {
	case <-b.ctx.Done():
		cleanup()
		return jobs.ErrCanceled
	default:
	}

	target, err := b.getAndValidateTarget()
	if err != nil {
		cleanup()
		return err
	}

	if err := b.runTargetMountScript(target); err != nil {
		cleanup()
		return err
	}

	srcPath, agentMount, s3Mount, err := b.mountSource(target)
	if err != nil {
		cleanup()
		return err
	}

	cmd, task, currOwner, err := b.startBackup(srcPath, target, errorMonitorDone)
	if err != nil {
		cleanup()
		return err
	}

	b.mu.Lock()
	b.Task = task
	b.mu.Unlock()

	b.waitGroup.Go(func() {
		b.waitForCompletion(cmd, task, currOwner, agentMount, s3Mount, cleanup)
	})

	return nil
}

func (b *BackupOperation) OnError(err error) {
	syslog.L.Error(err).WithField("jobId", b.job.ID).Write()

	if errors.Is(err, jobs.ErrOneInstance) {
		return
	}

	if errors.Is(err, ErrMountEmpty) {
		b.createOK(err)
		return
	}

	task, terr := proxmox.GenerateTaskErrorFile(
		b.job,
		err,
		[]string{
			"Error handling from a scheduled job run request",
			"Job ID: " + b.job.ID,
			"Source Mode: " + b.job.SourceMode,
		},
	)
	if terr != nil {
		syslog.L.Error(terr).WithField("jobId", b.job.ID).Write()
	} else {
		b.updateJobWithTask(task)
	}

	if rerr := system.SetRetrySchedule(b.ctx, b.job, b.extraExclusions); rerr != nil {
		syslog.L.Error(rerr).WithField("jobId", b.job.ID).Write()
	}
}

func (b *BackupOperation) OnSuccess() {
}

func (b *BackupOperation) Cleanup() {
	if b.queueTask != nil {
		b.queueTask.Close()
	}
}

func (b *BackupOperation) Wait() error {
	b.waitGroup.Wait()
	return b.err
}

func (b *BackupOperation) runPreScript() error {
	if strings.TrimSpace(b.job.PreScript) == "" {
		return nil
	}

	select {
	case <-b.Context().Done():
		return jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("running pre-backup script")

	b.mu.RLock()
	envVars, err := utils.StructToEnvVars(b.job)
	b.mu.RUnlock()

	if err != nil {
		envVars = []string{}
	}

	scriptOut, modEnvVars, err := utils.RunShellScript(b.Context(), b.job.PreScript, envVars)
	syslog.L.Info().WithJob(b.job.ID).WithMessage(scriptOut).WithField("script", b.job.PreScript).Write()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			syslog.L.Info().WithJob(b.job.ID).WithMessage("pre-backup script canceled").Write()
			return jobs.ErrCanceled
		}
		syslog.L.Error(err).WithJob(b.job.ID).WithMessage("error encountered while running job pre-backup script").Write()
		return err
	}

	if newNs, ok := modEnvVars["PBS_PLUS__NAMESPACE"]; ok {
		b.mu.Lock()
		latestJob, err := b.storeInstance.Database.GetJob(b.job.ID)
		if err == nil {
			b.job = latestJob
		}
		b.job.Namespace = newNs
		_ = b.storeInstance.Database.UpdateJob(nil, b.job)
		b.mu.Unlock()
	}

	return nil
}

func (b *BackupOperation) getAndValidateTarget() (types.Target, error) {
	select {
	case <-b.Context().Done():
		return types.Target{}, jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	targetID := b.job.Target
	b.mu.RUnlock()

	target, err := b.storeInstance.Database.GetTarget(targetID)
	if err != nil {
		if os.IsNotExist(err) {
			return target, fmt.Errorf("%w: %s", ErrTargetNotFound, targetID)
		}
		return target, fmt.Errorf("%w: %v", ErrTargetGet, err)
	}

	if b.skipCheck {
		return target, nil
	}

	if target.IsAgent {
		targetSplit := strings.Split(target.Name, " - ")
		_, exists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(targetSplit[0])
		if !exists {
			return target, fmt.Errorf("%w: %s", ErrTargetUnreachable, targetID)
		}
	} else if !target.IsS3 {
		if _, err := os.Stat(target.Path); err != nil {
			return target, fmt.Errorf("%w: %s (%v)", ErrTargetUnreachable, targetID, err)
		}
	}

	return target, nil
}

func (b *BackupOperation) runTargetMountScript(target types.Target) error {
	if target.MountScript == "" {
		return nil
	}

	select {
	case <-b.Context().Done():
		return jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("running target mount script")

	envVars, err := utils.StructToEnvVars(target)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, _, err := utils.RunShellScript(b.Context(), target.MountScript, envVars)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return jobs.ErrCanceled
		}
		syslog.L.Error(err).WithMessage("error encountered while running mount script").Write()
	}
	syslog.L.Info().WithMessage(scriptOut).WithField("script", target.MountScript).Write()
	return nil
}

func (b *BackupOperation) mountSource(target types.Target) (string, *mount.AgentMount, *mount.S3Mount, error) {
	select {
	case <-b.Context().Done():
		return "", nil, nil, jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("mounting target to server")

	srcPath := target.Path
	var agentMount *mount.AgentMount
	var s3Mount *mount.S3Mount
	var err error

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	if target.IsAgent {
		if job.SourceMode == "snapshot" {
			b.queueTask.UpdateDescription("waiting for agent to finish snapshot")
		}

		timedCtx, timedCtxCancel := context.WithTimeout(b.Context(), 5*time.Minute)
		defer timedCtxCancel()

		agentMount, err = mount.AgentFSMount(timedCtx, b.storeInstance, job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = agentMount.Path

		select {
		case <-b.Context().Done():
			agentMount.Unmount()
			agentMount.CloseMount()
			return "", nil, nil, jobs.ErrCanceled
		default:
		}

		b.mu.Lock()
		if latestJob, err := b.storeInstance.Database.GetJob(b.job.ID); err == nil {
			b.job = latestJob
		}
		job = b.job
		b.mu.Unlock()

		if agentMount.IsEmpty() {
			return "", agentMount, nil, ErrMountEmpty
		}
	} else if target.IsS3 {
		timedCtx, timedCtxCancel := context.WithTimeout(b.Context(), 5*time.Minute)
		defer timedCtxCancel()

		s3Mount, err = mount.S3FSMount(timedCtx, b.storeInstance, job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = s3Mount.Path

		select {
		case <-b.Context().Done():
			s3Mount.Unmount()
			return "", nil, nil, jobs.ErrCanceled
		default:
		}

		b.mu.Lock()
		if latestJob, err := b.storeInstance.Database.GetJob(b.job.ID); err == nil {
			b.job = latestJob
		}
		job = b.job
		b.mu.Unlock()

		if s3Mount.IsEmpty() {
			return "", nil, s3Mount, ErrMountEmpty
		}
	}

	if !target.IsS3 {
		srcPath = filepath.Join(srcPath, job.Subpath)
	}

	return srcPath, agentMount, s3Mount, nil
}

func (b *BackupOperation) startBackup(srcPath string, target types.Target, errorMonitorDone chan struct{}) (*exec.Cmd, proxmox.Task, string, error) {
	select {
	case <-b.Context().Done():
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("waiting for proxmox-backup-client to start")

	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	b.mu.RUnlock()

	cmd, err := prepareBackupCommand(b.Context(), job, b.storeInstance, srcPath, target.IsAgent, extraExclusions)
	if err != nil {
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrPrepareBackupCommand, err)
	}

	taskChan, readyChan, errChan := b.startTaskMonitoring(target)

	select {
	case <-readyChan:
	case err := <-errChan:
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskMonitoringInitializationFailed, err)
	case <-b.Context().Done():
		if errors.Is(b.Context().Err(), context.Canceled) {
			return nil, proxmox.Task{}, "", jobs.ErrCanceled
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskMonitoringTimedOut, b.Context().Err())
	}

	currOwner, _ := GetCurrentOwner(job, b.storeInstance)
	_ = FixDatastore(job, b.storeInstance)

	stdoutWriter := io.MultiWriter(b.logger, os.Stdout)
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

	b.waitGroup.Go(func() {
		monitorPBSClientLogs(b.logger.Path, cmd, errorMonitorDone)
	})

	var task proxmox.Task
	select {
	case task = <-taskChan:
	case err := <-errChan:
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskDetectionFailed, err)
	case <-b.Context().Done():
		_ = cmd.Process.Kill()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, b.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	}

	return cmd, task, currOwner, nil
}

func (b *BackupOperation) startTaskMonitoring(target types.Target) (chan proxmox.Task, chan struct{}, chan error) {
	readyChan := make(chan struct{})
	taskChan := make(chan proxmox.Task, 1)
	errChan := make(chan error, 1)

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	b.waitGroup.Go(func() {
		timedCtx, timedCancel := context.WithTimeout(b.Context(), 20*time.Second)
		defer timedCancel()

		task, err := proxmox.GetJobTask(timedCtx, readyChan, job, target)
		if err != nil {
			errChan <- err
			return
		}
		taskChan <- task
	})

	return taskChan, readyChan, errChan
}

func (b *BackupOperation) waitForCompletion(cmd *exec.Cmd, task proxmox.Task, currOwner string, agentMount *mount.AgentMount, s3Mount *mount.S3Mount, cleanup func()) {
	defer cleanup()

	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	b.mu.RUnlock()

	if err := updateJobStatus(false, 0, job, task, b.storeInstance); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(job, b.storeInstance, currOwner)
		}
	}

	done := make(chan error, 1)

	b.waitGroup.Go(func() {
		done <- cmd.Wait()
	})

	select {
	case err := <-done:
		if err != nil {
			b.err = err
		}
	case <-b.Context().Done():
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
		b.err = jobs.ErrCanceled
	}

	for _, ext := range extraExclusions {
		syslog.L.Warn().WithJob(job.ID).WithMessage(fmt.Sprintf("skipped %s due to an error from previous retry attempts", ext)).Write()
	}

	gracefulEnd := true
	if agentMount != nil && !agentMount.IsConnected() {
		gracefulEnd = false
	}

	_ = b.logger.Flush()
	succeeded, cancelled, warningsNum, errorPath, err := processPBSProxyLogs(gracefulEnd, task.UPID, b.logger)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to process logs").Write()
	}

	if errorPath != "" {
		b.mu.Lock()
		b.extraExclusions = append(b.extraExclusions, errorPath)
		b.mu.Unlock()
	}

	if newUpid, err := proxmox.ChangeUPIDStartTime(task.UPID, b.logger.StartTime); err == nil {
		task.UPID = newUpid
	}

	b.mu.RLock()
	currentJob := b.job
	b.mu.RUnlock()

	if err := updateJobStatus(succeeded, warningsNum, currentJob, task, b.storeInstance); err != nil {
		syslog.L.Error(err).WithMessage("failed to update job status - post cmd.Wait").Write()
	}

	if succeeded || cancelled {
		system.RemoveAllRetrySchedules(b.Context(), currentJob)
	} else {
		b.mu.RLock()
		excl := b.extraExclusions
		b.mu.RUnlock()
		_ = system.SetRetrySchedule(b.Context(), currentJob, excl)
	}

	if currOwner != "" {
		_ = SetDatastoreOwner(currentJob, b.storeInstance, currOwner)
	}

	b.runPostScript(succeeded, warningsNum)
}

func (b *BackupOperation) runPostScript(success bool, warningsNum int) {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	if job.PostScript == "" {
		return
	}

	if b.queueTask != nil {
		b.queueTask.UpdateDescription("running post-backup script")
	}

	syslog.L.Info().
		WithMessage("running post-backup script").
		WithField("script", job.PostScript).
		WithJob(job.ID).
		Write()

	envVars, err := utils.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_SUCCESS=%t", success))
	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_WARNINGS=%d", warningsNum))

	scriptOut, _, err := utils.RunShellScript(b.Context(), job.PostScript, envVars)
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

func (b *BackupOperation) createOK(err error) {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	task, terr := proxmox.GenerateTaskOKFile(
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
	latest, gerr := b.storeInstance.Database.GetJob(b.job.ID)
	if gerr != nil {
		latest = b.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime
	latest.LastSuccessfulEndtime = task.EndTime
	latest.LastSuccessfulUpid = task.UPID

	b.job = latest
	_ = b.storeInstance.Database.UpdateJob(nil, latest)
}

func (b *BackupOperation) updateJobWithTask(task proxmox.Task) {
	b.mu.Lock()
	defer b.mu.Unlock()
	latest, gerr := b.storeInstance.Database.GetJob(b.job.ID)
	if gerr != nil {
		latest = b.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime

	b.job = latest
	_ = b.storeInstance.Database.UpdateJob(nil, latest)
}
