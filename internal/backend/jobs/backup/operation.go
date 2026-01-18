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
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/backend/mount"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var (
	ErrOneInstance = errors.New("a job is still running; only one instance allowed")

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

	ErrBackupStatusUpdateFailed = errors.New("failed to update job status")
	ErrCanceled                 = errors.New("operation canceled")
)

type BackupOperation struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex

	Task      proxmox.Task
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

	agentMount       *mount.AgentMount
	s3Mount          *mount.S3Mount
	srcPath          string
	errorMonitorDone chan struct{}
}

var _ jobs.Operation = (*BackupOperation)(nil)

func NewBackupOperation(
	job database.Backup,
	storeInstance *store.Store,
	skipCheck bool,
	web bool,
	extraExclusions []string,
) *BackupOperation {
	return &BackupOperation{
		job:              job,
		storeInstance:    storeInstance,
		skipCheck:        skipCheck,
		web:              web,
		logger:           syslog.CreateJobLogger(job.ID),
		extraExclusions:  extraExclusions,
		waitGroup:        &sync.WaitGroup{},
		errorMonitorDone: make(chan struct{}, 1),
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
	queueTask, err := tasks.GenerateBackupQueuedTask(b.job, b.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		if err := updateBackupStatus(false, 0, b.job, queueTask.Task, b.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}
	b.queueTask = &queueTask

	if err := b.runPreScript(); err != nil {
		return err
	}

	err = b.validateTargetConnection()
	if err != nil {
		return err
	}

	if err := b.runTargetMountScript(b.job.Target); err != nil {
		return err
	}

	b.srcPath, b.agentMount, b.s3Mount, err = b.mountSource(b.job.Target)
	if err != nil {
		return err
	}

	b.queueTask.UpdateDescription("operation ready, waiting for queue to free up")

	return nil
}

func (b *BackupOperation) Execute() error {
	select {
	case <-b.ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	cmd, task, currOwner, err := b.startBackup(b.srcPath, b.job.Target)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.Task = task
	b.mu.Unlock()

	if err := updateBackupStatus(false, 0, b.job, task, b.storeInstance); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(b.job, b.storeInstance, currOwner)
		}
	}

	b.started.Store(true)

	b.waitGroup.Go(func() {
		b.waitForCompletion(cmd, currOwner)
	})

	return nil
}

func (b *BackupOperation) processPBSLogs(logErr error) (bool, int) {
	gracefulEnd := true
	if b.agentMount != nil && !b.agentMount.IsConnected() {
		gracefulEnd = false
	}

	_ = b.logger.Flush()
	succeeded, cancelled, warningsNum, errorPath, err := processPBSProxyLogs(gracefulEnd, b.Task.UPID, b.logger, logErr)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to process logs").Write()
	}

	if errorPath != "" {
		b.mu.Lock()
		b.extraExclusions = append(b.extraExclusions, errorPath)
		b.mu.Unlock()
	}

	syslog.L.Info().WithJob(b.job.ID).WithMessage("updating job status")
	if newUpid, err := proxmox.ChangeUPIDStartTime(b.Task.UPID, b.logger.StartTime); err == nil {
		b.mu.Lock()
		b.Task.UPID = newUpid
		b.mu.Unlock()
	}

	b.mu.RLock()
	currentJob := b.job
	b.mu.RUnlock()

	if err := updateBackupStatus(succeeded, warningsNum, currentJob, b.Task, b.storeInstance); err != nil {
		syslog.L.Error(err).WithMessage("failed to update job status - post cmd.Wait").Write()
	}

	if succeeded || cancelled {
		syslog.L.Info().WithJob(b.job.ID).WithMessage("succeeded/cancelled, removing all retry schedules")
		currentJob.RemoveAllRetrySchedules(b.Context())
	} else {
		syslog.L.Info().WithJob(b.job.ID).WithMessage("failed, setting a retry schedule")
		b.mu.RLock()
		excl := b.extraExclusions
		b.mu.RUnlock()
		currentJob.SetBackupRetrySchedule(b.Context(), excl)
	}

	return succeeded, warningsNum
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

	if b.started.Load() {
		_, _ = b.processPBSLogs(err)
		return
	}

	task, terr := tasks.GenerateBackupTaskErrorFile(
		b.job,
		err,
		[]string{
			"Error handling from a scheduled job run request",
			"Backup ID: " + b.job.ID,
			"Source Mode: " + b.job.SourceMode,
		},
	)
	if terr != nil {
		syslog.L.Error(terr).WithField("jobId", b.job.ID).Write()
	} else {
		b.updateBackupWithTask(task)
	}

	if rerr := b.job.SetBackupRetrySchedule(b.ctx, b.extraExclusions); rerr != nil {
		syslog.L.Error(rerr).WithField("jobId", b.job.ID).Write()
	}
}

func (b *BackupOperation) OnSuccess() {
}

func (b *BackupOperation) Cleanup() {
	b.cleanupOnce.Do(func() {
		b.mu.Lock()
		utils.ClearIOStats(b.job.CurrentPID)
		b.job.CurrentPID = 0
		b.mu.Unlock()

		if b.agentMount != nil {
			b.agentMount.Unmount()
			b.agentMount.CloseMount()
		}
		if b.s3Mount != nil {
			b.s3Mount.Unmount()
			b.s3Mount.CloseMount()
		}
		if b.logger != nil {
			_ = b.logger.Close()
		}
		if b.queueTask != nil {
			b.queueTask.Close()
		}

		close(b.errorMonitorDone)
	})
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

func (b *BackupOperation) validateTargetConnection() error {
	select {
	case <-b.Context().Done():
		return jobs.ErrCanceled
	default:
	}

	if b.skipCheck {
		return nil
	}

	switch b.job.Target.Type {
	case database.TargetTypeAgent:
		_, exists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(b.job.Target.GetHostname())
		if !exists {
			return fmt.Errorf("%w: %s", ErrTargetUnreachable, b.job.Target.Name)
		}
	case database.TargetTypeLocal:
		if _, err := os.Stat(b.job.Target.Path); err != nil {
			return fmt.Errorf("%w: %s (%v)", ErrTargetUnreachable, b.job.Target.Name, err)
		}
	case database.TargetTypeS3:
	}

	return nil
}

func (b *BackupOperation) runTargetMountScript(target database.Target) error {
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

func (b *BackupOperation) mountSource(target database.Target) (string, *mount.AgentMount, *mount.S3Mount, error) {
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

	if target.IsAgent() {
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
		if latestBackup, err := b.storeInstance.Database.GetBackup(b.job.ID); err == nil {
			b.job = latestBackup
		}
		job = b.job
		b.mu.Unlock()

		if agentMount.IsEmpty() {
			return "", agentMount, nil, ErrMountEmpty
		}
	} else if target.IsS3() {
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

func (b *BackupOperation) startBackup(srcPath string, target database.Target) (*exec.Cmd, proxmox.Task, string, error) {
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

	cmd, err := prepareBackupCommand(b.Context(), job, b.storeInstance, srcPath, target.IsAgent(), extraExclusions)
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

	go monitorPBSClientLogs(b.logger.Path, cmd, b.errorMonitorDone)

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

func (b *BackupOperation) startTaskMonitoring(target database.Target) (chan proxmox.Task, chan struct{}, chan error) {
	readyChan := make(chan struct{})
	taskChan := make(chan proxmox.Task, 1)
	errChan := make(chan error, 1)

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	go func() {
		defer syslog.L.Info().WithMessage("monitor goroutine closing").Write()

		timedCtx, timedCancel := context.WithTimeout(b.Context(), 20*time.Second)
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

func (b *BackupOperation) waitForCompletion(cmd *exec.Cmd, currOwner string) {
	b.mu.RLock()
	job := b.job
	extraExclusions := b.extraExclusions
	b.mu.RUnlock()

	done := make(chan error, 1)

	go func() {
		done <- cmd.Wait()
	}()

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

	succeeded, warningsNum := b.processPBSLogs(nil)

	if currOwner != "" {
		syslog.L.Info().WithJob(b.job.ID).WithMessage("setting owner to datastore owner")
		_ = SetDatastoreOwner(b.job, b.storeInstance, currOwner)
	}

	syslog.L.Info().WithJob(b.job.ID).WithMessage("checking post-backup script")
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

func (b *BackupOperation) updateBackupWithTask(task proxmox.Task) {
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
