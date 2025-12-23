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

func (b *BackupOperation) PreExecute(ctx context.Context) error {
	queueTask, err := proxmox.GenerateQueuedTask(b.job, b.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		if err := updateJobStatus(false, 0, b.job, queueTask.Task, b.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}
	b.queueTask = &queueTask

	return b.runPreScript(ctx)
}

func (b *BackupOperation) Execute(ctx context.Context) error {
	return b.executeBackup(ctx)
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

	if rerr := system.SetRetrySchedule(b.job, b.extraExclusions); rerr != nil {
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
	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}
	return b.err
}

func (b *BackupOperation) runPreScript(ctx context.Context) error {
	if strings.TrimSpace(b.job.PreScript) == "" {
		return nil
	}

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("running pre-backup script")

	envVars, err := utils.StructToEnvVars(b.job)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, modEnvVars, err := utils.RunShellScript(ctx, b.job.PreScript, envVars)
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
		latestJob, err := b.storeInstance.Database.GetJob(b.job.ID)
		if err == nil {
			b.job = latestJob
		}
		b.job.Namespace = newNs
		err = b.storeInstance.Database.UpdateJob(nil, b.job)
		if err != nil {
			syslog.L.Error(err).WithJob(b.job.ID).WithMessage("error encountered while running job pre-backup script update").Write()
		}
	}

	return nil
}

func (b *BackupOperation) executeBackup(ctx context.Context) error {
	errorMonitorDone := make(chan struct{})
	var agentMount *mount.AgentMount
	var s3Mount *mount.S3Mount

	cleanup := func() {
		utils.ClearIOStats(b.job.CurrentPID)
		b.job.CurrentPID = 0

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
	}

	select {
	case <-ctx.Done():
		cleanup()
		return jobs.ErrCanceled
	default:
	}

	target, err := b.getAndValidateTarget(ctx)
	if err != nil {
		cleanup()
		return err
	}

	select {
	case <-ctx.Done():
		cleanup()
		return jobs.ErrCanceled
	default:
	}

	if err := b.runTargetMountScript(ctx, target); err != nil {
		cleanup()
		return err
	}

	select {
	case <-ctx.Done():
		cleanup()
		return jobs.ErrCanceled
	default:
	}

	srcPath, agentMount, s3Mount, err := b.mountSource(ctx, target)
	if err != nil {
		cleanup()
		return err
	}

	select {
	case <-ctx.Done():
		cleanup()
		return jobs.ErrCanceled
	default:
	}

	cmd, task, currOwner, err := b.startBackup(ctx, srcPath, target, errorMonitorDone)
	if err != nil {
		cleanup()
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	b.Task = task
	b.waitGroup = wg

	go b.waitForCompletion(ctx, cmd, task, currOwner, agentMount, s3Mount, errorMonitorDone, wg)

	return nil
}

func (b *BackupOperation) getAndValidateTarget(ctx context.Context) (types.Target, error) {
	select {
	case <-ctx.Done():
		return types.Target{}, jobs.ErrCanceled
	default:
	}

	target, err := b.storeInstance.Database.GetTarget(b.job.Target)
	if err != nil {
		if os.IsNotExist(err) {
			return target, fmt.Errorf("%w: %s", ErrTargetNotFound, b.job.Target)
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
			return target, fmt.Errorf("%w: %s", ErrTargetUnreachable, b.job.Target)
		}
	} else if !target.IsS3 {
		if _, err := os.Stat(target.Path); err != nil {
			return target, fmt.Errorf("%w: %s (%v)", ErrTargetUnreachable, b.job.Target, err)
		}
	}

	return target, nil
}

func (b *BackupOperation) runTargetMountScript(ctx context.Context, target types.Target) error {
	if target.MountScript == "" {
		return nil
	}

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("running target mount script")

	envVars, err := utils.StructToEnvVars(target)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, _, err := utils.RunShellScript(ctx, target.MountScript, envVars)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return jobs.ErrCanceled
		}
		syslog.L.Error(err).WithMessage("error encountered while running mount script").Write()
	}
	syslog.L.Info().WithMessage(scriptOut).WithField("script", target.MountScript).Write()
	return nil
}

func (b *BackupOperation) mountSource(ctx context.Context, target types.Target) (string, *mount.AgentMount, *mount.S3Mount, error) {
	select {
	case <-ctx.Done():
		return "", nil, nil, jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("mounting target to server")

	srcPath := target.Path
	var agentMount *mount.AgentMount
	var s3Mount *mount.S3Mount
	var err error

	if target.IsAgent {
		if b.job.SourceMode == "snapshot" {
			b.queueTask.UpdateDescription("waiting for agent to finish snapshot")
		}

		agentMount, err = mount.AgentFSMount(b.storeInstance, b.job, target)
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

		if latestJob, err := b.storeInstance.Database.GetJob(b.job.ID); err == nil {
			b.job = latestJob
		}

		if agentMount.IsEmpty() {
			return "", agentMount, nil, ErrMountEmpty
		}
	} else if target.IsS3 {
		s3Mount, err = mount.S3FSMount(b.storeInstance, b.job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = s3Mount.Path

		select {
		case <-ctx.Done():
			s3Mount.Unmount()
			return "", nil, nil, jobs.ErrCanceled
		default:
		}

		if latestJob, err := b.storeInstance.Database.GetJob(b.job.ID); err == nil {
			b.job = latestJob
		}

		if s3Mount.IsEmpty() {
			return "", nil, s3Mount, ErrMountEmpty
		}
	}

	if !target.IsS3 {
		srcPath = filepath.Join(srcPath, b.job.Subpath)
	}

	return srcPath, agentMount, s3Mount, nil
}

func (b *BackupOperation) startBackup(ctx context.Context, srcPath string, target types.Target, errorMonitorDone chan struct{}) (*exec.Cmd, proxmox.Task, string, error) {
	select {
	case <-ctx.Done():
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("waiting for proxmox-backup-client to start")

	cmd, err := prepareBackupCommand(ctx, b.job, b.storeInstance, srcPath, target.IsAgent, b.extraExclusions)
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

	currOwner, _ := GetCurrentOwner(b.job, b.storeInstance)
	_ = FixDatastore(b.job, b.storeInstance)

	stdoutWriter := io.MultiWriter(b.logger, os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter

	syslog.L.Info().WithMessage("starting backup job").WithField("args", cmd.Args).Write()
	if err := cmd.Start(); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(b.job, b.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w (%s): %v", ErrProxmoxBackupClientStart, cmd.String(), err)
	}

	if cmd.Process != nil {
		b.job.CurrentPID = cmd.Process.Pid
	}

	go monitorPBSClientLogs(b.logger.Path, cmd, errorMonitorDone)

	var task proxmox.Task
	syslog.L.Info().WithMessage("waiting for task monitoring results").Write()

	select {
	case task = <-taskChan:
	case err := <-errChan:
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrTaskDetectionFailed, err)
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		if currOwner != "" {
			_ = SetDatastoreOwner(b.job, b.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", jobs.ErrCanceled
	}

	if err := updateJobStatus(false, 0, b.job, task, b.storeInstance); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(b.job, b.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrJobStatusUpdateFailed, err)
	}

	syslog.L.Info().WithMessage("task monitoring finished").WithField("task", task.UPID).Write()
	return cmd, task, currOwner, nil
}

func (b *BackupOperation) startTaskMonitoring(ctx context.Context, target types.Target) (chan proxmox.Task, chan struct{}, chan error) {
	readyChan := make(chan struct{})
	taskChan := make(chan proxmox.Task, 1)
	errChan := make(chan error, 1)

	monitorCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	syslog.L.Info().WithMessage("starting monitor goroutine").Write()
	go func() {
		defer syslog.L.Info().WithMessage("monitor goroutine closing").Write()
		task, err := proxmox.GetJobTask(monitorCtx, readyChan, b.job, target)
		if err != nil {
			syslog.L.Error(err).WithMessage("found error in getjobtask return").Write()
			select {
			case errChan <- err:
			case <-monitorCtx.Done():
			}
			return
		}

		syslog.L.Info().WithMessage("found task in getjobtask return").WithField("task", task.UPID).Write()
		select {
		case taskChan <- task:
		case <-monitorCtx.Done():
		}
	}()

	return taskChan, readyChan, errChan
}

func (b *BackupOperation) waitForCompletion(ctx context.Context, cmd *exec.Cmd, task proxmox.Task, currOwner string, agentMount *mount.AgentMount, s3Mount *mount.S3Mount, errorMonitorDone chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			b.err = err
		}
	case <-ctx.Done():
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
		b.err = jobs.ErrCanceled
	}

	utils.ClearIOStats(b.job.CurrentPID)
	b.job.CurrentPID = 0
	close(errorMonitorDone)

	for _, extraExclusion := range b.extraExclusions {
		syslog.L.Warn().WithJob(b.job.ID).WithMessage(fmt.Sprintf("skipped %s due to an error from previous retry attempts", extraExclusion)).Write()
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

	_ = b.logger.Close()

	if errorPath != "" {
		b.extraExclusions = append(b.extraExclusions, errorPath)
	}

	if newUpid, err := proxmox.ChangeUPIDStartTime(task.UPID, b.logger.StartTime); err == nil {
		task.UPID = newUpid
	}

	if err := updateJobStatus(succeeded, warningsNum, b.job, task, b.storeInstance); err != nil {
		syslog.L.Error(err).WithMessage("failed to update job status - post cmd.Wait").Write()
	}

	if succeeded || cancelled {
		system.RemoveAllRetrySchedules(b.job)
	} else {
		if err := system.SetRetrySchedule(b.job, b.extraExclusions); err != nil {
			syslog.L.Error(err).WithField("jobId", b.job.ID).Write()
		}
	}

	if currOwner != "" {
		_ = SetDatastoreOwner(b.job, b.storeInstance, currOwner)
	}

	if agentMount != nil {
		agentMount.Unmount()
		agentMount.CloseMount()
	}
	if s3Mount != nil {
		s3Mount.Unmount()
	}

	b.runPostScript(context.Background(), succeeded, warningsNum)
}

func (b *BackupOperation) runPostScript(ctx context.Context, success bool, warningsNum int) {
	if b.job.PostScript == "" {
		return
	}

	if b.queueTask != nil {
		b.queueTask.UpdateDescription("running post-backup script")
	}

	envVars, err := utils.StructToEnvVars(b.job)
	if err != nil {
		envVars = []string{}
	}

	if success {
		envVars = append(envVars, "PBS_PLUS__JOB_SUCCESS=true")
	} else {
		envVars = append(envVars, "PBS_PLUS__JOB_SUCCESS=false")
	}

	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_WARNINGS=%d", warningsNum))

	scriptOut, _, err := utils.RunShellScript(ctx, b.job.PostScript, envVars)
	if err != nil {
		syslog.L.Error(err).WithMessage("error encountered while running job post-backup script").Write()
	}
	syslog.L.Info().WithMessage(scriptOut).WithField("script", b.job.PostScript).Write()
}

func (b *BackupOperation) createOK(err error) {
	task, terr := proxmox.GenerateTaskOKFile(
		b.job,
		[]string{
			"Done handling from a job run request",
			"Job ID: " + b.job.ID,
			"Source Mode: " + b.job.SourceMode,
			"Response: " + err.Error(),
		},
	)
	if terr != nil {
		syslog.L.Error(terr).WithField("jobId", b.job.ID).Write()
		return
	}

	latest, gerr := b.storeInstance.Database.GetJob(b.job.ID)
	if gerr != nil {
		latest = b.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime
	latest.LastSuccessfulEndtime = task.EndTime
	latest.LastSuccessfulUpid = task.UPID

	if uerr := b.storeInstance.Database.UpdateJob(nil, latest); uerr != nil {
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}

func (b *BackupOperation) updateJobWithTask(task proxmox.Task) {
	latest, gerr := b.storeInstance.Database.GetJob(b.job.ID)
	if gerr != nil {
		latest = b.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime

	if uerr := b.storeInstance.Database.UpdateJob(nil, latest); uerr != nil {
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}

