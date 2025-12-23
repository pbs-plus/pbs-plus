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

	"github.com/pbs-plus/pbs-plus/internal/backend/mount"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

// Sentinel error values.
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

// BackupOperation encapsulates a backup operation.
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

// Wait blocks until the backup operation is complete.
func (b *BackupOperation) Wait() error {
	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}
	return b.err
}

func NewJob(
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

func (op *BackupOperation) PreScript(ctx context.Context) error {
	if strings.TrimSpace(op.job.PreScript) == "" {
		return nil
	}

	select {
	case <-ctx.Done():
		return ErrCanceled
	default:
	}

	op.queueTask.UpdateDescription("running pre-backup script")

	envVars, err := utils.StructToEnvVars(op.job)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, modEnvVars, err := utils.RunShellScript(ctx, op.job.PreScript, envVars)
	syslog.L.Info().WithJob(op.job.ID).WithMessage(scriptOut).WithField("script", op.job.PreScript).Write()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			syslog.L.Info().WithJob(op.job.ID).WithMessage("pre-backup script canceled").Write()
			return ErrCanceled
		}
		syslog.L.Error(err).WithJob(op.job.ID).WithMessage("error encountered while running job pre-backup script").Write()
		return err
	}

	if newNs, ok := modEnvVars["PBS_PLUS__NAMESPACE"]; ok {
		latestJob, err := op.storeInstance.Database.GetJob(op.job.ID)
		if err == nil {
			op.job = latestJob
		}
		op.job.Namespace = newNs
		err = op.storeInstance.Database.UpdateJob(nil, op.job)
		if err != nil {
			syslog.L.Error(err).WithJob(op.job.ID).WithMessage("error encountered while running job pre-backup script update").Write()
		}
	}

	return nil
}

func (op *BackupOperation) Execute(ctx context.Context) error {
	errorMonitorDone := make(chan struct{})
	var agentMount *mount.AgentMount
	var s3Mount *mount.S3Mount

	cleanup := func() {
		utils.ClearIOStats(op.job.CurrentPID)
		op.job.CurrentPID = 0

		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if s3Mount != nil {
			s3Mount.Unmount()
		}
		if op.logger != nil {
			_ = op.logger.Close()
		}
		close(errorMonitorDone)
	}

	select {
	case <-ctx.Done():
		cleanup()
		return ErrCanceled
	default:
	}

	target, err := op.getAndValidateTarget(ctx)
	if err != nil {
		cleanup()
		return err
	}

	select {
	case <-ctx.Done():
		cleanup()
		return ErrCanceled
	default:
	}

	if err := op.runTargetMountScript(ctx, target); err != nil {
		cleanup()
		return err
	}

	select {
	case <-ctx.Done():
		cleanup()
		return ErrCanceled
	default:
	}

	srcPath, agentMount, s3Mount, err := op.mountSource(ctx, target)
	if err != nil {
		cleanup()
		return err
	}

	select {
	case <-ctx.Done():
		cleanup()
		return ErrCanceled
	default:
	}

	cmd, task, currOwner, err := op.startBackup(ctx, srcPath, target, errorMonitorDone)
	if err != nil {
		cleanup()
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	op.Task = task
	op.waitGroup = wg

	go op.waitForCompletion(ctx, cmd, task, currOwner, agentMount, s3Mount, errorMonitorDone, wg)

	return nil
}

func (op *BackupOperation) getAndValidateTarget(ctx context.Context) (types.Target, error) {
	select {
	case <-ctx.Done():
		return types.Target{}, ErrCanceled
	default:
	}

	target, err := op.storeInstance.Database.GetTarget(op.job.Target)
	if err != nil {
		if os.IsNotExist(err) {
			return target, fmt.Errorf("%w: %s", ErrTargetNotFound, op.job.Target)
		}
		return target, fmt.Errorf("%w: %v", ErrTargetGet, err)
	}

	if op.skipCheck {
		return target, nil
	}

	if target.IsAgent {
		targetSplit := strings.Split(target.Name, " - ")
		_, exists := op.storeInstance.ARPCAgentsManager.GetStreamPipe(targetSplit[0])
		if !exists {
			return target, fmt.Errorf("%w: %s", ErrTargetUnreachable, op.job.Target)
		}
	} else if !target.IsS3 {
		if _, err := os.Stat(target.Path); err != nil {
			return target, fmt.Errorf("%w: %s (%v)", ErrTargetUnreachable, op.job.Target, err)
		}
	}

	return target, nil
}

func (op *BackupOperation) runTargetMountScript(ctx context.Context, target types.Target) error {
	if target.MountScript == "" {
		return nil
	}

	select {
	case <-ctx.Done():
		return ErrCanceled
	default:
	}

	op.queueTask.UpdateDescription("running target mount script")

	envVars, err := utils.StructToEnvVars(target)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, _, err := utils.RunShellScript(ctx, target.MountScript, envVars)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return ErrCanceled
		}
		syslog.L.Error(err).WithMessage("error encountered while running mount script").Write()
	}
	syslog.L.Info().WithMessage(scriptOut).WithField("script", target.MountScript).Write()
	return nil
}

func (op *BackupOperation) mountSource(ctx context.Context, target types.Target) (string, *mount.AgentMount, *mount.S3Mount, error) {
	select {
	case <-ctx.Done():
		return "", nil, nil, ErrCanceled
	default:
	}

	op.queueTask.UpdateDescription("mounting target to server")

	srcPath := target.Path
	var agentMount *mount.AgentMount
	var s3Mount *mount.S3Mount
	var err error

	if target.IsAgent {
		if op.job.SourceMode == "snapshot" {
			op.queueTask.UpdateDescription("waiting for agent to finish snapshot")
		}

		agentMount, err = mount.AgentFSMount(op.storeInstance, op.job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = agentMount.Path

		select {
		case <-ctx.Done():
			agentMount.Unmount()
			agentMount.CloseMount()
			return "", nil, nil, ErrCanceled
		default:
		}

		if latestJob, err := op.storeInstance.Database.GetJob(op.job.ID); err == nil {
			op.job = latestJob
		}

		if agentMount.IsEmpty() {
			return "", agentMount, nil, ErrMountEmpty
		}
	} else if target.IsS3 {
		s3Mount, err = mount.S3FSMount(op.storeInstance, op.job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = s3Mount.Path

		select {
		case <-ctx.Done():
			s3Mount.Unmount()
			return "", nil, nil, ErrCanceled
		default:
		}

		if latestJob, err := op.storeInstance.Database.GetJob(op.job.ID); err == nil {
			op.job = latestJob
		}

		if s3Mount.IsEmpty() {
			return "", nil, s3Mount, ErrMountEmpty
		}
	}

	if !target.IsS3 {
		srcPath = filepath.Join(srcPath, op.job.Subpath)
	}

	return srcPath, agentMount, s3Mount, nil
}

func (op *BackupOperation) startBackup(ctx context.Context, srcPath string, target types.Target, errorMonitorDone chan struct{}) (*exec.Cmd, proxmox.Task, string, error) {
	select {
	case <-ctx.Done():
		return nil, proxmox.Task{}, "", ErrCanceled
	default:
	}

	op.queueTask.UpdateDescription("waiting for proxmox-backup-client to start")

	cmd, err := prepareBackupCommand(ctx, op.job, op.storeInstance, srcPath, target.IsAgent, op.extraExclusions)
	if err != nil {
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrPrepareBackupCommand, err)
	}

	task, readyChan, err := op.startTaskMonitoring(ctx, target)
	if err != nil {
		return nil, proxmox.Task{}, "", err
	}

	currOwner, _ := GetCurrentOwner(op.job, op.storeInstance)
	_ = FixDatastore(op.job, op.storeInstance)

	stdoutWriter := io.MultiWriter(op.logger, os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter

	syslog.L.Info().WithMessage("starting backup job").WithField("args", cmd.Args).Write()
	if err := cmd.Start(); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w (%s): %v", ErrProxmoxBackupClientStart, cmd.String(), err)
	}

	if cmd.Process != nil {
		op.job.CurrentPID = cmd.Process.Pid
	}

	go monitorPBSClientLogs(op.logger.Path, cmd, errorMonitorDone)

	syslog.L.Info().WithMessage("waiting for task monitoring results").Write()
	select {
	case <-readyChan:
		// Task detected successfully
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		if currOwner != "" {
			_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", ErrCanceled
	}

	if err := updateJobStatus(false, 0, op.job, task, op.storeInstance); err != nil {
		if currOwner != "" {
			_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
		}
		return nil, proxmox.Task{}, "", fmt.Errorf("%w: %v", ErrJobStatusUpdateFailed, err)
	}

	syslog.L.Info().WithMessage("task monitoring finished").WithField("task", task.UPID).Write()
	return cmd, task, currOwner, nil
}

func (op *BackupOperation) startTaskMonitoring(ctx context.Context, target types.Target) (proxmox.Task, chan struct{}, error) {
	readyChan := make(chan struct{})
	taskChan := make(chan proxmox.Task, 1)
	errChan := make(chan error, 1)

	monitorCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	syslog.L.Info().WithMessage("starting monitor goroutine").Write()
	go func() {
		defer syslog.L.Info().WithMessage("monitor goroutine closing").Write()
		task, err := proxmox.GetJobTask(monitorCtx, readyChan, op.job, target)
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

	select {
	case <-readyChan:
		// Continue
	case err := <-errChan:
		return proxmox.Task{}, nil, fmt.Errorf("%w: %v", ErrTaskMonitoringInitializationFailed, err)
	case <-monitorCtx.Done():
		if errors.Is(monitorCtx.Err(), context.Canceled) {
			return proxmox.Task{}, nil, ErrCanceled
		}
		return proxmox.Task{}, nil, fmt.Errorf("%w: %v", ErrTaskMonitoringTimedOut, monitorCtx.Err())
	}

	select {
	case task := <-taskChan:
		return task, readyChan, nil
	case err := <-errChan:
		if os.IsNotExist(err) {
			return proxmox.Task{}, nil, ErrNilTask
		}
		return proxmox.Task{}, nil, fmt.Errorf("%w: %v", ErrTaskDetectionFailed, err)
	case <-monitorCtx.Done():
		if errors.Is(monitorCtx.Err(), context.Canceled) {
			return proxmox.Task{}, nil, ErrCanceled
		}
		return proxmox.Task{}, nil, ErrTaskDetectionTimedOut
	}
}

func (op *BackupOperation) waitForCompletion(ctx context.Context, cmd *exec.Cmd, task proxmox.Task, currOwner string, agentMount *mount.AgentMount, s3Mount *mount.S3Mount, errorMonitorDone chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			op.err = err
		}
	case <-ctx.Done():
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done // Wait for process to actually exit
		op.err = ErrCanceled
	}

	utils.ClearIOStats(op.job.CurrentPID)
	op.job.CurrentPID = 0
	close(errorMonitorDone)

	for _, extraExclusion := range op.extraExclusions {
		syslog.L.Warn().WithJob(op.job.ID).WithMessage(fmt.Sprintf("skipped %s due to an error from previous retry attempts", extraExclusion)).Write()
	}

	gracefulEnd := true
	if agentMount != nil && !agentMount.IsConnected() {
		gracefulEnd = false
	}

	_ = op.logger.Flush()

	succeeded, cancelled, warningsNum, errorPath, err := processPBSProxyLogs(gracefulEnd, task.UPID, op.logger)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to process logs").Write()
	}

	_ = op.logger.Close()

	if errorPath != "" {
		op.extraExclusions = append(op.extraExclusions, errorPath)
	}

	if newUpid, err := proxmox.ChangeUPIDStartTime(task.UPID, op.logger.StartTime); err == nil {
		task.UPID = newUpid
	}

	if err := updateJobStatus(succeeded, warningsNum, op.job, task, op.storeInstance); err != nil {
		syslog.L.Error(err).WithMessage("failed to update job status - post cmd.Wait").Write()
	}

	if succeeded || cancelled {
		system.RemoveAllRetrySchedules(op.job)
	} else {
		if err := system.SetRetrySchedule(op.job, op.extraExclusions); err != nil {
			syslog.L.Error(err).WithField("jobId", op.job.ID).Write()
		}
	}

	if currOwner != "" {
		_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
	}

	if agentMount != nil {
		agentMount.Unmount()
		agentMount.CloseMount()
	}
	if s3Mount != nil {
		s3Mount.Unmount()
	}

	op.runPostScript(context.Background(), succeeded, warningsNum)
}

func (op *BackupOperation) runPostScript(ctx context.Context, success bool, warningsNum int) {
	if op.job.PostScript == "" {
		return
	}

	if op.queueTask != nil {
		op.queueTask.UpdateDescription("running post-backup script")
	}

	envVars, err := utils.StructToEnvVars(op.job)
	if err != nil {
		envVars = []string{}
	}

	if success {
		envVars = append(envVars, "PBS_PLUS__JOB_SUCCESS=true")
	} else {
		envVars = append(envVars, "PBS_PLUS__JOB_SUCCESS=false")
	}

	envVars = append(envVars, fmt.Sprintf("PBS_PLUS__JOB_WARNINGS=%d", warningsNum))

	scriptOut, _, err := utils.RunShellScript(ctx, op.job.PostScript, envVars)
	if err != nil {
		syslog.L.Error(err).WithMessage("error encountered while running job post-backup script").Write()
	}
	syslog.L.Info().WithMessage(scriptOut).WithField("script", op.job.PostScript).Write()
}
