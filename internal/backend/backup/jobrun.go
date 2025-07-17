//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

	ErrMountInitialization  = errors.New("mount initialization error")
	ErrPrepareBackupCommand = errors.New("failed to prepare backup command")

	ErrTaskMonitoringInitializationFailed = errors.New("task monitoring initialization failed")
	ErrTaskMonitoringTimedOut             = errors.New("task monitoring initialization timed out")

	ErrProxmoxBackupClientStart = errors.New("proxmox-backup-client start error")

	ErrNilTask               = errors.New("received nil task")
	ErrTaskDetectionFailed   = errors.New("task detection failed")
	ErrTaskDetectionTimedOut = errors.New("task detection timed out")

	ErrJobStatusUpdateFailed = errors.New("failed to update job status")
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
	lock            *sync.Mutex
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
) (*BackupOperation, error) {
	return &BackupOperation{
		job:             job,
		storeInstance:   storeInstance,
		skipCheck:       skipCheck,
		web:             web,
		logger:          syslog.CreateBackupLogger(job.ID),
		extraExclusions: extraExclusions,
	}, nil
}

func (op *BackupOperation) PreScript(ctx context.Context) error {
	if strings.TrimSpace(op.job.PreScript) == "" {
		return nil
	}

	op.queueTask.UpdateDescription("running pre-backup script")

	envVars, err := utils.StructToEnvVars(op.job)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, modEnvVars, err := utils.RunShellScript(ctx, op.job.PreScript, envVars)
	if err != nil {
		syslog.L.Error(err).WithJob(op.job.ID).WithMessage("error encountered while running job pre-backup script").Write()
	}
	syslog.L.Info().WithJob(op.job.ID).WithMessage(scriptOut).WithField("script", op.job.PreScript).Write()

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
	var err error

	errorMonitorDone := make(chan struct{})

	var agentMount *mount.AgentMount

	postScriptExecute := func() {
		if op.job.PostScript != "" {
			op.queueTask.UpdateDescription("running post-backup script")

			envVars, err := utils.StructToEnvVars(op.job)
			if err != nil {
				envVars = []string{}
			}

			scriptOut, _, err := utils.RunShellScript(ctx, op.job.PostScript, envVars)
			if err != nil {
				syslog.L.Error(err).WithMessage("error encountered while running job post-backup script").Write()
			}
			syslog.L.Info().WithMessage(scriptOut).WithField("script", op.job.PostScript).Write()
		}
	}

	errCleanUp := func() {
		utils.ClearIOStats(op.job.CurrentPID)
		op.job.CurrentPID = 0

		if op.lock != nil {
			op.lock.Unlock()
		}
		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if op.logger != nil {
			_ = op.logger.Close()
		}

		postScriptExecute()
		close(errorMonitorDone)
	}

	target, err := op.storeInstance.Database.GetTarget(op.job.Target)
	if err != nil {
		errCleanUp()
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", ErrTargetNotFound, op.job.Target)
		}
		return fmt.Errorf("%w: %v", ErrTargetGet, err)
	}

	if target.MountScript != "" {
		op.queueTask.UpdateDescription("running target mount script")

		envVars, err := utils.StructToEnvVars(target)
		if err != nil {
			envVars = []string{}
		}

		scriptOut, _, err := utils.RunShellScript(ctx, target.MountScript, envVars)
		if err != nil {
			syslog.L.Error(err).WithMessage("error encountered while running mount script").Write()
		}
		syslog.L.Info().WithMessage(scriptOut).WithField("script", target.MountScript).Write()
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")

	if !op.skipCheck && isAgent {
		targetSplit := strings.Split(target.Name, " - ")
		_, exists := op.storeInstance.ARPCSessionManager.GetSession(targetSplit[0])
		if !exists {
			errCleanUp()
			return fmt.Errorf("%w: %s", ErrTargetUnreachable, op.job.Target)
		}
	} else if !op.skipCheck && !isAgent {
		_, err := os.Stat(target.Path)
		if err != nil {
			errCleanUp()
			return fmt.Errorf("%w: %s (%v)", ErrTargetUnreachable, op.job.Target, err)
		}
	}

	op.queueTask.UpdateDescription("mounting target to server")

	srcPath := target.Path
	if isAgent {
		if op.job.SourceMode == "snapshot" {
			op.queueTask.UpdateDescription("waiting for agent to finish snapshot")
		}

		agentMount, err = mount.Mount(op.storeInstance, op.job, target)
		if err != nil {
			errCleanUp()
			return fmt.Errorf("%w: %v", ErrMountInitialization, err)
		}
		srcPath = agentMount.Path

		// In case mount updates the job.
		latestAgent, err := op.storeInstance.Database.GetJob(op.job.ID)
		if err == nil {
			op.job = latestAgent
		}
	}
	srcPath = filepath.Join(srcPath, op.job.Subpath)

	op.queueTask.UpdateDescription("waiting for proxmox-backup-client to start")

	cmd, err := prepareBackupCommand(ctx, op.job, op.storeInstance, srcPath, isAgent, op.extraExclusions)
	if err != nil {
		errCleanUp()
		return fmt.Errorf("%w: %v", ErrPrepareBackupCommand, err)
	}

	readyChan := make(chan struct{})
	taskResultChan := make(chan proxmox.Task, 1)
	taskErrorChan := make(chan error, 1)

	monitorCtx, monitorCancel := context.WithTimeout(ctx, 20*time.Second)
	defer monitorCancel()

	syslog.L.Info().WithMessage("starting monitor goroutine").Write()
	go func() {
		defer syslog.L.Info().WithMessage("monitor goroutine closing").Write()
		task, err := proxmox.GetJobTask(monitorCtx, readyChan, op.job, target)
		if err != nil {
			syslog.L.Error(err).WithMessage("found error in getjobtask return").Write()

			select {
			case taskErrorChan <- err:
			case <-monitorCtx.Done():
			}
			return
		}

		syslog.L.Info().WithMessage("found task in getjobtask return").WithField("task", task.UPID).Write()

		select {
		case taskResultChan <- task:
		case <-monitorCtx.Done():
		}
	}()

	select {
	case <-readyChan:
	case err := <-taskErrorChan:
		monitorCancel()
		errCleanUp()
		return fmt.Errorf("%w: %v", ErrTaskMonitoringInitializationFailed, err)
	case <-monitorCtx.Done():
		errCleanUp()
		return fmt.Errorf("%w: %v", ErrTaskMonitoringTimedOut, monitorCtx.Err())
	}

	currOwner, err := GetCurrentOwner(op.job, op.storeInstance)
	if err != nil {
		syslog.L.Error(err).Write()
	}

	err = FixDatastore(op.job, op.storeInstance)
	if err != nil {
		syslog.L.Error(err).Write()
	}

	stdoutWriter := io.MultiWriter(op.logger, os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter

	syslog.L.Info().WithMessage("starting backup job").WithField("args", cmd.Args).Write()
	if err := cmd.Start(); err != nil {
		monitorCancel()
		if currOwner != "" {
			_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
		}
		errCleanUp()
		return fmt.Errorf("%w (%s): %v",
			ErrProxmoxBackupClientStart, cmd.String(), err)
	}

	if cmd.Process != nil {
		op.job.CurrentPID = cmd.Process.Pid
	}

	go monitorPBSClientLogs(op.logger.Path, cmd, errorMonitorDone)

	syslog.L.Info().WithMessage("waiting for task monitoring results").Write()
	var task proxmox.Task
	select {
	case task = <-taskResultChan:
	case err := <-taskErrorChan:
		monitorCancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
		}
		if os.IsNotExist(err) {
			return ErrNilTask
		}
		return fmt.Errorf("%w: %v", ErrTaskDetectionFailed, err)
	case <-monitorCtx.Done():
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
		}
		return fmt.Errorf("%w: %v", ErrTaskDetectionTimedOut, monitorCtx.Err())
	}

	if err := updateJobStatus(false, 0, op.job, task, op.storeInstance); err != nil {
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(op.job, op.storeInstance, currOwner)
		}
		return fmt.Errorf("%w: %v", ErrJobStatusUpdateFailed, err)
	}

	syslog.L.Info().WithMessage("task monitoring finished").WithField("task", task.UPID).Write()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	op.Task = task
	op.waitGroup = wg

	go func() {
		defer wg.Done()
		defer func() {
			if op.lock != nil {
				op.lock.Unlock()
			}
		}()

		if err := cmd.Wait(); err != nil {
			op.err = err
		}

		utils.ClearIOStats(op.job.CurrentPID)
		op.job.CurrentPID = 0

		close(errorMonitorDone)

		for _, extraExclusion := range op.extraExclusions {
			syslog.L.Warn().WithJob(op.job.ID).WithMessage(fmt.Sprintf("skipped %s due to an error from previous retry attempts", extraExclusion)).Write()
		}

		// Agent mount must still be connected after proxmox-backup-client terminates
		// If not, then the client side process crashed
		gracefulEnd := true
		if agentMount != nil {
			mountConnected := agentMount.IsConnected()
			if !mountConnected {
				gracefulEnd = false
			}
		}

		_ = op.logger.Flush()

		succeeded, cancelled, warningsNum, errorPath, err := processPBSProxyLogs(gracefulEnd, task.UPID, op.logger)
		if err != nil {
			syslog.L.Error(err).
				WithMessage("failed to process logs").
				Write()
		}

		_ = op.logger.Close()

		if errorPath != "" {
			op.extraExclusions = append(op.extraExclusions, errorPath)
		}

		if newUpid, err := proxmox.ChangeUPIDStartTime(task.UPID, op.logger.StartTime); err == nil {
			task.UPID = newUpid
		}

		if err := updateJobStatus(succeeded, warningsNum, op.job, task, op.storeInstance); err != nil {
			syslog.L.Error(err).
				WithMessage("failed to update job status - post cmd.Wait").
				Write()
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

		postScriptExecute()
	}()

	return nil
}
