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
	rpclocker "github.com/pbs-plus/pbs-plus/internal/proxy/locker"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
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
	Task      proxmox.Task
	waitGroup *sync.WaitGroup
	err       error
}

// Wait blocks until the backup operation is complete.
func (b *BackupOperation) Wait() error {
	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}
	return b.err
}

func RunBackup(
	ctx context.Context,
	job types.Job,
	storeInstance *store.Store,
	skipCheck bool,
	web bool,
	extraExclusions *[]string,
) (*BackupOperation, error) {
	var err error

	if storeInstance.Locker == nil {
		storeInstance.Locker, err = rpclocker.NewLockerClient(constants.LockSocketPath)
		if err != nil {
			syslog.L.Error(err).WithMessage("locker server failed, restarting")
			return nil, err
		}
	}

	if locked, err := storeInstance.Locker.TryLock("BackupJob-" + job.ID); err != nil || !locked {
		return nil, ErrOneInstance
	}

	clientLogFile := syslog.GetOrCreateBackupLoggerStatic(job.ID)

	errorMonitorDone := make(chan struct{})

	var agentMount *mount.AgentMount

	errCleanUp := func() {
		utils.ClearIOStats(job.CurrentPID)
		job.CurrentPID = 0

		storeInstance.Locker.Unlock("BackupJob-" + job.ID)
		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if clientLogFile != nil {
			_ = clientLogFile.Close()
		}
		close(errorMonitorDone)
	}

	queueTask, err := proxmox.GenerateQueuedTask(job, web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		queueTaskLogPath, err := proxmox.GetLogPath(queueTask.UPID)
		if err == nil {
			defer os.Remove(queueTaskLogPath)
		}

		if err := updateJobStatus(false, 0, job, queueTask, storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}

	if err := storeInstance.Locker.Lock("BackupJobInitializing"); err != nil {
		errCleanUp()
		return nil, fmt.Errorf("%w: %v", ErrBackupMutexLock, err)
	}
	defer storeInstance.Locker.Unlock("BackupJobInitializing")

	if proxmox.Session.APIToken == nil {
		errCleanUp()
		return nil, ErrAPITokenRequired
	}

	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		errCleanUp()
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrTargetNotFound, job.Target)
		}
		return nil, fmt.Errorf("%w: %v", ErrTargetGet, err)
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")

	if !skipCheck && isAgent {
		targetSplit := strings.Split(target.Name, " - ")
		_, exists := storeInstance.ARPCSessionManager.GetSession(targetSplit[0])
		if !exists {
			errCleanUp()
			return nil, fmt.Errorf("%w: %s", ErrTargetUnreachable, job.Target)
		}
	} else if !skipCheck && !isAgent {
		_, err := os.Stat(target.Path)
		if err != nil {
			errCleanUp()
			return nil, fmt.Errorf("%w: %s (%v)", ErrTargetUnreachable, job.Target, err)
		}
	}

	srcPath := target.Path
	if isAgent {
		agentMount, err = mount.Mount(storeInstance, job, target)
		if err != nil {
			errCleanUp()
			return nil, fmt.Errorf("%w: %v", ErrMountInitialization, err)
		}
		srcPath = agentMount.Path

		// In case mount updates the job.
		latestAgent, err := storeInstance.Database.GetJob(job.ID)
		if err == nil {
			job = latestAgent
		}
	}
	srcPath = filepath.Join(srcPath, job.Subpath)

	cmd, err := prepareBackupCommand(ctx, job, storeInstance, srcPath, isAgent, *extraExclusions)
	if err != nil {
		errCleanUp()
		return nil, fmt.Errorf("%w: %v", ErrPrepareBackupCommand, err)
	}

	readyChan := make(chan struct{})
	taskResultChan := make(chan proxmox.Task, 1)
	taskErrorChan := make(chan error, 1)

	monitorCtx, monitorCancel := context.WithTimeout(ctx, 20*time.Second)
	defer monitorCancel()

	syslog.L.Info().WithMessage("starting monitor goroutine").Write()
	go func() {
		defer syslog.L.Info().WithMessage("monitor goroutine closing").Write()
		task, err := proxmox.Session.GetJobTask(monitorCtx, readyChan, job, target)
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
		return nil, fmt.Errorf("%w: %v", ErrTaskMonitoringInitializationFailed, err)
	case <-monitorCtx.Done():
		errCleanUp()
		return nil, fmt.Errorf("%w: %v", ErrTaskMonitoringTimedOut, monitorCtx.Err())
	}

	currOwner, _ := GetCurrentOwner(job, storeInstance)
	_ = FixDatastore(job, storeInstance)

	stdoutWriter := io.MultiWriter(clientLogFile.File, os.Stdout)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stdoutWriter

	syslog.L.Info().WithMessage("starting backup job").WithField("args", cmd.Args).Write()
	if err := cmd.Start(); err != nil {
		monitorCancel()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		errCleanUp()
		return nil, fmt.Errorf("%w (%s): %v",
			ErrProxmoxBackupClientStart, cmd.String(), err)
	}

	if cmd.Process != nil {
		job.CurrentPID = cmd.Process.Pid
	}

	go monitorPBSClientLogs(clientLogFile.Path, cmd, errorMonitorDone)

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
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		if os.IsNotExist(err) {
			return nil, ErrNilTask
		}
		return nil, fmt.Errorf("%w: %v", ErrTaskDetectionFailed, err)
	case <-monitorCtx.Done():
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return nil, fmt.Errorf("%w: %v", ErrTaskDetectionTimedOut, monitorCtx.Err())
	}

	if err := updateJobStatus(false, 0, job, task, storeInstance); err != nil {
		errCleanUp()
		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}
		return nil, fmt.Errorf("%w: %v", ErrJobStatusUpdateFailed, err)
	}

	syslog.L.Info().WithMessage("task monitoring finished").WithField("task", task.UPID).Write()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	operation := &BackupOperation{
		Task:      task,
		waitGroup: wg,
	}

	go func() {
		defer wg.Done()
		defer func() {
			storeInstance.Locker.Unlock("BackupJob-" + job.ID)
		}()

		if err := cmd.Wait(); err != nil {
			operation.err = err
		}

		utils.ClearIOStats(job.CurrentPID)
		job.CurrentPID = 0

		close(errorMonitorDone)

		for _, extraExclusion := range *extraExclusions {
			syslog.L.Warn().WithJob(job.ID).WithMessage(fmt.Sprintf("skipped %s due to an error from previous retry attempts", extraExclusion)).Write()
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

		succeeded, cancelled, warningsNum, errorPath, err := processPBSProxyLogs(gracefulEnd, task.UPID, clientLogFile)
		if err != nil {
			syslog.L.Error(err).
				WithMessage("failed to process logs").
				Write()
		}

		if errorPath != "" {
			*extraExclusions = append(*extraExclusions, errorPath)
		}

		_ = clientLogFile.Close()

		if err := updateJobStatus(succeeded, warningsNum, job, task, storeInstance); err != nil {
			syslog.L.Error(err).
				WithMessage("failed to update job status - post cmd.Wait").
				Write()
		}

		if succeeded || cancelled {
			system.RemoveAllRetrySchedules(job)
		} else {
			if err := system.SetRetrySchedule(job, *extraExclusions); err != nil {
				syslog.L.Error(err).WithField("jobId", job.ID).Write()
			}
		}

		if currOwner != "" {
			_ = SetDatastoreOwner(job, storeInstance, currOwner)
		}

		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
	}()

	return operation, nil
}
