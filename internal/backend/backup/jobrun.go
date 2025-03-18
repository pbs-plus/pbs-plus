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

	"github.com/alexflint/go-filemutex"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/system"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
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
) (*BackupOperation, error) {
	jobInstanceMutex, err := filemutex.New(
		fmt.Sprintf("/tmp/pbs-plus-mutex-job-%s", job.ID),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJobMutexCreation, err)
	}
	if err := jobInstanceMutex.TryLock(); err != nil {
		return nil, ErrOneInstance
	}

	clientLogFile, err := os.CreateTemp("", fmt.Sprintf("backup-%s-stdout-*", job.ID))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStdoutTempCreation, err)
	}
	clientLogPath := clientLogFile.Name()

	errorMonitorDone := make(chan struct{})

	var agentMount *mount.AgentMount

	errCleanUp := func() {
		utils.ClearIOStats(job.CurrentPID)
		job.CurrentPID = 0

		_ = jobInstanceMutex.Close()
		if agentMount != nil {
			agentMount.Unmount()
			agentMount.CloseMount()
		}
		if clientLogFile != nil {
			_ = clientLogFile.Close()
			_ = os.Remove(clientLogPath)
		}
		close(errorMonitorDone)
	}

	backupMutex, err := filemutex.New("/tmp/pbs-plus-mutex-lock")
	if err != nil {
		errCleanUp()
		return nil, fmt.Errorf("%w: %v", ErrBackupMutexCreation, err)
	}
	defer backupMutex.Close()

	if err := backupMutex.Lock(); err != nil {
		errCleanUp()
		return nil, fmt.Errorf("%w: %v", ErrBackupMutexLock, err)
	}

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

	if !skipCheck {
		targetSplit := strings.Split(target.Name, " - ")
		_, exists := storeInstance.ARPCSessionManager.GetSession(targetSplit[0])
		if !exists {
			errCleanUp()
			return nil, fmt.Errorf("%w: %s", ErrTargetUnreachable, job.Target)
		}
	}

	srcPath := target.Path
	isAgent := strings.HasPrefix(target.Path, "agent://")
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

	cmd, err := prepareBackupCommand(ctx, job, storeInstance, srcPath, isAgent)
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

	stdoutWriter := io.MultiWriter(clientLogFile, os.Stdout)
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

	go monitorPBSClientLogs(clientLogPath, cmd, errorMonitorDone)

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

	if err := updateJobStatus(false, job, task, storeInstance); err != nil {
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
		defer jobInstanceMutex.Close()

		if err := cmd.Wait(); err != nil {
			operation.err = err
		}

		utils.ClearIOStats(job.CurrentPID)
		job.CurrentPID = 0

		close(errorMonitorDone)

		_ = clientLogFile.Close()

		succeeded, cancelled, err := processPBSProxyLogs(task.UPID, clientLogPath)
		if err != nil {
			syslog.L.Error(err).
				WithMessage("failed to process logs").
				Write()
		}
		_ = os.Remove(clientLogPath)

		if err := updateJobStatus(succeeded, job, task, storeInstance); err != nil {
			syslog.L.Error(err).
				WithMessage("failed to update job status - post cmd.Wait").
				Write()
		}

		if succeeded || cancelled {
			system.RemoveAllRetrySchedules(job)
		} else {
			if err := system.SetRetrySchedule(job); err != nil {
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
