//go:build linux

package restore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	agenttypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	vfssessions "github.com/pbs-plus/pbs-plus/internal/store/vfs"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var (
	ErrJobMutexCreation = errors.New("failed to create job mutex")
	ErrOneInstance      = errors.New("a job is still running; only one instance allowed")

	ErrStdoutTempCreation = errors.New("failed to create stdout temp file")

	ErrRestoreMutexCreation = errors.New("failed to create restore mutex")
	ErrRestoreMutexLock     = errors.New("failed to lock restore mutex")

	ErrAPITokenRequired = errors.New("API token is required")

	ErrTargetGet         = errors.New("failed to get target")
	ErrTargetNotFound    = errors.New("target does not exist")
	ErrTargetUnreachable = errors.New("target unreachable")

	ErrPrepareRestoreCommand = errors.New("failed to prepare restore command")

	ErrTaskMonitoringInitializationFailed = errors.New("task monitoring initialization failed")
	ErrTaskMonitoringTimedOut             = errors.New("task monitoring initialization timed out")

	ErrProxmoxRestoreClientStart = errors.New("proxmox-restore-client start error")

	ErrNilTask               = errors.New("received nil task")
	ErrTaskDetectionFailed   = errors.New("task detection failed")
	ErrTaskDetectionTimedOut = errors.New("task detection timed out")
	ErrMountEmpty            = errors.New("target directory is empty, skipping restore")

	ErrJobStatusUpdateFailed = errors.New("failed to update job status")
	ErrCanceled              = errors.New("operation canceled")
)

type RestoreOperation struct {
	ctx    context.Context
	cancel context.CancelFunc

	task      *proxmox.RestoreTask
	queueTask *proxmox.QueuedTask
	waitGroup *sync.WaitGroup
	err       error

	job           types.Restore
	storeInstance *store.Store
	skipCheck     bool
	web           bool
}

var _ jobs.Operation = (*RestoreOperation)(nil)

func NewRestoreOperation(
	job types.Restore,
	storeInstance *store.Store,
	skipCheck bool,
	web bool,
) (*RestoreOperation, error) {
	task, err := proxmox.GetRestoreTask(job)
	if err != nil {
		return nil, err
	}

	return &RestoreOperation{
		job:           job,
		storeInstance: storeInstance,
		skipCheck:     skipCheck,
		web:           web,
		task:          task,
	}, nil
}

func (b *RestoreOperation) GetID() string {
	return b.job.ID
}

func (b *RestoreOperation) SetContext(ctx context.Context, cancel context.CancelFunc) {
	b.ctx = ctx
	b.cancel = cancel
}

func (b *RestoreOperation) Context() context.Context {
	return b.ctx
}

func (b *RestoreOperation) PreExecute() error {
	queueTask, err := proxmox.GenerateRestoreQueuedTask(b.job, b.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		if err := updateRestoreStatus(false, 0, b.job, queueTask.Task, b.storeInstance); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}
	b.queueTask = &queueTask

	return nil
}

func (b *RestoreOperation) Execute() error {
	b.updateRestoreWithTask(b.task.Task)

	syslog.L.Info().
		WithMessage("Received restore request").
		WithFields(map[string]any{
			"restoreId": b.job.ID,
			"target":    b.job.DestTarget,
		}).Write()

	preCtx, cancel := context.WithTimeout(b.ctx, 5*time.Minute)
	defer cancel()

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", b.job.DestTarget))

	arpcSess, exists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(b.job.DestTarget.GetHostname())
	if !exists {
		return errors.New("destination target is unreachable")
	}

	restoreReq := agenttypes.RestoreReq{
		RestoreId: b.job.ID,
		SrcPath:   b.job.SrcPath,
		DestPath:  b.job.DestPath,
	}

	b.task.WriteString(fmt.Sprintf("calling restore to %s", b.job.DestTarget))

	_, err := arpcSess.CallMessage(preCtx, "restore", &restoreReq)
	if err != nil {
		return err
	}

	// The child session key is "targetHostname|restoreId|restore".
	childKey := b.job.GetStreamID()

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", childKey))

	agentRPC, exists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(childKey)
	if !exists {
		return errors.New("child destination target is unreachable")
	}

	socketPath := filepath.Join(constants.RestoreSocketPath, strings.ReplaceAll(childKey, "|", "-")+".sock")

	b.task.WriteString(fmt.Sprintf("running pxar reader [datastore: %s, namespace: %s, snapshot: %s]", b.job.Store, b.job.Namespace, b.job.Snapshot))
	reader, err := pxar.NewPxarReader(socketPath, b.job.Store, b.job.Namespace, b.job.Snapshot, b.task)
	if err != nil {
		return err
	}

	b.task.WriteString(fmt.Sprintf("running remote pxar reader [datastore: %s, namespace: %s, snapshot: %s]", b.job.Store, b.job.Namespace, b.job.Snapshot))
	srv := pxar.NewRemoteServer(reader)
	agentRPC.SetRouter(*srv.Router())

	vfssessions.CreatePxarReader(childKey, reader)

	syslog.L.Info().
		WithMessage("Restore request sent").
		WithFields(map[string]any{
			"restoreId": b.job.ID,
		}).Write()

	b.task.WriteString(fmt.Sprintf("sending ready signal to stream pipe of %s", childKey))
	_, err = agentRPC.CallMessage(preCtx, "server_ready", &restoreReq)
	if err != nil {
		return err
	}

	defer func() {
		b.task.WriteString(fmt.Sprintf("disconnecting stream pipe session of %s", childKey))
		vfssessions.DisconnectSession(childKey)
		agentRPC.Close()
	}()

	select {
	case <-b.ctx.Done():
		return b.ctx.Err()
	case <-srv.DoneCh:
		b.task.WriteString("received done signal from agent")
	}

	return nil
}

func (b *RestoreOperation) OnError(err error) {
	syslog.L.Error(err).WithField("jobId", b.job.ID).Write()

	if errors.Is(err, jobs.ErrOneInstance) {
		return
	}

	if errors.Is(err, ErrMountEmpty) {
		b.createOK(err)
		return
	}

	b.task.CloseErr(err)
}

func (b *RestoreOperation) OnSuccess() {
	b.task.CloseOK()
}

func (b *RestoreOperation) Cleanup() {
	if b.queueTask != nil {
		b.queueTask.Close()
	}
}

func (b *RestoreOperation) Wait() error {
	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}
	return b.err
}

func (b *RestoreOperation) createOK(err error) {
	task, terr := proxmox.GenerateRestoreTaskOKFile(
		b.job,
		[]string{
			"Done handling from a job run request",
			"Restore ID: " + b.job.ID,
			"Snapshot: " + b.job.Snapshot,
			"Store: " + b.job.Store,
			"Destination: " + b.job.DestTarget.String(),
			"Response: " + err.Error(),
		},
	)
	if terr != nil {
		syslog.L.Error(terr).WithField("jobId", b.job.ID).Write()
		return
	}

	latest, gerr := b.storeInstance.Database.GetRestore(b.job.ID)
	if gerr != nil {
		latest = b.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime
	latest.LastSuccessfulEndtime = task.EndTime
	latest.LastSuccessfulUpid = task.UPID

	if uerr := b.storeInstance.Database.UpdateRestore(nil, latest); uerr != nil {
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}

func (b *RestoreOperation) updateRestoreWithTask(task proxmox.Task) {
	latest, gerr := b.storeInstance.Database.GetRestore(b.job.ID)
	if gerr != nil {
		latest = b.job
	}
	latest.LastRunUpid = task.UPID
	latest.LastRunState = task.Status
	latest.LastRunEndtime = task.EndTime

	if uerr := b.storeInstance.Database.UpdateRestore(nil, latest); uerr != nil {
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}
