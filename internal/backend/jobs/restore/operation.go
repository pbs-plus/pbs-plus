//go:build linux

package restore

import (
	"context"
	"errors"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
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

	Task      proxmox.Task
	queueTask *proxmox.QueuedTask
	waitGroup *sync.WaitGroup
	err       error

	logger *syslog.JobLogger

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
) *RestoreOperation {
	return &RestoreOperation{
		job:           job,
		storeInstance: storeInstance,
		skipCheck:     skipCheck,
		web:           web,
		logger:        syslog.CreateJobLogger(job.ID),
	}
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

func (b *RestoreOperation) PreExecute(ctx context.Context) error {
	// TODO: queue log

	return nil
}

func (b *RestoreOperation) Execute(ctx context.Context) error {
	return b.executeRestore(ctx)
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

	// TODO: error log
}

func (b *RestoreOperation) OnSuccess() {
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

func (b *RestoreOperation) executeRestore(ctx context.Context) error {
	return nil
}

func (b *RestoreOperation) createOK(err error) {
	// TODO: generate ok log
}
