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
	"github.com/pbs-plus/pbs-plus/internal/utils"
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
	mu     sync.RWMutex

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

func (b *RestoreOperation) runPreScript() error {
	if strings.TrimSpace(b.job.PreScript) == "" {
		return nil
	}

	select {
	case <-b.Context().Done():
		return jobs.ErrCanceled
	default:
	}

	b.queueTask.UpdateDescription("running pre-restore script")
	b.task.WriteString(fmt.Sprintf("running pre-restore script %s", b.job.PreScript))

	envVars, err := utils.StructToEnvVars(b.job)

	if err != nil {
		envVars = []string{}
	}

	scriptOut, _, err := utils.RunShellScript(b.Context(), b.job.PreScript, envVars)
	syslog.L.Info().WithJob(b.job.ID).WithMessage(scriptOut).WithField("script", b.job.PreScript).Write()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			syslog.L.Info().WithJob(b.job.ID).WithMessage("pre-restore script canceled").Write()
			return jobs.ErrCanceled
		}
		b.task.WriteString(err.Error())
		b.task.WriteString(fmt.Sprintf("encountered error while running %s", b.job.PreScript))

		syslog.L.Error(err).WithJob(b.job.ID).WithMessage("error encountered while running job pre-restore script").Write()
		return err
	}

	b.task.WriteString(scriptOut)

	return nil
}

func (b *RestoreOperation) runPostScript() {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	if job.PostScript == "" {
		return
	}

	if b.queueTask != nil {
		b.queueTask.UpdateDescription("running post-restore script")
	}

	b.task.WriteString(fmt.Sprintf("running post-restore script %s", b.job.PostScript))
	syslog.L.Info().
		WithMessage("running post-restore script").
		WithField("script", job.PostScript).
		WithJob(job.ID).
		Write()

	envVars, err := utils.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, _, err := utils.RunShellScript(b.Context(), job.PostScript, envVars)
	if err != nil {
		b.task.WriteString(err.Error())
		b.task.WriteString(fmt.Sprintf("encountered error while running %s", b.job.PostScript))
		syslog.L.Error(err).
			WithMessage("error encountered while running job post-restore script").
			WithJob(job.ID).
			Write()
	}

	b.task.WriteString(scriptOut)
	syslog.L.Info().
		WithMessage(scriptOut).
		WithField("script", job.PostScript).
		WithJob(job.ID).
		Write()
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

	if err := b.runPreScript(); err != nil {
		return err
	}

	return nil
}

func (b *RestoreOperation) agentExecute() error {
	preCtx, cancel := context.WithTimeout(b.ctx, 5*time.Minute)
	defer cancel()

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", b.job.DestTarget))

	arpcSess, exists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(b.job.DestTarget.GetHostname())
	if !exists {
		return errors.New("destination target is unreachable")
	}

	destPath := b.job.DestPath

	targetInfo := b.job.DestTargetPath.GetPathInfo()
	destPath = filepath.Join(targetInfo.HostPath, destPath)
	if targetInfo.IsWindows {
		destPath = filepath.FromSlash(destPath)
	}

	srcPath := b.job.SrcPath
	if strings.TrimSpace(b.job.SrcPath) == "" {
		srcPath = "/"
	}

	restoreReq := agenttypes.RestoreReq{
		RestoreId: b.job.ID,
		SrcPath:   srcPath,
		DestPath:  destPath,
	}

	b.task.WriteString(fmt.Sprintf("calling restore to %s (%s)", b.job.DestTarget, destPath))

	_, err := arpcSess.CallMessage(preCtx, "restore", &restoreReq)
	if err != nil {
		return err
	}

	// The child session key is "targetHostname|restoreId|restore".
	childKey := b.job.GetStreamID()

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", childKey))

	pipeCtx, pipeCtxCancel := context.WithTimeout(b.ctx, time.Second*10)
	defer pipeCtxCancel()

	agentRPC, err := b.storeInstance.ARPCAgentsManager.WaitStreamPipe(pipeCtx, childKey)
	if err != nil {
		return err
	}

	socketPath := filepath.Join(constants.RestoreSocketPath, strings.ReplaceAll(childKey, "|", "-")+".sock")

	b.task.WriteString(fmt.Sprintf("running pxar reader [datastore: %s, namespace: %s, snapshot: %s]", b.job.Store, b.job.Namespace, b.job.Snapshot))
	reader, err := pxar.NewPxarReader(b.ctx, socketPath, b.job.Store, b.job.Namespace, b.job.Snapshot, b.task)
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

func (b *RestoreOperation) localExecute() error {
	destPath := b.job.DestPath

	targetInfo := b.job.DestTargetPath.GetPathInfo()
	destPath = filepath.Join(targetInfo.HostPath, destPath)

	srcPath := b.job.SrcPath
	if strings.TrimSpace(b.job.SrcPath) == "" {
		srcPath = "/"
	}

	childKey := b.job.GetStreamID()
	socketPath := filepath.Join(constants.RestoreSocketPath, strings.ReplaceAll(childKey, "|", "-")+".sock")

	b.task.WriteString(fmt.Sprintf("running pxar reader [datastore: %s, namespace: %s, snapshot: %s]", b.job.Store, b.job.Namespace, b.job.Snapshot))
	reader, err := pxar.NewPxarReader(b.ctx, socketPath, b.job.Store, b.job.Namespace, b.job.Snapshot, b.task)
	if err != nil {
		return err
	}

	vfssessions.CreatePxarReader(childKey, reader)

	syslog.L.Info().
		WithMessage("Restore request sent").
		WithFields(map[string]any{
			"restoreId": b.job.ID,
		}).Write()

	b.task.WriteString("starting local restore")

	results := make(chan []error, 1)
	defer close(results)

	go pxar.LocalRestore(b.ctx, reader, []string{srcPath}, destPath, results)

	defer func() {
		b.task.WriteString(fmt.Sprintf("ending session of %s", childKey))
		vfssessions.DisconnectSession(childKey)
	}()

	numErrs := 0
	select {
	case <-b.ctx.Done():
		return b.ctx.Err()
	case errs := <-results:
		for _, err := range errs {
			numErrs++
			b.task.WriteString(fmt.Sprintf("error: %s", err.Error()))
		}
	}

	if numErrs > 0 {
		return fmt.Errorf("local restore has encountered %d errors", numErrs)
	}

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

	info := b.job.DestTargetPath.GetPathInfo()

	switch info.Type {
	case types.TargetTypeAgent:
		return b.agentExecute()
	case types.TargetTypeLocal:
		return b.localExecute()
	case types.TargetTypeS3:
		return fmt.Errorf("S3 restores are unsupported for now (%s)", b.job.DestTargetPath)
	}

	return fmt.Errorf("only agent and local restores are supported for now (%s)", b.job.DestTargetPath)
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

	b.runPostScript()

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
