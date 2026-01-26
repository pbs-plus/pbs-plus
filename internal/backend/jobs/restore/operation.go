//go:build linux

package restore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	agenttypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/tasks"
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

	task      *tasks.RestoreTask
	queueTask *tasks.QueuedTask
	waitGroup *sync.WaitGroup
	err       error
	errCh     chan error
	errCount  atomic.Int32

	job           database.Restore
	remoteServer  *pxar.RemoteServer
	localClient   *pxar.Client
	storeInstance *store.Store
	skipCheck     bool
	web           bool
}

var _ jobs.Operation = (*RestoreOperation)(nil)

func NewRestoreOperation(
	job database.Restore,
	storeInstance *store.Store,
	skipCheck bool,
	web bool,
) (*RestoreOperation, error) {
	task, err := tasks.GetRestoreTask(job)
	if err != nil {
		return nil, err
	}

	return &RestoreOperation{
		job:           job,
		storeInstance: storeInstance,
		skipCheck:     skipCheck,
		web:           web,
		waitGroup:     &sync.WaitGroup{},
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
		b.errCount.Add(1)

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
		b.errCount.Add(1)
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
	queueTask, err := tasks.GenerateRestoreQueuedTask(b.job, b.web)
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

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", b.job.DestTarget.Name))

	arpcSess, exists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(b.job.DestTarget.GetHostname())
	if !exists {
		return fmt.Errorf("%w: %s", ErrTargetUnreachable, b.job.DestTarget.Name)
	}

	timeoutCtx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()

	respMsg, err := arpcSess.CallMessage(
		timeoutCtx,
		"target_status",
		&types.TargetStatusReq{Drive: b.job.DestTarget.VolumeID},
	)
	if err != nil || !strings.HasPrefix(respMsg, "reachable") {
		return fmt.Errorf("%w: %s", ErrTargetUnreachable, b.job.DestTarget.Name)
	}

	destPath := b.job.DestSubpath
	basePath := b.job.DestTarget.GetAgentHostPath()

	fullPath := path.Join(basePath, destPath)

	if b.job.DestTarget.AgentHost.OperatingSystem == "windows" {
		fullPath = strings.ReplaceAll(fullPath, "/", "\\")
		if len(fullPath) >= 2 && fullPath[1] == ':' {
			drive := strings.ToUpper(fullPath[:2])
			remaining := fullPath[2:]
			remaining = regexp.MustCompile(`\\+`).ReplaceAllString(remaining, "\\")
			if !strings.HasPrefix(remaining, "\\") {
				remaining = "\\" + remaining
			}
			fullPath = drive + remaining
		}
		destPath = fullPath
	} else {
		destPath = fullPath
	}

	srcPath := b.job.SrcPath
	if strings.TrimSpace(b.job.SrcPath) == "" {
		srcPath = "/"
	}

	restoreReq := agenttypes.RestoreReq{
		RestoreId: b.job.ID,
		SrcPath:   srcPath,
		DestPath:  destPath,
		Mode:      b.job.Mode,
	}

	b.storeInstance.ARPCAgentsManager.Expect(b.job.GetStreamID())
	defer b.storeInstance.ARPCAgentsManager.NotExpect(b.job.GetStreamID())

	b.task.WriteString(fmt.Sprintf("calling restore to %s (%s)", b.job.DestTarget.Name, destPath))

	_, err = arpcSess.CallMessage(preCtx, "restore", &restoreReq)
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

	b.remoteServer, b.errCh = pxar.NewRemoteServer(reader)
	if b.remoteServer == nil {
		return fmt.Errorf("b.remoteServer is nil")
	}

	b.waitGroup.Go(func() {
		for {
			select {
			case <-b.ctx.Done():
				return
			case err, ok := <-b.errCh:
				if !ok {
					return
				}
				if err != nil {
					b.task.WriteString(fmt.Sprintf("%s", err))
					b.errCount.Add(1)
				}
			}
		}
	})

	agentRPC.SetRouter(*b.remoteServer.Router())

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

	return nil
}

func (b *RestoreOperation) localExecute() error {
	destPath := b.job.DestSubpath

	destPath = filepath.Join(b.job.DestTarget.Path, destPath)

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

	b.localClient, b.errCh = pxar.NewLocalClient(reader, b.job.ID)

	syslog.L.Info().
		WithMessage("Restore request sent").
		WithFields(map[string]any{
			"restoreId": b.job.ID,
		}).Write()

	b.task.WriteString("starting local restore")

	b.waitGroup.Go(func() {
		pxar.RestoreWithOptions(b.ctx, b.localClient, []string{srcPath}, pxar.RestoreOptions{
			DestDir: destPath,
			Mode:    pxar.RestoreMode(b.job.Mode),
		})
	})

	b.waitGroup.Go(func() {
		for {
			select {
			case <-b.ctx.Done():
				return
			case err, ok := <-b.errCh:
				if !ok {
					return
				}
				if err != nil {
					b.task.WriteString(fmt.Sprintf("client error: %s", err.Error()))
					b.errCount.Add(1)
				}
			}
		}
	})

	vfssessions.CreatePxarReader(childKey, reader)

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

	switch b.job.DestTarget.Type {
	case database.TargetTypeAgent:
		return b.agentExecute()
	case database.TargetTypeLocal:
		return b.localExecute()
	case database.TargetTypeS3:
		return fmt.Errorf("S3 restores are unsupported for now (%s)", b.job.DestTarget.Path)
	default:
		return ErrTargetNotFound
	}
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
	errCount := b.errCount.Load()
	if errCount > 0 {
		b.task.CloseWarn(int(errCount))
		return
	}

	b.task.CloseOK()
}

func (b *RestoreOperation) Cleanup() {
	if b.queueTask != nil {
		b.queueTask.Close()
	}

	childKey := b.job.GetStreamID()
	agentRPC, ok := b.storeInstance.ARPCAgentsManager.GetStreamPipe(childKey)
	if ok {
		agentRPC.Close()
	}

	if b.localClient != nil {
		b.localClient.Close()
	}

	if b.remoteServer != nil {
		b.remoteServer.Close()
	}

	if b.errCh != nil {
		close(b.errCh)
	}

	b.task.WriteString(fmt.Sprintf("disconnecting stream pipe session of %s", childKey))
	vfssessions.DisconnectSession(childKey)
}

func (b *RestoreOperation) Wait() error {
	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}

	if b.remoteServer != nil {
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		case <-b.remoteServer.DoneCh:
			b.task.WriteString("received done signal from agent")
		}
	}

	b.runPostScript()

	return b.err
}

func (b *RestoreOperation) createOK(err error) {
	task, terr := tasks.GenerateRestoreTaskOKFile(
		b.job,
		[]string{
			"Done handling from a job run request",
			"Restore ID: " + b.job.ID,
			"Snapshot: " + b.job.Snapshot,
			"Store: " + b.job.Store,
			"Destination: " + b.job.DestTarget.Name,
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
	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunEndtime = task.EndTime
	latest.History.LastSuccessfulEndtime = task.EndTime
	latest.History.LastSuccessfulUpid = task.UPID

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
	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunEndtime = task.EndTime

	if uerr := b.storeInstance.Database.UpdateRestore(nil, latest); uerr != nil {
		syslog.L.Error(uerr).
			WithField("jobId", latest.ID).
			WithField("upid", task.UPID).
			Write()
	}
}
