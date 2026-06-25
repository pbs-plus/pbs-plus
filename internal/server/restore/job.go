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
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

type restoreJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	logger       *log.Logger
	task         *RestoreTask
	queueTask    *tasklog.QueuedTask
	waitGroup    *sync.WaitGroup
	err          error
	errChClosed  atomic.Bool
	errCh        chan error
	errCount     atomic.Int32
	receivedDone atomic.Bool

	job           database.Restore
	remoteServer  *pxar.RemoteServer
	localClient   *pxar.Client
	agentPipe     *arpc.StreamPipe
	storeInstance *store.Store
	skipCheck     bool
	web           bool
}

func NewRestoreJob(
	job database.Restore,
	storeInstance *store.Store,
	skipCheck bool,
	web bool,
) (*jobs.Job, error) {
	task, err := GetRestoreTask(job)
	if err != nil {
		return nil, err
	}

	j := &restoreJob{
		job:           job,
		storeInstance: storeInstance,
		skipCheck:     skipCheck,
		web:           web,
		waitGroup:     &sync.WaitGroup{},
		task:          task,
		logger:        log.WithScope(log.Scope{JobID: job.ID}),
	}

	return &jobs.Job{
		ID:        job.ID,
		PreExec:   j.preExecute,
		Execute:   j.execute,
		OnSuccess: j.onSuccess,
		OnError:   j.onError,
		Cleanup:   j.cleanup,
	}, nil
}

func (b *restoreJob) execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	b.updateRestoreWithTask(b.task.Task)
	b.logger.Info("restore starting", "target", b.job.DestTarget.Name, "snapshot", b.job.Snapshot, "store", b.job.Store)

	switch b.job.DestTarget.Type {
	case database.TargetTypeAgent:
		return b.agentExecute(ctx)
	case database.TargetTypeLocal:
		return b.localExecute(ctx)
	case database.TargetTypeS3:
		return fmt.Errorf("S3 restores are unsupported for now (%s)", b.job.DestTarget.Path)
	default:
		return jobs.ErrTargetNotFound
	}
}

func (b *restoreJob) preExecute(ctx context.Context) error {
	wid := tasklog.FormatWorkerID(b.job.Store, "host-", b.job.DestTarget.GetHostname())
	queueTask, err := tasklog.WriteQueuedLog("pbsplusgen-queue", "reader", wid, b.web)
	if err != nil {
		b.logger.Error(err, "failed to create queue task, not fatal")
	} else {
		if err := updateRestoreStatus(false, 0, b.job, queueTask.Task, b.storeInstance); err != nil {
			b.logger.Error(err, "failed to set queue task, not fatal")
		}
	}
	b.queueTask = queueTask

	if err := b.runPreScript(ctx); err != nil {
		return err
	}

	return nil
}

func (b *restoreJob) onError(err error) {
	b.logger.Error(err, "restore job failed")

	if errors.Is(err, jobs.ErrOneInstance) {
		return
	}

	if errors.Is(err, jobs.ErrMountEmpty) {
		b.createOK(err)
		return
	}

	b.task.WriteString("Restore job summary:")
	b.writeStatsSummary()
	b.task.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
	b.task.CloseErr(err)

	if err := updateRestoreStatus(false, 0, b.job, b.task.Task, b.storeInstance); err != nil {
		b.logger.Error(err, "failed to update restore status on error")
	}

	if b.storeInstance.BatchTracker != nil {
		b.storeInstance.BatchTracker.RecordJobResult(
			b.job.NotificationMode,
			notification.JobTypeRestore,
			b.job.ID,
			b.job.Store,
			fmt.Errorf("restore failed: %w", err),
			map[string]string{
				"snapshot":  b.job.Snapshot,
				"namespace": b.job.Namespace,
				"target":    b.job.DestTarget.Name,
				"succeeded": "false",
			},
		)
	}
}

func (b *restoreJob) onSuccess() {
	b.task.WriteString("Restore job summary:")
	b.writeStatsSummary()
	b.task.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))

	errCount := b.errCount.Load()
	if errCount > 0 {
		b.task.CloseWarn(int(errCount))
		if err := updateRestoreStatus(true, int(errCount), b.job, b.task.Task, b.storeInstance); err != nil {
			b.logger.Error(err, "failed to update restore status with warnings")
		}
	} else {
		b.task.CloseOK()
		b.logger.Info("restore completed successfully")
		if err := updateRestoreStatus(true, 0, b.job, b.task.Task, b.storeInstance); err != nil {
			b.logger.Error(err, "failed to update restore status on success")
		}
	}

	var notifyErr error
	if errCount > 0 {
		notifyErr = fmt.Errorf("restore completed with %d errors", errCount)
	}
	if b.storeInstance.BatchTracker != nil {
		b.storeInstance.BatchTracker.RecordJobResult(
			b.job.NotificationMode,
			notification.JobTypeRestore,
			b.job.ID,
			b.job.Store,
			notifyErr,
			map[string]string{
				"snapshot":  b.job.Snapshot,
				"namespace": b.job.Namespace,
				"target":    b.job.DestTarget.Name,
				"succeeded": fmt.Sprintf("%v", errCount == 0),
				"errors":    fmt.Sprintf("%d", errCount),
			},
		)
	}
}

func (b *restoreJob) cleanup() {
	if b.queueTask != nil {
		b.queueTask.Close()
	}

	childKey := b.job.GetStreamID()

	agentRPC, ok := b.storeInstance.ARPCAgentsManager.GetStreamPipe(childKey)
	if ok {
		agentRPC.Close()
	}

	if b.localClient != nil {
		if err := b.localClient.Close(); err != nil {
			b.logger.Error(err, "failed to close local client")
		}
	}

	if b.remoteServer != nil {
		if err := b.remoteServer.Close(); err != nil {
			b.logger.Error(err, "failed to close remote server")
		}
	}

	if b.errCh != nil {
		if !b.errChClosed.Swap(true) {
			close(b.errCh)
		}
	}

	sessions.DisconnectSession(childKey)
}

func (b *restoreJob) writeStatsSummary() {
	r := sessions.GetSessionPxarReader(b.job.GetStreamID())
	if r == nil {
		return
	}
	s := r.GetStats()

	b.task.WriteString(fmt.Sprintf(" - %d total files", s.FilesAccessed))
	b.task.WriteString(fmt.Sprintf(" - %d total folders", s.FoldersAccessed))
	b.task.WriteString(fmt.Sprintf("Restored total: %s", formatBytes(int64(s.TotalBytes))))
	b.task.WriteString(fmt.Sprintf("Duration: %s", formatDuration(r.Elapsed())))
	if s.ByteReadSpeed > 0 {
		b.task.WriteString(fmt.Sprintf("Read speed: %s", formatSpeed(s.ByteReadSpeed)))
	}
	if s.FileAccessSpeed > 0 {
		b.task.WriteString(fmt.Sprintf("Entry processing rate: %.0f entries/s", s.FileAccessSpeed))
	}
}

func (b *restoreJob) runPreScript(ctx context.Context) error {
	if strings.TrimSpace(b.job.PreScript) == "" {
		return nil
	}

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	if err := b.queueTask.UpdateDescription("running pre-restore script"); err != nil {
		b.logger.Error(err, "failed to update queue task description")
	}
	b.task.WriteString(fmt.Sprintf("running pre-restore script %s", b.job.PreScript))

	envVars, err := jobs.StructToEnvVars(b.job)
	if err != nil {
		envVars = []string{}
	}

	scriptOut, _, err := jobs.RunShellScript(ctx, b.job.PreScript, envVars)
	b.logger.Info(scriptOut, "script", b.job.PreScript)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			b.logger.Info("pre-restore script canceled")
			return jobs.ErrCanceled
		}
		b.task.WriteString(err.Error())
		b.task.WriteString(fmt.Sprintf("encountered error while running %s", b.job.PreScript))
		b.errCount.Add(1)
		b.logger.Error(err,

			"error encountered while running job pre-restore script")

		return err
	}

	b.task.WriteString(scriptOut)

	return nil
}

func (b *restoreJob) agentExecute(ctx context.Context) error {
	preCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", b.job.DestTarget.Name))

	qSess, qExists := b.storeInstance.ARPCAgentsManager.GetQuicPipe(b.job.DestTarget.GetHostname())
	tSess, tExists := b.storeInstance.ARPCAgentsManager.GetStreamPipe(b.job.DestTarget.GetHostname())
	if !qExists && !tExists {
		return fmt.Errorf("%w: %s", jobs.ErrTargetUnreachable, b.job.DestTarget.Name)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var respMsg string
	var statusErr error
	if qExists {
		respMsg, statusErr = qSess.CallMessage(
			timeoutCtx,
			"target_status",
			&types.TargetStatusReq{Drive: b.job.DestTarget.VolumeID},
		)
	} else {
		respMsg, statusErr = tSess.CallMessage(
			timeoutCtx,
			"target_status",
			&types.TargetStatusReq{Drive: b.job.DestTarget.VolumeID},
		)
	}
	if statusErr != nil || !strings.HasPrefix(respMsg, "reachable") {
		return fmt.Errorf("%w: %s", jobs.ErrTargetUnreachable, b.job.DestTarget.Name)
	}

	destPath := b.job.DestSubpath
	basePath := b.job.DestTarget.GetAgentHostPath()
	fullPath := path.Join(basePath, destPath)

	if b.job.DestTarget.AgentHost.OperatingSystem == "windows" {
		fullPath = strings.ReplaceAll(fullPath, "/", "\\")
		if len(fullPath) >= 2 && fullPath[1] == ':' {
			drive := strings.ToUpper(fullPath[:2])
			remaining := fullPath[2:]
			remaining = regexp.MustCompile(`\+`).ReplaceAllString(remaining, "\\")
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

	restoreReq := types.RestoreReq{
		RestoreID: b.job.ID,
		SrcPath:   srcPath,
		DestPath:  destPath,
		Mode:      b.job.Mode,
	}

	b.storeInstance.ARPCAgentsManager.Expect(b.job.GetStreamID())
	defer b.storeInstance.ARPCAgentsManager.NotExpect(b.job.GetStreamID())

	b.task.WriteString(fmt.Sprintf("calling restore to %s (%s)", b.job.DestTarget.Name, destPath))

	var restoreErr error
	if qExists {
		_, restoreErr = qSess.CallMessage(preCtx, "restore", &restoreReq)
	} else {
		_, restoreErr = tSess.CallMessage(preCtx, "restore", &restoreReq)
	}
	if restoreErr != nil {
		return restoreErr
	}

	childKey := b.job.GetStreamID()

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", childKey))

	pipeCtx, pipeCtxCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pipeCtxCancel()

	agentRPC, err := b.storeInstance.ARPCAgentsManager.WaitStreamPipe(pipeCtx, childKey)
	if err != nil {
		return err
	}

	b.agentPipe = agentRPC

	socketPath := filepath.Join(
		conf.RestoreSocketPath,
		strings.ReplaceAll(childKey, "|", "-")+".sock",
	)

	b.task.WriteString(fmt.Sprintf(
		"running pxar reader [datastore: %s, namespace: %s, snapshot: %s]",
		b.job.Store, b.job.Namespace, b.job.Snapshot,
	))

	reader, err := pxar.NewPxarReader(
		ctx, socketPath, b.job.Store, b.job.Namespace, b.job.Snapshot, b.task,
	)
	if err != nil {
		return err
	}

	b.task.WriteString(fmt.Sprintf(
		"running remote pxar reader [datastore: %s, namespace: %s, snapshot: %s]",
		b.job.Store, b.job.Namespace, b.job.Snapshot,
	))

	b.remoteServer, b.errCh = pxar.NewRemoteServer(reader)
	if b.remoteServer == nil {
		return fmt.Errorf("b.remoteServer is nil")
	}

	go func() {
		defer b.waitGroup.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-b.errCh:
				if !ok {
					return
				}
				if err != nil {
					b.task.WriteString(fmt.Sprintf("%s", err))
					b.logger.Error(err, "", "restore", "agent-error")
					b.errCount.Add(1)
				}
			}
		}
	}()
	b.waitGroup.Add(1)

	agentRPC.SetRouter(*b.remoteServer.Router())
	sessions.NewPxarReader(childKey, reader)
	b.logger.Info("restore request sent to agent")

	b.task.WriteString(fmt.Sprintf("sending ready signal to stream pipe of %s", childKey))

	_, err = agentRPC.CallMessage(preCtx, "server_ready", &restoreReq)
	if err != nil {
		return err
	}

	return b.waitForCompletion(ctx)
}

func (b *restoreJob) localExecute(ctx context.Context) error {
	destPath := filepath.Join(b.job.DestTarget.Path, b.job.DestSubpath)

	srcPath := b.job.SrcPath
	if strings.TrimSpace(b.job.SrcPath) == "" {
		srcPath = "/"
	}

	childKey := b.job.GetStreamID()
	socketPath := filepath.Join(
		conf.RestoreSocketPath,
		strings.ReplaceAll(childKey, "|", "-")+".sock",
	)

	b.task.WriteString(fmt.Sprintf(
		"running pxar reader [datastore: %s, namespace: %s, snapshot: %s]",
		b.job.Store, b.job.Namespace, b.job.Snapshot,
	))

	reader, err := pxar.NewPxarReader(
		ctx, socketPath, b.job.Store, b.job.Namespace, b.job.Snapshot, b.task,
	)
	if err != nil {
		return err
	}

	b.localClient, b.errCh = pxar.NewLocalClient(reader, b.job.ID)
	b.logger.Info("restore request sent to agent")

	b.task.WriteString("starting local restore")

	b.waitGroup.Go(func() {
		if err := pxar.RestoreWithOptions(ctx, b.localClient, []string{srcPath}, pxar.RestoreOptions{
			DestDir: destPath,
			Mode:    pxar.RestoreMode(b.job.Mode),
		}); err != nil && b.err == nil {
			b.err = err
		}
	})

	b.waitGroup.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-b.errCh:
				if !ok {
					return
				}
				if err != nil {
					b.task.WriteString(fmt.Sprintf("client error: %s", err.Error()))
					b.logger.Error(err, "", "restore", "local-error")
					b.errCount.Add(1)
				}
			}
		}
	})

	sessions.NewPxarReader(childKey, reader)

	return b.waitForCompletion(ctx)
}

func (b *restoreJob) waitForCompletion(ctx context.Context) error {
	if b.remoteServer != nil {
		var pipeCloseCh <-chan struct{}
		if b.agentPipe != nil {
			pipeCloseCh = b.agentPipe.CloseChan()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.remoteServer.DoneCh:
			b.receivedDone.Store(true)
			b.task.WriteString("received done signal from agent")
		case <-pipeCloseCh:
			// The agent's client.Close() sends pxar.Done as a synchronous RPC
			// (server closes DoneCh and ACKs) and only closes the pipe after
			// the ACK. So DoneCh is closed strictly before the pipe closes.
			// and Go's select picks one nondeterministically  -  which used to
			// that was already received always wins; only treat the close as
			select {
			case <-b.remoteServer.DoneCh:
				b.receivedDone.Store(true)
				b.task.WriteString("received done signal from agent")
			default:
				if !b.receivedDone.Load() {
					b.task.WriteString("agent disconnected")
					b.err = fmt.Errorf("lost connection to agent without receiving done signal")
				}
			}
		}
	}

	// Close errCh to unblock the error-collecting goroutine.
	// for local restores the restore goroutine may still be in
	// progress, but waitGroup.Wait() ensures both goroutines finish.
	if b.errCh != nil {
		if !b.errChClosed.Swap(true) {
			close(b.errCh)
		}
	}

	if b.waitGroup != nil {
		b.waitGroup.Wait()
	}

	if ctx.Err() == nil {
		b.runPostScript()
	}

	if b.err != nil {
		return b.err
	}

	return ctx.Err()
}

func (b *restoreJob) runPostScript() {
	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	if job.PostScript == "" {
		return
	}

	if b.queueTask != nil {
		if err := b.queueTask.UpdateDescription("running post-restore script"); err != nil {
			b.logger.Error(err, "failed to update queue task description")
		}
	}

	b.task.WriteString(fmt.Sprintf("running post-restore script %s", b.job.PostScript))
	b.logger.Info("running post-restore script",
		"script", job.PostScript)

	envVars, err := jobs.StructToEnvVars(job)
	if err != nil {
		envVars = []string{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	scriptOut, _, err := jobs.RunShellScript(ctx, job.PostScript, envVars)
	if err != nil {
		b.task.WriteString(err.Error())
		b.task.WriteString(fmt.Sprintf("encountered error while running %s", b.job.PostScript))
		b.errCount.Add(1)
		b.logger.Error(err,
			"error encountered while running job post-restore script")

	}

	b.task.WriteString(scriptOut)
	b.logger.Info(scriptOut,
		"script", b.job.PostScript)

}

func (b *restoreJob) createOK(err error) {
	task, terr := GenerateRestoreTaskOKFile(
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
		b.logger.Error(terr, "failed to generate restore OK task file")
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
		b.logger.Error(uerr, "failed to update restore with task", "upid", task.UPID)

	}
}

func (b *restoreJob) updateRestoreWithTask(task proxmox.Task) {
	latest, gerr := b.storeInstance.Database.GetRestore(b.job.ID)
	if gerr != nil {
		latest = b.job
	}

	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunEndtime = task.EndTime

	if uerr := b.storeInstance.Database.UpdateRestore(nil, latest); uerr != nil {
		b.logger.Error(uerr, "failed to update restore with task", "upid", task.UPID)

	}
}
