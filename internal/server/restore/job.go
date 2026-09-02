//go:build linux

package restore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

type restoreJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	logger       *log.Logger
	task         *RestoreTask
	upid         string
	waitGroup    *sync.WaitGroup
	err          error
	errChClosed  atomic.Bool
	errCh        chan error
	errCount     atomic.Int32
	receivedDone atomic.Bool

	job           coredb.Restore
	executionID   string
	remoteServer  *pxar.RemoteServer
	localClient   *pxar.Client
	agentPipe     *arpc.StreamPipe
	app           *application.Runtime
	skipCheck     bool
	databaseAware bool
	stagingDir    string
}

func (b *restoreJob) execute(ctx context.Context, idempotencyKey string) error {
	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	b.updateRestoreWithTask(b.task.Task)
	b.logger.Info("restore starting", "target", b.job.DestTarget.Name, "snapshot", b.job.Snapshot, "store", b.job.Store)

	switch {
	case b.job.DestTarget.IsDatabase() && b.databaseAware:
		return b.databaseExecute(ctx)
	case b.job.DestTarget.IsDovecot() && b.databaseAware:
		return b.dovecotExecute(ctx)
	case b.job.DestTarget.IsAgent():
		return b.agentExecute(ctx, idempotencyKey)
	case b.job.DestTarget.IsLocal():
		return b.localExecute(ctx)
	case b.job.DestTarget.IsS3():
		return fmt.Errorf("S3 restores are unsupported for now (%s)", b.job.DestTarget.Path)
	default:
		return jobs.ErrTargetNotFound
	}
}

func (b *restoreJob) finalizeFailure(err error) {
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

	if err := updateRestoreStatus(false, 0, b.job, b.task.Task, b.executionID, b.app); err != nil {
		b.logger.Error(err, "failed to update restore status on error")
	}

	if b.app.BatchTracker != nil {
		b.app.BatchTracker.RecordJobResult(
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

func (b *restoreJob) finalizeSuccess() {
	b.task.WriteString("Restore job summary:")
	b.writeStatsSummary()
	b.task.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))

	errCount := b.errCount.Load()
	if errCount > 0 {
		b.task.CloseWarn(int(errCount))
		if err := updateRestoreStatus(true, int(errCount), b.job, b.task.Task, b.executionID, b.app); err != nil {
			b.logger.Error(err, "failed to update restore status with warnings")
		}
	} else {
		b.task.CloseOK()
		b.logger.Info("restore completed successfully")
		if err := updateRestoreStatus(true, 0, b.job, b.task.Task, b.executionID, b.app); err != nil {
			b.logger.Error(err, "failed to update restore status on success")
		}
	}

	var notifyErr error
	if errCount > 0 {
		notifyErr = fmt.Errorf("restore completed with %d errors", errCount)
	}
	if b.app.BatchTracker != nil {
		b.app.BatchTracker.RecordJobResult(
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
	childKey := b.job.GetStreamID()

	agentRPC, ok := b.app.Agents.GetStreamPipe(childKey)
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
	if b.stagingDir != "" {
		if err := os.RemoveAll(b.stagingDir); err != nil {
			b.logger.Error(err, "failed to remove restore staging data")
		}
	}
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

func (b *restoreJob) agentExecute(ctx context.Context, idempotencyKey string) error {
	preCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	b.task.WriteString(fmt.Sprintf("getting stream pipe of %s", b.job.DestTarget.Name))

	qSess, qExists := b.app.Agents.GetQuicPipe(b.job.DestTarget.GetHostname())
	tSess, tExists := b.app.Agents.GetStreamPipe(b.job.DestTarget.GetHostname())
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
			&fswire.TargetStatusReq{Drive: b.job.DestTarget.VolumeID},
		)
	} else {
		respMsg, statusErr = tSess.CallMessage(
			timeoutCtx,
			"target_status",
			&fswire.TargetStatusReq{Drive: b.job.DestTarget.VolumeID},
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

	restoreReq := fswire.RestoreReq{
		RestoreID:      b.job.ID,
		SrcPath:        srcPath,
		DestPath:       destPath,
		Mode:           b.job.Mode,
		IdempotencyKey: idempotencyKey,
	}

	b.app.Agents.Expect(b.job.GetStreamID())
	defer b.app.Agents.NotExpect(b.job.GetStreamID())

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

	agentRPC, err := b.app.Agents.WaitStreamPipe(pipeCtx, childKey)
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

	if err := b.startLocalRestore(ctx, destPath, []string{srcPath}, pxar.RestoreMode(b.job.Mode)); err != nil {
		return err
	}
	return b.waitForCompletion(ctx)
}

func (b *restoreJob) startLocalRestore(ctx context.Context, destPath string, sources []string, mode pxar.RestoreMode) error {
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
		if err := pxar.RestoreWithOptions(ctx, b.localClient, sources, pxar.RestoreOptions{
			DestDir: destPath,
			Mode:    mode,
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
	return nil
}

func (b *restoreJob) waitForCompletion(ctx context.Context) error {
	if err := b.waitForTransfer(ctx); err != nil {
		return err
	}
	if ctx.Err() == nil {
		b.runPostScript()
	}
	return ctx.Err()
}

func (b *restoreJob) waitForTransfer(ctx context.Context) error {
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

	latest, gerr := b.app.CoreDB.GetRestore(b.job.ID)
	if gerr != nil {
		latest = b.job
	}

	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunEndtime = task.EndTime
	latest.History.LastSuccessfulEndtime = task.EndTime
	latest.History.LastSuccessfulUpid = task.UPID

	if uerr := b.app.CoreDB.UpdateRestore(nil, latest); uerr != nil {
		b.logger.Error(uerr, "failed to update restore with task", "upid", task.UPID)
		return
	}
	jobs.MarkRunFinalized(b.job.ID, b.executionID)
}

func (b *restoreJob) updateRestoreWithTask(task proxmox.Task) {
	latest, gerr := b.app.CoreDB.GetRestore(b.job.ID)
	if gerr != nil {
		latest = b.job
	}

	latest.History.LastRunUpid = task.UPID
	latest.History.LastRunState = task.Status
	latest.History.LastRunEndtime = task.EndTime

	if uerr := b.app.CoreDB.UpdateRestore(nil, latest); uerr != nil {
		b.logger.Error(uerr, "failed to update restore with task", "upid", task.UPID)
		return
	}
	jobs.MarkRunFinalized(b.job.ID, b.executionID)
}
