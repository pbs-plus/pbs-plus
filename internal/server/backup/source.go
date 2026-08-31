//go:build linux

package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

type taskLogWriter struct {
	destination io.Writer
	logLine     func(string)
}

func (w taskLogWriter) Write(data []byte) (int, error) {
	text := strings.TrimSuffix(string(data), "\n")
	if text != "" && w.logLine != nil {
		for line := range strings.SplitSeq(text, "\n") {
			w.logLine(line)
		}
	}
	if w.destination == nil {
		return len(data), nil
	}
	written, err := w.destination.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	return written, err
}

func (b *backupJob) logQueuedLine(line string) {
	b.mu.RLock()
	task := b.scriptTask
	b.mu.RUnlock()
	if task != nil {
		task.LogString(line)
	}
}

func (b *backupJob) validateTargetConnection(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	if b.skipCheck {
		return nil
	}

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	switch job.Target.Type {
	case coredb.TargetTypeFilesystem:
		if job.Target.IsLocal() {
			if _, err := os.Stat(job.Target.Path); err != nil {
				return fmt.Errorf("%w: %s (%v)", jobs.ErrTargetUnreachable, job.Target.Name, err)
			}
			break
		}
		qSess, qExists := b.app.Agents.GetQuicPipe(job.Target.GetHostname())
		tSess, tExists := b.app.Agents.GetStreamPipe(job.Target.GetHostname())
		if !qExists && !tExists {
			return fmt.Errorf("%w: %s", jobs.ErrTargetUnreachable, job.Target.Name)
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var respMsg string
		var err error
		if qExists {
			respMsg, err = qSess.CallMessage(
				timeoutCtx,
				"target_status",
				&fswire.TargetStatusReq{Drive: job.Target.VolumeID},
			)
		} else {
			respMsg, err = tSess.CallMessage(
				timeoutCtx,
				"target_status",
				&fswire.TargetStatusReq{Drive: job.Target.VolumeID},
			)
		}
		if err != nil || !isReachable(respMsg) {
			return fmt.Errorf("%w: %s", jobs.ErrTargetUnreachable, job.Target.Name)
		}

	case coredb.TargetTypeS3:
	}

	return nil
}

func isReachable(msg string) bool {
	return len(msg) >= 9 && msg[:9] == "reachable"
}

func (b *backupJob) mountSource(ctx context.Context, target coredb.Target) (string, *mountrpc.AgentMount, *mountrpc.S3Mount, error) {
	select {
	case <-ctx.Done():
		return "", nil, nil, jobs.ErrCanceled
	default:
	}

	var (
		srcPath    = target.Path
		agentMount *mountrpc.AgentMount
		s3Mount    *mountrpc.S3Mount
		err        error
	)

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	if target.IsDatabase() && b.databaseAware {
		bundle, err := database.ResolveClientBundle(ctx, target, job.DatabaseClientFamily, job.DatabaseClientDir)
		if err != nil {
			return "", nil, nil, err
		}
		password, err := b.app.CoreDB.GetDatabasePassword(target.Name)
		if err != nil {
			return "", nil, nil, fmt.Errorf("get database password: %w", err)
		}
		databaseLog := taskLogWriter{
			destination: b.logger.JobStdoutWriter(),
			logLine:     b.logQueuedLine,
		}
		if _, err := fmt.Fprintf(databaseLog, "--- %s log starts here ---\n", databaseLogLabel(target)); err != nil {
			return "", nil, nil, fmt.Errorf("write database log marker: %w", err)
		}
		stagedDump, err := database.StageDump(ctx, "", target, password, database.DumpOptions{
			Scope:     job.DatabaseScope,
			Database:  job.DatabaseName,
			LogWriter: databaseLog,
		}, bundle)
		if err != nil {
			return "", nil, nil, err
		}
		b.mu.Lock()
		b.stagedDump = stagedDump
		b.mu.Unlock()
		srcPath = stagedDump.ArchiveDir
		if len(stagedDump.Manifest.Failed) > 0 {
			b.logger.Warn("database dump skipped failed databases", "databases", stagedDump.Manifest.Failed)
		}
	} else if target.IsAgent() {
		timedCtx, timedCtxCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer timedCtxCancel()

		agentMount, err = mountrpc.AgentFSMount(timedCtx, b.app, job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = agentMount.Path

		select {
		case <-ctx.Done():
			agentMount.Unmount()
			agentMount.CloseMount()
			return "", nil, nil, jobs.ErrCanceled
		default:
		}

		b.mu.Lock()
		if latestBackup, err := b.app.CoreDB.GetBackup(b.job.ID); err == nil {
			b.job = latestBackup
		}
		job = b.job
		b.mu.Unlock()

		if agentMount.IsEmpty() {
			return "", agentMount, nil, jobs.ErrMountEmpty
		}
	} else if target.IsS3() {
		timedCtx, timedCtxCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer timedCtxCancel()

		s3Mount, err = mountrpc.S3FSMount(timedCtx, b.app, job, target)
		if err != nil {
			return "", nil, nil, err
		}
		srcPath = s3Mount.Path

		select {
		case <-ctx.Done():
			s3Mount.Unmount()
			s3Mount.CloseMount()
			return "", nil, nil, jobs.ErrCanceled
		default:
		}

		b.mu.Lock()
		if latestBackup, err := b.app.CoreDB.GetBackup(b.job.ID); err == nil {
			b.job = latestBackup
		}
		job = b.job
		b.mu.Unlock()

		if s3Mount.IsEmpty() {
			return "", nil, s3Mount, jobs.ErrMountEmpty
		}
	}

	if !target.IsDatabase() {
		srcPath = filepath.Join(srcPath, job.Subpath)
	}

	if job.Subpath != "" && !target.IsS3() {
		info, err := os.Stat(srcPath)
		if err != nil {
			if os.IsNotExist(err) {
				return "", agentMount, s3Mount, fmt.Errorf("%w: %q does not exist under the mount point", jobs.ErrSubpathNotFound, job.Subpath)
			}
			return "", agentMount, s3Mount, fmt.Errorf("%w: cannot access subpath %q: %w", jobs.ErrSubpathNotFound, job.Subpath, err)
		}
		if !info.IsDir() {
			return "", agentMount, s3Mount, fmt.Errorf("%w: %q is not a directory", jobs.ErrSubpathNotFound, job.Subpath)
		}
	}

	return srcPath, agentMount, s3Mount, nil
}
