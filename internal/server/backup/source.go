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
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/dovecot"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
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
		if err := validateAgentConnection(job.Target, b.app.Agents.IsOnline(job.Target.GetHostname())); err != nil {
			return err
		}

	case coredb.TargetTypeS3:
	}

	return nil
}

func validateAgentConnection(target coredb.Target, online bool) error {
	if !online {
		return fmt.Errorf("%w: %s", jobs.ErrTargetUnreachable, target.Name)
	}
	return nil
}

func (b *backupJob) mountSource(ctx context.Context, target coredb.Target) (string, *mountrpc.AgentMount, *mountrpc.S3Mount, error) {
	select {
	case <-ctx.Done():
		return "", nil, nil, jobs.ErrCanceled
	default:
	}

	var (
		srcPath         = target.Path
		agentMount      *mountrpc.AgentMount
		s3Mount         *mountrpc.S3Mount
		pluginSource    bool
		supportsSubpath bool
		err             error
	)

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	pluginTarget, pluginErr := b.app.CoreDB.GetPluginTarget(ctx, target.Name)
	if pluginErr == nil {
		lease, err := plugins.OpenBackup(ctx, b.app.CoreDB, b.app.PluginSupervisor, pluginTarget.Name, job.ID, b.executionID, job.PluginOptions, b.handlePluginEvent)
		if err != nil {
			return "", nil, nil, err
		}
		b.mu.Lock()
		b.pluginLease = lease
		b.mu.Unlock()
		srcPath = lease.Path
		pluginSource = true
		supportsSubpath = lease.Supports(targetplugin.FeatureSubpath)
	} else if !errors.Is(pluginErr, coredb.ErrTargetNotFound) {
		return "", nil, nil, pluginErr
	} else if target.IsDatabase() && b.databaseAware {
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
		bundle, err := database.SelectClientBundle(ctx, target, password, databaseLog)
		if err != nil {
			return "", nil, nil, err
		}
		scope := job.DatabaseScope
		if scope == "" && target.Type == coredb.TargetTypeLDAP {
			scope = "server"
		}
		stagedDump, err := database.StageDump(ctx, "", target, password, database.DumpOptions{
			Scope:     scope,
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
	} else if target.IsDovecot() && b.databaseAware {
		password, err := b.app.CoreDB.GetDatabasePassword(target.Name)
		if err != nil {
			return "", nil, nil, fmt.Errorf("get Dovecot password: %w", err)
		}
		dovecotLog := taskLogWriter{
			destination: b.logger.JobStdoutWriter(),
			logLine:     b.logQueuedLine,
		}
		if _, err := fmt.Fprintln(dovecotLog, "--- Dovecot log starts here ---"); err != nil {
			return "", nil, nil, fmt.Errorf("write Dovecot log marker: %w", err)
		}
		client, err := dovecot.SelectClient(ctx, target)
		if err != nil {
			return "", nil, nil, err
		}
		if _, err := fmt.Fprintf(dovecotLog, "using Dovecot client %s from %s\n", client.Version, client.Program); err != nil {
			return "", nil, nil, fmt.Errorf("write Dovecot client selection: %w", err)
		}
		stagedBackup, err := dovecot.StageBackup(ctx, "", target, password, dovecot.BackupOptions{
			Username:  job.DovecotUsername,
			Mailbox:   job.DovecotMailbox,
			LogWriter: dovecotLog,
		}, client)
		if err != nil {
			return "", nil, nil, err
		}
		b.mu.Lock()
		b.stagedDovecot = stagedBackup
		b.mu.Unlock()
		srcPath = stagedBackup.ArchiveDir
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

	if pluginSource && job.Subpath != "" && !supportsSubpath {
		return "", agentMount, s3Mount, errors.New("plugin backup source does not support subpaths")
	}
	if (pluginSource && supportsSubpath) || (!pluginSource && !target.IsDatabase() && !target.IsDovecot()) {
		srcPath = filepath.Join(srcPath, job.Subpath)
	}

	if job.Subpath != "" && (pluginSource || !target.IsS3()) {
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

func (b *backupJob) handlePluginEvent(event targetplugin.HostEvent) error {
	attributes := []any{"completed", event.Completed, "total", event.Total}
	switch event.Level {
	case targetplugin.EventDebug:
		b.logger.Debug(event.Message, attributes...)
	case targetplugin.EventWarning:
		b.logger.Warn(event.Message, attributes...)
	case targetplugin.EventError:
		b.logger.Error(errors.New(event.Message), "target plugin reported an error", attributes...)
	default:
		b.logger.Info(event.Message, attributes...)
	}
	return nil
}
