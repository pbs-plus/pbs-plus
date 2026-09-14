//go:build linux

package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

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

	response, err := plugins.ProbeTarget(ctx, b.app.CoreDB, b.app.PluginSupervisor, job.Target.Name)
	if err != nil {
		return err
	}
	if !response.Available {
		return fmt.Errorf("%w: %s (%s)", jobs.ErrTargetUnreachable, job.Target.Name, response.Message)
	}
	return nil
}

func (b *backupJob) mountSource(ctx context.Context, target coredb.Target) (string, error) {
	select {
	case <-ctx.Done():
		return "", jobs.ErrCanceled
	default:
	}

	b.mu.RLock()
	job := b.job
	b.mu.RUnlock()

	pluginTarget, err := b.app.CoreDB.GetPluginTarget(ctx, target.Name)
	if err != nil {
		return "", fmt.Errorf("get plugin target %q: %w", target.Name, err)
	}
	var emptyAgentMount atomic.Bool
	lease, err := plugins.OpenBackup(ctx, b.app.CoreDB, b.app.PluginSupervisor, pluginTarget.Name, job.ID, b.executionID, job.PluginOptions, b.handlePluginEvent,
		func(mountCtx context.Context, request targetplugin.HostAgentBackupMountRequest) (targetplugin.HostAgentBackupMountResponse, func() error, error) {
			return b.brokerAgentMount(mountCtx, job, target.Name, request, &emptyAgentMount)
		})
	if err != nil {
		return "", err
	}
	b.mu.Lock()
	b.pluginLease = lease
	b.mu.Unlock()
	if emptyAgentMount.Load() {
		return "", jobs.ErrMountEmpty
	}

	srcPath := lease.Path
	if job.Subpath != "" {
		if !lease.Supports(targetplugin.FeatureSubpath) {
			return "", errors.New("plugin backup source does not support subpaths")
		}
		srcPath = filepath.Join(srcPath, job.Subpath)
		info, err := os.Stat(srcPath)
		if err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("%w: %q does not exist under the mount point", jobs.ErrSubpathNotFound, job.Subpath)
			}
			return "", fmt.Errorf("%w: cannot access subpath %q: %w", jobs.ErrSubpathNotFound, job.Subpath, err)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("%w: %q is not a directory", jobs.ErrSubpathNotFound, job.Subpath)
		}
	}

	return srcPath, nil
}

// brokerAgentMount keeps agent mount creation and release host-owned; the plugin only receives the mounted path.
func (b *backupJob) brokerAgentMount(ctx context.Context, job coredb.Backup, targetName string, request targetplugin.HostAgentBackupMountRequest, empty *atomic.Bool) (targetplugin.HostAgentBackupMountResponse, func() error, error) {
	mountCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	mount, err := mountrpc.AgentFSMount(mountCtx, b.app, job, coredb.Target{
		Name:      targetName,
		AgentHost: coredb.AgentHost{Name: request.Hostname, OperatingSystem: request.OperatingSystem},
		VolumeID:  request.VolumeID,
	})
	if err != nil {
		return targetplugin.HostAgentBackupMountResponse{}, nil, err
	}
	b.mu.Lock()
	b.agentMount = mount
	b.mu.Unlock()
	empty.Store(mount.IsEmpty())
	return targetplugin.HostAgentBackupMountResponse{Path: mount.Path, Empty: mount.IsEmpty()}, func() error {
		mount.Unmount()
		mount.CloseMount()
		return nil
	}, nil
}

func (b *backupJob) handlePluginEvent(event targetplugin.HostEvent) error {
	b.mu.RLock()
	task := b.scriptTask
	b.mu.RUnlock()
	if task != nil {
		task.LogString(event.Message)
	}
	if writer := b.logger.JobStdoutWriter(); writer != nil {
		if _, err := fmt.Fprintln(writer, event.Message); err != nil {
			return fmt.Errorf("write plugin event to backup log: %w", err)
		}
	}
	attributes := []any{"completed", event.Completed, "total", event.Total}
	switch event.Level {
	case targetplugin.EventWarning:
		b.logger.Warn(event.Message, attributes...)
	case targetplugin.EventError:
		b.logger.Error(errors.New(event.Message), "target plugin reported an error", attributes...)
	}
	return nil
}
