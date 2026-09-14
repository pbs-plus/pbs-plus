//go:build linux

package restore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const maxSnapshotMetadataBytes = 1 << 20

func (b *restoreJob) pluginExecute(ctx context.Context, target coredb.PluginTarget, idempotencyKey string) (err error) {
	metadata, err := b.snapshotPluginMetadata(ctx)
	if err != nil {
		legacy, ok := plugins.LegacyLocalSnapshotMetadata(ctx, b.app.CoreDB, target)
		if !ok {
			return err
		}
		metadata = legacy
		b.task.WriteString("snapshot predates plugin metadata; using the legacy local filesystem mapping")
	}
	b.task.WriteString(fmt.Sprintf(
		"restoring with plugin %s %s [target type: %s, archive: %s v%d]",
		metadata.PluginID, metadata.PluginVersion, metadata.TargetType, metadata.Archive.Type, metadata.Archive.FormatVersion,
	))

	lease, err := plugins.OpenRestore(ctx, b.app.CoreDB, b.app.PluginSupervisor, target.Name, b.job.ID,
		b.executionID, idempotencyKey, metadata, b.job.PluginOptions, b.handlePluginEvent, nil)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := lease.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("release plugin restore lease: %w", closeErr))
		}
	}()

	sourcePath := b.job.SrcPath
	if strings.TrimSpace(sourcePath) == "" {
		sourcePath = "/"
	}

	switch lease.Mode {
	case targetplugin.RestoreModePath:
		if err := b.startLocalRestore(ctx, lease.Path, []string{sourcePath}, pxar.RestoreMode(b.job.Mode)); err != nil {
			return err
		}
		if err := b.waitForTransfer(ctx); err != nil {
			return err
		}
	case targetplugin.RestoreModeStructured:
		if err := b.startLocalRestore(ctx, lease.StagingPath(), []string{sourcePath}, pxar.RestoreModeNormal); err != nil {
			return err
		}
		if err := b.waitForTransfer(ctx); err != nil {
			return err
		}
		if err := lease.Consume(ctx); err != nil {
			return err
		}
	case targetplugin.RestoreModeAgent:
		return fmt.Errorf("plugin %q requested a brokered agent restore, which is not wired into the scheduler yet", metadata.PluginID)
	default:
		return fmt.Errorf("plugin restore mode %q is not supported", lease.Mode)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	b.runPostScript()
	return nil
}

func (b *restoreJob) snapshotPluginMetadata(ctx context.Context) (targetplugin.SnapshotMetadata, error) {
	encoded, err := pxar.ReadArchiveFile(ctx, b.job.Store, b.job.Namespace, b.job.Snapshot,
		proxmox.PluginMetadataArchiveName, "/"+targetplugin.SnapshotMetadataFileName, maxSnapshotMetadataBytes)
	if err != nil {
		return targetplugin.SnapshotMetadata{}, fmt.Errorf("this snapshot carries no target plugin metadata: %w", err)
	}
	var metadata targetplugin.SnapshotMetadata
	if err := targetplugin.UnmarshalProtocol(encoded, &metadata); err != nil {
		return targetplugin.SnapshotMetadata{}, fmt.Errorf("decode snapshot plugin metadata: %w", err)
	}
	return metadata, nil
}

func (b *restoreJob) handlePluginEvent(event targetplugin.HostEvent) error {
	b.task.WriteString(event.Message)
	attributes := []any{"completed", event.Completed, "total", event.Total}
	switch event.Level {
	case targetplugin.EventDebug:
		b.logger.Debug(event.Message, attributes...)
	case targetplugin.EventWarning:
		b.logger.Warn(event.Message, attributes...)
	case targetplugin.EventError:
		b.errCount.Add(1)
		b.logger.Error(errors.New(event.Message), "target plugin reported an error", attributes...)
	default:
		b.logger.Info(event.Message, attributes...)
	}
	return nil
}
