//go:build linux

package plugins

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Masterminds/semver"
	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

// PluginInstallURL is the management route that installs a plugin release.
const PluginInstallURL = "/api2/extjs/config/d2d-plugin-install"

// RestorePluginUnavailableError reports that no installed version can restore a snapshot.
type RestorePluginUnavailableError struct {
	PluginID   string
	Version    string
	TargetType string
	Reason     string
}

func (e *RestorePluginUnavailableError) Error() string {
	return fmt.Sprintf("restoring this snapshot needs plugin %q version %s for target type %q: %s; install a compatible version with %s",
		e.PluginID, e.Version, e.TargetType, e.Reason, PluginInstallURL)
}

// SelectRestorePlugin returns the installed version that can restore a snapshot, preferring the active one.
func SelectRestorePlugin(ctx context.Context, db *coredb.Store, metadata targetplugin.SnapshotMetadata) (targetplugin.PluginManifest, coredb.InstalledPluginVersion, error) {
	if err := metadata.Validate(); err != nil {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, fmt.Errorf("snapshot plugin metadata: %w", err)
	}
	unavailable := func(reason string) error {
		return &RestorePluginUnavailableError{
			PluginID:   metadata.PluginID,
			Version:    metadata.PluginVersion,
			TargetType: metadata.TargetType,
			Reason:     reason,
		}
	}
	plugin, err := db.GetInstalledPlugin(ctx, metadata.PluginID)
	if err != nil {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, unavailable("the plugin is not installed")
	}
	if !plugin.Enabled {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, unavailable("the plugin is disabled")
	}
	versions, err := db.ListInstalledPluginVersions(ctx, metadata.PluginID)
	if err != nil {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, err
	}
	var (
		bestManifest  targetplugin.PluginManifest
		bestInstalled coredb.InstalledPluginVersion
		loadErr       error
	)
	for _, version := range versions {
		manifest, installed, err := loadInstalledManifest(ctx, db, metadata.PluginID, version.Version)
		if err != nil {
			loadErr = errors.Join(loadErr, err)
			continue
		}
		if !restoreCompatible(manifest, metadata) {
			continue
		}
		if installed.Version == plugin.ActiveVersion {
			return manifest, installed, nil
		}
		if bestInstalled.Version == "" || newerVersion(manifest.Version, bestManifest.Version) {
			bestManifest, bestInstalled = manifest, installed
		}
	}
	if bestInstalled.Version != "" {
		return bestManifest, bestInstalled, nil
	}
	if loadErr != nil {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, loadErr
	}
	return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, unavailable("no installed version provides a compatible archive contract")
}

// ponytail: plugin major version stands in for declared archive ranges; add manifest ranges when a plugin needs a narrower window.
func restoreCompatible(manifest targetplugin.PluginManifest, metadata targetplugin.SnapshotMetadata) bool {
	if !slices.Contains(manifest.TargetTypes, metadata.TargetType) {
		return false
	}
	if manifest.TargetSchema.Version < metadata.TargetSchemaVersion || manifest.BackupSchema.Version < metadata.BackupSchemaVersion {
		return false
	}
	installed, err := semver.NewVersion(manifest.Version)
	if err != nil {
		return false
	}
	snapshot, err := semver.NewVersion(metadata.PluginVersion)
	if err != nil {
		return false
	}
	return installed.Major() == snapshot.Major() && !installed.LessThan(snapshot)
}

func newerVersion(candidate, current string) bool {
	parsedCandidate, err := semver.NewVersion(candidate)
	if err != nil {
		return false
	}
	parsedCurrent, err := semver.NewVersion(current)
	if err != nil {
		return true
	}
	return parsedCurrent.LessThan(parsedCandidate)
}

type RestoreLease struct {
	targetplugin.RestoreOpenResponse

	process   *targetplugin.Process
	cancel    context.CancelFunc
	workspace string
	request   targetplugin.RestoreOpenRequest

	agentRestoreStarted   atomic.Bool
	agentRestoreCompleted atomic.Bool
	consumeStarted        atomic.Bool
	closeOnce             sync.Once
	closeErr              error
}

func OpenRestore(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, targetName, jobID, cancellationID, idempotencyKey string, metadata targetplugin.SnapshotMetadata, options *coredb.PluginJobOptions, eventSink func(targetplugin.HostEvent) error, agentRestore func(context.Context, targetplugin.HostAgentRestoreRequest) error) (*RestoreLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	target, err := db.GetPluginTarget(ctx, targetName)
	if err != nil {
		return nil, err
	}
	if target.PluginID != metadata.PluginID {
		return nil, fmt.Errorf("target %q is owned by plugin %q, but the snapshot was written by plugin %q", targetName, target.PluginID, metadata.PluginID)
	}
	manifest, installed, err := loadActiveManifest(ctx, db, target.PluginID)
	if err != nil {
		return nil, err
	}
	if target.PluginVersion != installed.Version || target.SchemaVersion != manifest.TargetSchema.Version {
		return nil, errors.New("plugin target requires schema migration before restore")
	}
	_, compatible, err := SelectRestorePlugin(ctx, db, metadata)
	if err != nil {
		return nil, err
	}
	if compatible.Version != installed.Version {
		return nil, fmt.Errorf("restoring this snapshot needs plugin %q version %q; activate it before restoring", metadata.PluginID, compatible.Version)
	}
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(target.Config, &config); err != nil {
		return nil, fmt.Errorf("decode target config: %w", err)
	}
	secrets, err := db.ResolvePluginTargetSecrets(ctx, targetName)
	if err != nil {
		return nil, err
	}
	jobOptions, err := restoreOptions(manifest, installed, jobID, options)
	if err != nil {
		return nil, err
	}
	workspace, err := os.MkdirTemp("", ".pbs-plus-plugin-restore-")
	if err != nil {
		return nil, fmt.Errorf("create plugin restore workspace: %w", err)
	}
	leaseCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	process, err := supervisor.Open(leaseCtx, installed.PluginID, installed.InstallPath)
	if err != nil {
		cancel()
		_ = os.RemoveAll(workspace)
		return nil, err
	}
	operationID := uuid.NewString()
	if cancellationID == "" {
		cancellationID = operationID
	}
	if idempotencyKey == "" {
		idempotencyKey = "restore-open:" + operationID
	}
	lease := &RestoreLease{
		process:   process,
		cancel:    cancel,
		workspace: workspace,
		request: targetplugin.RestoreOpenRequest{
			Operation: targetplugin.Operation{
				ProtocolVersion:   targetplugin.CurrentProtocolVersion,
				ID:                operationID,
				IdempotencyKey:    idempotencyKey,
				DeadlineUnixMilli: restoreDeadlineUnixMilli(ctx),
				PluginVersion:     installed.Version,
				TargetType:        target.TargetType,
				SchemaVersion:     target.SchemaVersion,
				BrokerToken:       process.BrokerToken(),
			},
			Job: targetplugin.JobInput{
				Target:         targetplugin.TargetInput{Config: config, Secrets: secrets},
				Options:        jobOptions,
				Workspace:      workspace,
				JobID:          jobID,
				CancellationID: cancellationID,
			},
			Archive: metadata.Archive,
		},
	}
	process.SetEventSink(eventSink)
	if agentRestore != nil {
		process.SetAgentRestoreHandler(func(handlerCtx context.Context, request targetplugin.HostAgentRestoreRequest) error {
			if !lease.agentRestoreStarted.CompareAndSwap(false, true) {
				return errors.New("agent restore broker was called more than once")
			}
			if err := agentRestore(handlerCtx, request); err != nil {
				return err
			}
			lease.agentRestoreCompleted.Store(true)
			return nil
		})
	}
	if err := process.Invoke(ctx, targetplugin.MethodRestoreOpen, lease.request, &lease.RestoreOpenResponse); err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	switch lease.Mode {
	case targetplugin.RestoreModePath:
		if err := validateBackupPath(workspace, lease.Path, manifest.TargetSchema.Fields, config); err != nil {
			return nil, errors.Join(err, lease.Close())
		}
	case targetplugin.RestoreModeStructured:
	case targetplugin.RestoreModeAgent:
		if !lease.agentRestoreCompleted.Load() {
			return nil, errors.Join(errors.New("plugin did not complete the brokered agent restore"), lease.Close())
		}
	}
	return lease, nil
}

func (lease *RestoreLease) StagingPath() string {
	if lease == nil {
		return ""
	}
	return lease.workspace
}

func (lease *RestoreLease) Consume(ctx context.Context) error {
	if lease == nil || lease.process == nil {
		return errors.New("plugin restore lease is nil")
	}
	if lease.Mode != targetplugin.RestoreModeStructured {
		return fmt.Errorf("plugin restore mode %q does not consume structured archives", lease.Mode)
	}
	if !lease.consumeStarted.CompareAndSwap(false, true) {
		return errors.New("plugin structured restore was already consumed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	operation := lease.request.Operation
	operation.ID = uuid.NewString()
	operation.IdempotencyKey += ":consume"
	operation.DeadlineUnixMilli = restoreDeadlineUnixMilli(ctx)
	request := targetplugin.RestoreConsumeRequest{
		Operation:   operation,
		Job:         lease.request.Job,
		Archive:     lease.request.Archive,
		ArchivePath: lease.workspace,
	}
	var response targetplugin.RestoreConsumeResponse
	return lease.process.Invoke(ctx, targetplugin.MethodRestoreConsume, request, &response)
}

func (lease *RestoreLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.closeOnce.Do(func() {
		lease.closeErr = lease.process.Close()
		lease.cancel()
		lease.closeErr = errors.Join(lease.closeErr, os.RemoveAll(lease.workspace))
	})
	return lease.closeErr
}

func restoreOptions(manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, jobID string, options *coredb.PluginJobOptions) (targetplugin.Values, error) {
	if options == nil {
		return nil, nil
	}
	if options.JobID != jobID || options.PluginID != installed.PluginID || options.PluginVersion != installed.Version || options.SchemaVersion != manifest.RestoreSchema.Version {
		return nil, errors.New("plugin restore options require schema migration")
	}
	var values targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(options.Options, &values); err != nil {
		return nil, fmt.Errorf("decode plugin restore options: %w", err)
	}
	return values, nil
}

func restoreDeadlineUnixMilli(ctx context.Context) int64 {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline.UnixMilli()
	}
	return math.MaxInt64
}
