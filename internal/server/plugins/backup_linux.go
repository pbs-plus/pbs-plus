//go:build linux

package plugins

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const backupOpenTimeout = 5 * time.Minute

// AgentBackupMount mounts one agent volume for the host and returns its release step.
type AgentBackupMount func(context.Context, targetplugin.HostAgentBackupMountRequest) (targetplugin.HostAgentBackupMountResponse, func() error, error)

type BackupLease struct {
	targetplugin.BackupOpenResponse
	Metadata           targetplugin.SnapshotMetadata
	MetadataSourcePath string

	process   *targetplugin.Process
	cancel    context.CancelFunc
	workspace string
	mu        sync.Mutex
	brokered  []string
	closeOnce sync.Once
	closeErr  error
}

func OpenBackup(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, targetName, jobID, cancellationID string, options *coredb.PluginJobOptions, eventSink func(targetplugin.HostEvent) error, agentMount AgentBackupMount) (*BackupLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	target, err := db.GetPluginTarget(ctx, targetName)
	if err != nil {
		return nil, err
	}
	manifest, installed, err := loadActiveManifest(ctx, db, target.PluginID)
	if err != nil {
		return nil, err
	}
	if target.PluginVersion != installed.Version || target.SchemaVersion != manifest.TargetSchema.Version {
		return nil, errors.New("plugin target requires schema migration before backup")
	}
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(target.Config, &config); err != nil {
		return nil, fmt.Errorf("decode target config: %w", err)
	}
	secrets, err := db.ResolvePluginTargetSecrets(ctx, targetName)
	if err != nil {
		return nil, err
	}
	jobOptions, err := backupOptions(manifest, installed, jobID, options)
	if err != nil {
		return nil, err
	}
	workspace, err := os.MkdirTemp("", ".pbs-plus-plugin-backup-")
	if err != nil {
		return nil, fmt.Errorf("create plugin backup workspace: %w", err)
	}
	leaseCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	process, err := supervisor.Open(leaseCtx, installed.PluginID, installed.InstallPath)
	if err != nil {
		cancel()
		_ = os.RemoveAll(workspace)
		return nil, err
	}
	process.SetEventSink(eventSink)
	openCtx, openCancel := context.WithTimeout(ctx, backupOpenTimeout)
	defer openCancel()
	deadline, _ := openCtx.Deadline()
	operationID := uuid.NewString()
	if cancellationID == "" {
		cancellationID = operationID
	}
	request := targetplugin.BackupOpenRequest{
		Operation: targetplugin.Operation{
			ProtocolVersion:   targetplugin.CurrentProtocolVersion,
			ID:                operationID,
			IdempotencyKey:    "backup-open:" + operationID,
			DeadlineUnixMilli: deadline.UnixMilli(),
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
	}
	lease := &BackupLease{process: process, cancel: cancel, workspace: workspace}
	if agentMount != nil {
		process.SetAgentBackupMountHandler(func(mountCtx context.Context, mountRequest targetplugin.HostAgentBackupMountRequest) (targetplugin.HostAgentBackupMountResponse, func() error, error) {
			response, cleanup, err := agentMount(mountCtx, mountRequest)
			if err != nil {
				return targetplugin.HostAgentBackupMountResponse{}, nil, err
			}
			lease.mu.Lock()
			lease.brokered = append(lease.brokered, response.Path)
			lease.mu.Unlock()
			return response, cleanup, nil
		})
	}
	if err := process.Invoke(openCtx, targetplugin.MethodBackupOpen, request, &lease.BackupOpenResponse); err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	if lease.Kind != targetplugin.SourceDirectory {
		return nil, errors.Join(fmt.Errorf("plugin backup source kind %q is not supported", lease.Kind), lease.Close())
	}
	if err := validateBackupPath(workspace, lease.Path, manifest.TargetSchema.Fields, config, lease.brokeredPaths()); err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	lease.Metadata = targetplugin.SnapshotMetadata{
		FormatVersion:       targetplugin.SnapshotMetadataFormatVersion,
		PluginID:            installed.PluginID,
		PluginVersion:       installed.Version,
		TargetType:          target.TargetType,
		TargetSchemaVersion: target.SchemaVersion,
		BackupSchemaVersion: manifest.BackupSchema.Version,
		Archive:             lease.Archive,
	}
	lease.MetadataSourcePath, err = writeSnapshotMetadata(lease.Metadata)
	if err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	return lease, nil
}

func (lease *BackupLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.closeOnce.Do(func() {
		lease.closeErr = lease.process.Close()
		lease.cancel()
		lease.closeErr = errors.Join(lease.closeErr, os.RemoveAll(lease.workspace), os.RemoveAll(lease.MetadataSourcePath))
	})
	return lease.closeErr
}

func (lease *BackupLease) brokeredPaths() []string {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return slices.Clone(lease.brokered)
}

func (lease *BackupLease) Supports(feature targetplugin.HostFeature) bool {
	return lease != nil && slices.Contains(lease.HostFeatures, feature)
}

func writeSnapshotMetadata(metadata targetplugin.SnapshotMetadata) (string, error) {
	if err := metadata.Validate(); err != nil {
		return "", err
	}
	encoded, err := targetplugin.MarshalProtocol(metadata)
	if err != nil {
		return "", fmt.Errorf("encode plugin snapshot metadata: %w", err)
	}
	directory, err := os.MkdirTemp("", ".pbs-plus-plugin-metadata-")
	if err != nil {
		return "", fmt.Errorf("create plugin snapshot metadata directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, targetplugin.SnapshotMetadataFileName), encoded, 0o600); err != nil {
		_ = os.RemoveAll(directory)
		return "", fmt.Errorf("write plugin snapshot metadata: %w", err)
	}
	return directory, nil
}

func backupOptions(manifest targetplugin.PluginManifest, installed coredb.InstalledPluginVersion, jobID string, options *coredb.PluginJobOptions) (targetplugin.Values, error) {
	if options == nil {
		return nil, nil
	}
	if options.JobID != jobID || options.PluginID != installed.PluginID || options.PluginVersion != installed.Version || options.SchemaVersion != manifest.BackupSchema.Version {
		return nil, errors.New("plugin backup options require schema migration")
	}
	var values targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(options.Options, &values); err != nil {
		return nil, fmt.Errorf("decode plugin backup options: %w", err)
	}
	return values, nil
}

func validateBackupPath(workspace, path string, fields []targetplugin.FormField, config targetplugin.Values, extraRoots []string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat plugin backup source: %w", err)
	}
	if !info.IsDir() {
		return errors.New("plugin backup source is not a directory")
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve plugin backup source: %w", err)
	}
	allowed := append([]string{workspace}, configuredPaths(fields, config)...)
	allowed = append(allowed, extraRoots...)
	for _, root := range allowed {
		resolvedRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		relative, err := filepath.Rel(resolvedRoot, resolvedPath)
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil
		}
	}
	return errors.New("plugin backup source is outside its workspace and configured paths")
}

func configuredPaths(fields []targetplugin.FormField, config targetplugin.Values) []string {
	var paths []string
	for _, field := range fields {
		if field.Control == targetplugin.ControlPath {
			if value, ok := config[field.Key]; ok {
				if path, ok := value.StringValue(); ok && filepath.IsAbs(path) {
					paths = append(paths, path)
				}
			}
		}
		paths = append(paths, configuredPaths(field.Fields, config)...)
	}
	return paths
}
