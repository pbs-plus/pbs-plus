//go:build linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

// main verifies bundled plugin installation against a throwaway state directory, never production state.
func main() {
	artifacts := flag.String("artifacts", conf.BundledPluginsPath, "directory holding bundled plugin executables")
	state := flag.String("state", "", "throwaway state directory for the verification database and install root")
	source := flag.String("source", "", "existing directory used as the verification backup source")
	repository := flag.String("repository", "", "optional proxmox-backup-client repository for a real snapshot")
	namespace := flag.String("namespace", "", "optional datastore namespace for the verification snapshot")
	backupID := flag.String("backup-id", "pbs-plus-plugin-verify", "backup ID for the verification snapshot")
	flag.Parse()

	if *state == "" || *source == "" {
		fmt.Fprintln(os.Stderr, "-state and -source are required")
		os.Exit(2)
	}
	snapshot := snapshotRequest{repository: *repository, namespace: *namespace, backupID: *backupID}
	if err := run(context.Background(), *artifacts, *state, *source, snapshot); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

var snapshotPattern = regexp.MustCompile(`host/[^/\s]+/[0-9TZ:-]+`)

type snapshotRequest struct {
	repository string
	namespace  string
	backupID   string
}

func run(ctx context.Context, artifacts, state, source string, snapshot snapshotRequest) error {
	if err := os.MkdirAll(state, 0o700); err != nil {
		return err
	}
	conf.PluginsBasePath = filepath.Join(state, "plugins")
	conf.SecretsKeyPath = filepath.Join(state, ".secret.key")

	db, err := coredb.Initialize(ctx, filepath.Join(state, "verify.db"))
	if err != nil {
		return fmt.Errorf("initialize verification database: %w", err)
	}
	defer db.Close()

	supervisor, err := targetplugin.NewSupervisor(8, 4)
	if err != nil {
		return err
	}

	installed, err := plugins.InstallBuiltins(ctx, db, artifacts)
	if err != nil {
		return fmt.Errorf("install bundled plugins: %w", err)
	}
	fmt.Printf("installed bundled plugins: %d\n", installed)

	registered, err := db.ListInstalledPlugins(ctx)
	if err != nil {
		return err
	}
	for _, plugin := range registered {
		healthy, err := plugins.CheckHealth(ctx, db, supervisor, plugin.PluginID, plugin.ActiveVersion)
		if err != nil {
			return fmt.Errorf("health check %s: %w", plugin.PluginID, err)
		}
		fmt.Printf("plugin %s %s enabled=%t healthy=%t\n", plugin.PluginID, plugin.ActiveVersion, plugin.Enabled, healthy)
	}

	types, err := plugins.ListTargetTypes(ctx, db)
	if err != nil {
		return err
	}
	for _, targetType := range types {
		fmt.Printf("target type %s from %s %s\n", targetType.TargetType, targetType.PluginID, targetType.PluginVersion)
	}

	const targetName = "plugin-verify-local"
	if err := plugins.CreateTarget(ctx, db, supervisor, targetName, filesystem.PluginID, filesystem.TargetTypeLocal,
		map[string][]string{"path": {source}}); err != nil {
		return fmt.Errorf("create target: %w", err)
	}
	probe, err := plugins.ProbeTarget(ctx, db, supervisor, targetName)
	if err != nil {
		return fmt.Errorf("probe target: %w", err)
	}
	fmt.Printf("probe available=%t size=%+v %s\n", probe.Available, probe.Size, probe.Message)

	lease, err := plugins.OpenBackup(ctx, db, supervisor, targetName, "verify-job", "verify-run", nil,
		func(event targetplugin.HostEvent) error {
			fmt.Printf("plugin event [%s] %s\n", event.Level, event.Message)
			return nil
		}, nil)
	if err != nil {
		return fmt.Errorf("open backup lease: %w", err)
	}
	metadataFile := filepath.Join(lease.MetadataSourcePath, targetplugin.SnapshotMetadataFileName)
	metadata, readErr := os.ReadFile(metadataFile)
	snapshotName, snapshotErr := writeSnapshot(ctx, lease, snapshot)
	closeErr := lease.Close()
	if snapshotErr != nil {
		return snapshotErr
	}
	if snapshotName != "" {
		if err := verifySnapshot(ctx, snapshotName, snapshot, lease.Metadata); err != nil {
			return err
		}
	}
	if readErr != nil {
		return fmt.Errorf("read snapshot metadata: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close backup lease: %w", closeErr)
	}
	fmt.Printf("backup lease path=%s features=%v metadata=%d bytes\n", lease.Path, lease.HostFeatures, len(metadata))
	fmt.Printf("snapshot metadata plugin=%s %s archive=%s v%d\n",
		lease.Metadata.PluginID, lease.Metadata.PluginVersion, lease.Metadata.Archive.Type, lease.Metadata.Archive.FormatVersion)

	if _, err := os.Stat(lease.MetadataSourcePath); !os.IsNotExist(err) {
		return fmt.Errorf("plugin metadata directory survived lease close: %v", err)
	}
	return nil
}

// writeSnapshot mirrors the scheduler backup command so a real snapshot carries the plugin metadata archive.
func writeSnapshot(ctx context.Context, lease *plugins.BackupLease, request snapshotRequest) (string, error) {
	if request.repository == "" {
		return "", nil
	}
	arguments := []string{
		"backup",
		"verify-source.pxar:" + lease.Path,
		targetplugin.SnapshotMetadataArchiveName + ".pxar:" + lease.MetadataSourcePath,
		"--repository", request.repository,
		"--change-detection-mode=metadata",
		"--backup-type", "host",
		"--backup-id", request.backupID,
		"--crypt-mode=none",
	}
	if request.namespace != "" {
		arguments = append(arguments, "--ns", request.namespace)
	}
	if lease.Supports(targetplugin.FeatureExclusions) {
		arguments = append(arguments, "--exclude", "!"+targetplugin.SnapshotMetadataFileName)
	}
	command := exec.CommandContext(ctx, "/usr/bin/proxmox-backup-client", arguments...)
	output, err := command.CombinedOutput()
	os.Stdout.Write(output)
	if err != nil {
		return "", fmt.Errorf("proxmox-backup-client backup: %w", err)
	}
	name := snapshotPattern.FindString(string(output))
	if name == "" {
		return "", errors.New("backup output carried no snapshot name")
	}
	return name, nil
}

// verifySnapshot restores the host-owned metadata archive, which is what restore dispatch reads to pick a plugin.
func verifySnapshot(ctx context.Context, snapshotName string, request snapshotRequest, want targetplugin.SnapshotMetadata) error {
	destination, err := os.MkdirTemp("", "plugin-verify-restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(destination)

	arguments := []string{
		"restore", snapshotName,
		targetplugin.SnapshotMetadataArchiveName + ".pxar", destination,
		"--repository", request.repository,
	}
	if request.namespace != "" {
		arguments = append(arguments, "--ns", request.namespace)
	}
	command := exec.CommandContext(ctx, "/usr/bin/proxmox-backup-client", arguments...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("proxmox-backup-client restore: %w", err)
	}
	encoded, err := os.ReadFile(filepath.Join(destination, targetplugin.SnapshotMetadataFileName))
	if err != nil {
		return fmt.Errorf("read restored snapshot metadata: %w", err)
	}
	var restored targetplugin.SnapshotMetadata
	if err := targetplugin.UnmarshalProtocol(encoded, &restored); err != nil {
		return fmt.Errorf("decode restored snapshot metadata: %w", err)
	}
	if restored != want {
		return fmt.Errorf("restored metadata %+v does not match the lease metadata %+v", restored, want)
	}
	fmt.Printf("snapshot %s round-tripped plugin metadata for %s %s\n", snapshotName, restored.PluginID, restored.PluginVersion)
	return nil
}
