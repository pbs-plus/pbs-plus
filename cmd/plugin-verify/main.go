//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

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
	flag.Parse()

	if *state == "" || *source == "" {
		fmt.Fprintln(os.Stderr, "-state and -source are required")
		os.Exit(2)
	}
	if err := run(context.Background(), *artifacts, *state, *source); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

func run(ctx context.Context, artifacts, state, source string) error {
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
	closeErr := lease.Close()
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
