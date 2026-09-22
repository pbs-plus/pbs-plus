//go:build linux

package plugins

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

func TestInstallBuiltins(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	db, err := coredb.Initialize(ctx, filepath.Join(directory, "builtins.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	root := filepath.Join(directory, "installed")
	oldRoot := conf.PluginsBasePath
	conf.PluginsBasePath = root
	t.Cleanup(func() { conf.PluginsBasePath = oldRoot })

	artifacts := filepath.Join(directory, "bundled")
	build := exec.Command("go", "build", "-o", filepath.Join(artifacts, "pbs-plus-plugin-filesystem"),
		"github.com/pbs-plus/pbs-plus/cmd/plugin-filesystem")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build plugin: %v\n%s", err, output)
	}

	installed, err := InstallBuiltins(ctx, db, artifacts)
	if err != nil || installed != 1 {
		t.Fatalf("InstallBuiltins = %d, %v", installed, err)
	}
	plugin, err := db.GetInstalledPlugin(ctx, filesystem.PluginID)
	if err != nil || plugin.ActiveVersion != filesystem.Version || !plugin.Enabled {
		t.Fatalf("installed plugin = %#v, %v", plugin, err)
	}
	version, err := db.GetInstalledPluginVersion(ctx, filesystem.PluginID, filesystem.Version)
	if err != nil || version.InstallPath != filepath.Join(root, filesystem.PluginID, filesystem.Version, "plugin") {
		t.Fatalf("installed version = %#v, %v", version, err)
	}
	manifest, _, err := loadActiveManifest(ctx, db, filesystem.PluginID)
	if err != nil || manifest.TargetSchema.Version != filesystem.Descriptor().TargetSchema.Version {
		t.Fatalf("active manifest = %#v, %v", manifest, err)
	}

	again, err := InstallBuiltins(ctx, db, artifacts)
	if err != nil || again != 0 {
		t.Fatalf("second InstallBuiltins = %d, %v", again, err)
	}
}
