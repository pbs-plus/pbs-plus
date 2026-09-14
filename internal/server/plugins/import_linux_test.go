//go:build linux

package plugins

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

func TestImportLocalTargets(t *testing.T) {
	ctx := context.Background()
	db, err := coredb.Initialize(ctx, filepath.Join(t.TempDir(), "import-local.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer db.Close()

	if _, err := ImportLocalTargets(ctx, db); err == nil || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("import without plugin = %v", err)
	}

	pluginID, version := installFilesystemPlugin(t, ctx, db, filesystem.Version)
	createLocalTarget(t, ctx, db, "docs", "/srv/docs")

	imported, err := ImportLocalTargets(ctx, db)
	if err != nil || imported != 1 {
		t.Fatalf("ImportLocalTargets = %d, %v", imported, err)
	}
	pluginTarget, err := db.GetPluginTarget(ctx, "docs")
	if err != nil || pluginTarget.PluginID != pluginID ||
		pluginTarget.TargetType != filesystem.TargetTypeLocal || pluginTarget.PluginVersion != version {
		t.Fatalf("imported plugin target = %#v, %v", pluginTarget, err)
	}
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(pluginTarget.Config, &config); err != nil {
		t.Fatalf("UnmarshalProtocol: %v", err)
	}
	if path, _ := config["path"].StringValue(); path != "/srv/docs" {
		t.Fatalf("imported config = %#v", config)
	}
	legacyAfter, err := db.GetTarget("docs")
	if err != nil || legacyAfter.Type != coredb.TargetTypeFilesystem || legacyAfter.Access != coredb.FilesystemAccessLocal || legacyAfter.Path != "/srv/docs" {
		t.Fatalf("legacy target after import = %#v, %v", legacyAfter, err)
	}

	again, err := ImportLocalTargets(ctx, db)
	if err != nil || again != 0 {
		t.Fatalf("second ImportLocalTargets = %d, %v", again, err)
	}
}
